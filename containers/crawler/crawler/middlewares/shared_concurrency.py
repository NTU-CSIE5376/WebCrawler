"""Enforce site policy concurrency budgets across processes sharing /data/ipc."""
import asyncio
import fcntl
import hashlib
from pathlib import Path
from urllib.parse import urlsplit

from scrapy import signals

from ..site_policies import SitePolicies


DEFAULT_BUDGET_DIR = "/data/ipc/request_budgets"


class RequestBudget:
    """File leases shared by processes; worker exit/crash releases locks.

    All workers must use the same shared directory and policy limits. Never
    delete/replace lock files while workers run: locks attach to inodes.
    """

    def __init__(self, scope, limit, directory=DEFAULT_BUDGET_DIR):
        self.directory = Path(directory) / hashlib.sha256(scope.encode()).hexdigest()
        self.limit = limit

    def try_acquire(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        for index in range(self.limit):
            lease = (self.directory / f"{index}.lock").open("a+b")
            try:
                fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                lease.close()
                continue
            except BaseException:
                lease.close()
                raise
            return lease
        return None


class SharedConcurrencyMiddleware:
    def __init__(self, policies, directory=DEFAULT_BUDGET_DIR):
        self.policies = policies
        self.directory = directory
        self.budgets = {}
        # Keep leases out of request.meta: retries and redirects copy metadata.
        self.leases = {}

    @classmethod
    def from_crawler(cls, crawler):
        instance = cls(SitePolicies.load(), crawler.settings.get(
            "SHARED_REQUEST_BUDGET_DIR", DEFAULT_BUDGET_DIR))
        crawler.signals.connect(instance.close, signal=signals.engine_stopped)
        return instance

    async def process_request(self, request, spider=None):
        url = urlsplit(request.url)
        if url.scheme not in {"http", "https"} or not url.hostname:
            return
        scopes = self.policies.shared_budgets(url.hostname)
        leases = []
        try:
            for scope, limit in scopes:
                budget = self.budgets.setdefault(
                    scope, RequestBudget(scope, limit, self.directory))
                while True:
                    lease = budget.try_acquire()
                    if lease is not None:
                        leases.append(lease)
                        break
                    # Waiting does not block downloads for unrelated domains.
                    await asyncio.sleep(0.1)
        except BaseException:
            for lease in leases:
                lease.close()
            raise
        if leases:
            self.leases[request] = leases

    def _release(self, request):
        for lease in self.leases.pop(request, []):
            lease.close()

    def process_response(self, request, response, spider=None):
        self._release(request)
        return response

    def process_exception(self, request, exception, spider=None):
        self._release(request)

    def close(self):
        for leases in self.leases.values():
            for lease in leases:
                lease.close()
        self.leases.clear()
