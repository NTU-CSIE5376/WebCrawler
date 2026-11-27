"""Offerer access to the public crawler job API; no shared job filesystem."""
import json
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4


class CrawlJobClient:
    def __init__(self, base_url, offerer_id, timeout=10):
        self.base_url = base_url.rstrip("/")
        self.url = self.base_url + "/v1/manual-bulk-crawls"
        self.offerer_id = offerer_id
        self.prefix = f"offerer/{offerer_id}/domain/"
        self.timeout = timeout

    def _request(self, url, payload=None):
        data = json.dumps(payload).encode() if payload is not None else None
        request = Request(url, data=data, headers={"Content-Type": "application/json"})
        with urlopen(request, timeout=self.timeout) as response:
            return json.load(response)

    def active_inventory(self):
        pending, outstanding = self.inventory_by_domain()
        return set(pending), outstanding

    def inventory_by_domain(self):
        domains, outstanding, cursor = {}, 0, ""
        while True:
            page = self._request(self.url + "?" + urlencode(dict(
                crawler_id=self.offerer_id, client_prefix=self.prefix, limit=200, after=cursor)))
            for job in page["jobs"]:
                domain_id = int(job["client_reference"][len(self.prefix):].split("/")[0])
                remaining = job["total_urls"] - job["completed_urls"]
                domains[domain_id] = domains.get(domain_id,0)+remaining
                outstanding += remaining
            cursor = page["next_cursor"]
            if cursor is None:
                return domains, outstanding

    def performance(self):
        response = self._request(self.base_url + f"/v1/crawlers/{self.offerer_id}/performance")
        return {int(key):value for key,value in response.get("domains",{}).items()} if response.get("available") else {}

    def new_reference(self, domain_id):
        return self.prefix + str(domain_id) + "/" + uuid4().hex

    def submit(self, domain_id, urls, *, reconcile=False, reference=None):
        reference = reference or self.new_reference(domain_id)
        if reconcile:
            page = self._request(self.url + "?" + urlencode(dict(
                crawler_id=self.offerer_id, client_prefix=reference, limit=1000)))
            for job in page["jobs"]:
                if job["client_reference"] == reference:
                    return job["job_id"]
        return self._request(self.url, dict(urls=urls, processing_mode="ingest",
                                           validate_urls=False,
                                           client_reference=reference))["job_id"]
