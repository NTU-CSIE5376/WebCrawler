"""Dispatch site-specific hooks using the domain/host policy registry.

Handlers implement process_request(request, policy),
process_spider_output(response, result, policy), and/or
response_downloaded(response, request, policy). They are constructed once
per configured class, using from_crawler when available, like Scrapy middleware.
"""
from scrapy import Request, signals
from scrapy.utils.httpobj import urlparse_cached
from scrapy.utils.misc import build_from_crawler, load_object
from scrapy.utils.asyncgen import as_async_generator
from ..site_policies import SitePolicies


_STATE_KEY = "_site_policy_overrides"
_SENSITIVE_HEADERS = {b"Authorization", b"Proxy-Authorization", b"Cookie"}
_POLICY_META = ("dont_merge_cookies", "download_slot", "autothrottle_dont_adjust_delay")


class SiteMiddlewareDispatcher:
    def __init__(self, policies, *, crawler=None):
        self.policies = policies
        paths = {rule.middleware for rules in (policies.config.domains, policies.config.hosts)
                 for rule in rules.values() if rule.middleware is not None}
        self.handlers = {}
        for path in paths:
            handler_class = load_object(path)
            handler = build_from_crawler(handler_class, crawler) if crawler is not None else handler_class()
            if not any(callable(getattr(handler, hook, None))
                       for hook in ("process_request", "process_spider_output", "response_downloaded")):
                raise ValueError(f"Site middleware {path!r} must implement process_request, process_spider_output, or response_downloaded")
            self.handlers[path] = handler

    @classmethod
    def from_crawler(cls, crawler):
        if not hasattr(crawler, "_site_middleware_dispatcher"):
            crawler._site_middleware_dispatcher = cls(SitePolicies.load(), crawler=crawler)
            crawler.signals.connect(crawler._site_middleware_dispatcher.response_downloaded,
                                    signal=signals.response_downloaded)
        return crawler._site_middleware_dispatcher

    def response_downloaded(self, response, request, spider=None):
        url = urlparse_cached(request)
        if url.scheme not in {"http", "https"} or url.hostname is None:
            return
        policy = self.policies.resolve(url.hostname)
        hook = getattr(self.handlers.get(policy.middleware), "response_downloaded", None)
        if hook is not None:
            hook(response, request, policy)

    def process_request(self, request, spider=None):
        url = urlparse_cached(request)
        origin = (url.scheme, url.hostname, url.port if url.hostname is not None else None)
        previous = request.meta.pop(_STATE_KEY, None)
        if previous is not None:
            # Redirects and retries copy headers/meta. Restore site overrides
            # before selecting the destination handler, including unmatched URLs.
            for name, values in previous["headers"].items():
                request.headers.pop(name, None)
                if values is not None and not (origin != previous["origin"] and name in _SENSITIVE_HEADERS):
                    request.headers[name] = values
            if previous["timeout_overridden"]:
                request.meta.pop("download_timeout", None)
                if previous["timeout_present"]:
                    request.meta["download_timeout"] = previous["timeout"]
            for name, (present, value) in previous["meta"].items():
                request.meta.pop(name, None)
                if present:
                    request.meta[name] = value
        if url.scheme not in {"http", "https"} or url.hostname is None:
            return
        policy = self.policies.resolve(url.hostname)
        before = request.headers.copy()
        timeout_present = "download_timeout" in request.meta
        timeout = request.meta.get("download_timeout")
        original_meta = {name: (name in request.meta, request.meta.get(name)) for name in _POLICY_META}
        if policy.request_limits.download_slot is not None:
            request.meta["download_slot"] = policy.request_limits.download_slot
        if policy.crawler_mode == "fixed":
            request.meta["autothrottle_dont_adjust_delay"] = True
        hook = getattr(self.handlers.get(policy.middleware), "process_request", None)
        result = None if hook is None else hook(request, policy)
        changed = {name for name in before.keys() | request.headers.keys()
                   if (name in before) != (name in request.headers)
                   or before.getlist(name) != request.headers.getlist(name)}
        request.meta[_STATE_KEY] = {
            "origin": origin,
            "headers": {name: tuple(before.getlist(name)) if name in before else None for name in changed},
            "timeout_overridden": (timeout_present != ("download_timeout" in request.meta)
                                   or timeout != request.meta.get("download_timeout")),
            "timeout_present": timeout_present, "timeout": timeout,
            "meta": original_meta,
        }
        if isinstance(result, Request):
            result.meta[_STATE_KEY] = request.meta[_STATE_KEY]
        return result

    async def process_spider_output(self, response, result, spider=None):
        url = urlparse_cached(response)
        if url.scheme in {"http", "https"} and url.hostname is not None:
            policy = self.policies.resolve(url.hostname)
            handler = self.handlers.get(policy.middleware)
            hook = getattr(handler, "process_spider_output", None)
            if hook is not None:
                result = hook(response, result, policy)
        async for item in as_async_generator(result):
            yield item
