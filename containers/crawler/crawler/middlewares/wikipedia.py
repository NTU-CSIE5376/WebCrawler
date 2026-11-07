"""Wikipedia website rules, selected declaratively in site_policies.yml."""
from scrapy.exceptions import IgnoreRequest


class WikipediaMiddleware:
    def process_request(self, request, policy):
        if request.method not in {"GET", "HEAD"}:
            raise IgnoreRequest("Wikipedia crawling requires GET or HEAD")
        for name, values in policy.headers.items():
            request.headers[name] = values
        request.headers["Accept-Encoding"] = "gzip"
        request.headers["Cookie"] = []
        request.meta["dont_merge_cookies"] = True
        if policy.download_timeout_sec is not None:
            request.meta["download_timeout"] = policy.download_timeout_sec
