"""Request rules for public ESPN pages."""
from scrapy.exceptions import IgnoreRequest
from scrapy.utils.httpobj import urlparse_cached


_BOT_USER_AGENT = "WebCrawlerBot/1.0 (+https://github.com/NTU-CSIE5376/WebCrawler)"
_NAVIGATION_HEADERS = ("Sec-Fetch-Dest", "Sec-Fetch-Mode", "Sec-Fetch-Site",
                       "Sec-Fetch-User", "Upgrade-Insecure-Requests")


class ESPNMiddleware:
    # Match only the site's public frontend: developer/business subdomains can
    # use the same paths for unrelated content.
    frontend_hosts = frozenset({"espn.com", "www.espn.com"})
    non_content_paths = ("/login", "/members", "/personalization")

    def process_request(self, request, policy):
        url = urlparse_cached(request)
        if url.hostname in self.frontend_hosts:
            path = url.path.rstrip("/")
            if any(path == prefix or path.startswith(prefix + "/")
                   for prefix in self.non_content_paths):
                raise IgnoreRequest("Non-content login, account, or sharing endpoint")
        # Apply the same crawler identity to robots and pages.
        request.headers["User-Agent"] = _BOT_USER_AGENT
        for name in _NAVIGATION_HEADERS:
            request.headers[name] = []
        for name, values in policy.headers.items():
            request.headers[name] = values
        # Keep public responses stateless and use a decoder available in the
        # crawler image. Do not rewrite URLs or switch to alternate endpoints.
        request.headers["Accept-Encoding"] = "gzip"
        request.headers["Cookie"] = []
        request.meta["dont_merge_cookies"] = True
        if policy.download_timeout_sec is not None:
            request.meta["download_timeout"] = policy.download_timeout_sec
