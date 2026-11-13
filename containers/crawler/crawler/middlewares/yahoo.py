"""Identify stateless public Yahoo crawls without changing requested URLs."""


class YahooMiddleware:
    def process_request(self, request, policy):
        request.headers["User-Agent"] = "WebCrawlerBot/1.0 (+https://github.com/NTU-CSIE5376/WebCrawler)"
        for name in ("Sec-Fetch-Dest", "Sec-Fetch-Mode", "Sec-Fetch-Site",
                     "Sec-Fetch-User", "Upgrade-Insecure-Requests"):
            request.headers[name] = []
        for name, values in policy.headers.items():
            request.headers[name] = values
        request.headers["Cookie"] = []
        request.meta["dont_merge_cookies"] = True
        if policy.download_timeout_sec is not None:
            request.meta["download_timeout"] = policy.download_timeout_sec
