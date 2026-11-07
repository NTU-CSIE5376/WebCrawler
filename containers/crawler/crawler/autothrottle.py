"""Scrapy AutoThrottle with a policy-specific minimum delay and fixed opt-out."""
from scrapy.extensions.throttle import AutoThrottle
from scrapy.utils.httpobj import urlparse_cached

from .site_policies import SitePolicies


class SiteAutoThrottle(AutoThrottle):
    def __init__(self, crawler):
        self.policies = SitePolicies.load()
        super().__init__(crawler)

    def _min_delay(self, spider=None):
        # The global floor would prevent sites from choosing a smaller minimum.
        # Apply the resolved site's floor after Scrapy's adaptive calculation.
        return 0

    def _start_delay(self, spider=None):
        default = self.policies.config.default
        minimum = default.request_limits.min_request_delay_sec
        if default.crawler_mode == "fixed":
            return minimum
        return max(minimum, self.crawler.settings.getfloat("AUTOTHROTTLE_START_DELAY"))

    def _adjust_delay(self, slot, latency, response):
        hostname = urlparse_cached(response).hostname
        policy = self.policies.resolve(hostname) if hostname else self.policies.config.default
        if policy.crawler_mode == "fixed":
            return
        global_target = self.target_concurrency
        if policy.request_limits.target_concurrent_requests is not None:
            self.target_concurrency = policy.request_limits.target_concurrent_requests
        try:
            # Scrapy's adjustment is synchronous; scope the override to this slot.
            super()._adjust_delay(slot, latency, response)
        finally:
            self.target_concurrency = global_target
        slot.delay = max(slot.delay, policy.request_limits.min_request_delay_sec)
