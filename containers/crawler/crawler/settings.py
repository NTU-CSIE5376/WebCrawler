import os

from crawler.dispatch_defaults import MANUAL_SHARE_PERCENT
from crawler.site_policies import SitePolicies

BOT_NAME = "crawler"
LOG_LEVEL = os.getenv("CRAWLER_LOG_LEVEL", "WARNING")

SPIDER_MODULES = ["crawler.spiders"]
NEWSPIDER_MODULE = "crawler.spiders"

ROBOTSTXT_OBEY = True

DUPEFILTER_CLASS = "scrapy.dupefilters.BaseDupeFilter" # Disable dupefilter

# One dispatch policy for all jobs on this crawler; weighted gives manual work
# MANUAL_BULK_CRAWL_SHARE_PERCENT of the dispatches while both sources have work.
MANUAL_BULK_CRAWL_SCHEDULING_POLICY = os.getenv("MANUAL_BULK_CRAWL_SCHEDULING_POLICY", "weighted")
# This production value is owned by main, not the host environment.
MANUAL_BULK_CRAWL_SHARE_PERCENT = MANUAL_SHARE_PERCENT
# FIFO keeps newly fed
# requests from overtaking older ones at the same priority, including startup.
SCHEDULER = "crawler.fair_scheduler.FairScheduler"
SCHEDULER_MEMORY_QUEUE = "scrapy.squeues.FifoMemoryQueue"
SCHEDULER_DISK_QUEUE = "scrapy.squeues.PickleFifoDiskQueue"
SCHEDULER_START_MEMORY_QUEUE = None
SCHEDULER_START_DISK_QUEUE = None
RETRY_PRIORITY_ADJUST = 0

# Default Scrapy headers trip Akamai / Cloudflare bot fingerprints (e.g.
# wapo silently RSTs HTTP/2 streams that lack Sec-Fetch-*, surfacing as
# TimeoutError rather than a 4xx). Match a real Chrome request.
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)
DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

# Apply site identity before RobotsTxtMiddleware (100). Header overrides also
# cover retries, redirects, robots.txt, and both normal and manual requests.
DOWNLOADER_MIDDLEWARES = {
    "crawler.middlewares.site_dispatcher.SiteMiddlewareDispatcher": 80,
    "crawler.middlewares.retry_after.RetryAfterMiddleware": 90,
    # Last before the downloader, first on responses/exceptions: robots fetches
    # acquire separately; retries/redirects release before reacquiring a lease.
    "crawler.middlewares.shared_concurrency.SharedConcurrencyMiddleware": 1000,
}
SPIDER_MIDDLEWARES = {"crawler.middlewares.site_dispatcher.SiteMiddlewareDispatcher": 500}


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


# Kill switch: requeue retries/redirects under their host, not the policy slot.
SCHEDULER_RESTORE_POLICY_META = _env_bool("CRAWLER_SCHEDULER_RESTORE_POLICY_META", True)


CONCURRENT_REQUESTS = int(os.getenv("CRAWLER_CONCURRENT_REQUESTS", "256"))
SCRAPER_SLOT_MAX_ACTIVE_SIZE = int(
    os.getenv("CRAWLER_SCRAPER_SLOT_MAX_ACTIVE_SIZE", "100000000")
)
_SITE_POLICIES = SitePolicies.load()
_DEFAULT_POLICY = _SITE_POLICIES.config.default
CRAWLER_THROTTLE_MODE = _DEFAULT_POLICY.crawler_mode
CONCURRENT_REQUESTS_PER_DOMAIN = _DEFAULT_POLICY.request_limits.max_concurrent_requests
DOWNLOAD_DELAY = _DEFAULT_POLICY.request_limits.min_request_delay_sec

# Keep the extension available when a domain selects AutoThrottle even if the
# global mode is fixed. Fixed requests opt out through request metadata.
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = float(os.getenv("CRAWLER_AUTOTHROTTLE_START_DELAY", "0.5"))
AUTOTHROTTLE_MAX_DELAY = float(os.getenv("CRAWLER_AUTOTHROTTLE_MAX_DELAY", "10.0"))
AUTOTHROTTLE_TARGET_CONCURRENCY = float(os.getenv("CRAWLER_AUTOTHROTTLE_TARGET_CONCURRENCY", "4.0"))
AUTOTHROTTLE_DEBUG = _env_bool("CRAWLER_AUTOTHROTTLE_DEBUG", False)
DOWNLOAD_SLOTS = _SITE_POLICIES.download_slots(start_delay=AUTOTHROTTLE_START_DELAY)

# Keep a local request backlog so one slow domain does not block the next IPC batch.
IPC_PREFETCH_LOW_WATERMARK_REQUESTS = CONCURRENT_REQUESTS * 2
IPC_PREFETCH_TARGET_REQUESTS = CONCURRENT_REQUESTS * 8

# Reload domain files when active in-flight domains drop below this threshold.
IPC_DOMAIN_LOW_WATERMARK = 144
# Also check the watermark on a timer: completions of owned requests and
# spider_idle are not enough once owned work has drained while manual bulk
# crawls keep the engine busy.
IPC_TOP_UP_INTERVAL_SEC = 5.0

DNS_TIMEOUT = 15
DOWNLOAD_TIMEOUT = 15
DOWNLOAD_MAXSIZE = 10 * 1024 * 1024
RETRY_ENABLED = True
RETRY_TIMES = 1 # initial + retry = 2

REDIRECT_ENABLED = True
COOKIES_ENABLED = False

#HTTPCACHE_ENABLED = True
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_POLICY = "scrapy.extensions.httpcache.RFC2616Policy"

# Memory & stats logging
EXTENSIONS = {
    "scrapy.extensions.throttle.AutoThrottle": None,
    "crawler.autothrottle.SiteAutoThrottle": 500,
    "scrapy.extensions.logstats.LogStats": 500,
    "scrapy.extensions.memusage.MemoryUsage": 500,
}

ITEM_PIPELINES = {
    "crawler.pipelines.JsonPipeline": 500,
}

URL_QUEUE_TEMPLATE = "/data/ipc/url_queue/crawler_{id:02d}"
RESULT_DIR_TEMPLATE = "/data/ipc/crawl_result/crawler_{id:02d}"
# Transport location and resource limits belong to the crawler. Per-job output
# destinations arrive in the queue payload, without reading API configuration.
MANUAL_BULK_CRAWL_QUEUE_DIR = os.getenv(
    "CRAWLER_MANUAL_BULK_CRAWL_QUEUE_DIR", "/data/manual_bulk_crawls/pending",
)
MANUAL_BULK_CRAWL_MAX_ACTIVE_JOBS = int(os.getenv("CRAWLER_MANUAL_BULK_CRAWL_MAX_ACTIVE_JOBS", "64"))
# Outstanding manual requests per host: latency / delay rounded up, plus this headroom.
MANUAL_BULK_CRAWL_HOST_HEADROOM = int(os.getenv("CRAWLER_MANUAL_BULK_CRAWL_HOST_HEADROOM", "1"))
INTERVAL_MINUTES = 10
