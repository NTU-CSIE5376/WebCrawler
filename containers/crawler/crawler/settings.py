import json
import os

BOT_NAME = "crawler"
LOG_LEVEL = os.getenv("CRAWLER_LOG_LEVEL", "WARNING")

SPIDER_MODULES = ["crawler.spiders"]
NEWSPIDER_MODULE = "crawler.spiders"

ROBOTSTXT_OBEY = True

DUPEFILTER_CLASS = "scrapy.dupefilters.BaseDupeFilter" # Disable dupefilter

# One dispatch policy for all jobs on this crawler; weighted gives manual work
# MANUAL_BULK_CRAWL_SHARE_PERCENT of the dispatches while both sources have work.
MANUAL_BULK_CRAWL_SCHEDULING_POLICY = os.getenv("MANUAL_BULK_CRAWL_SCHEDULING_POLICY", "weighted")
MANUAL_BULK_CRAWL_SHARE_PERCENT = int(os.getenv("MANUAL_BULK_CRAWL_SHARE_PERCENT", "50"))
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


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


CONCURRENT_REQUESTS = int(os.getenv("CRAWLER_CONCURRENT_REQUESTS", "256"))
SCRAPER_SLOT_MAX_ACTIVE_SIZE = int(
    os.getenv("CRAWLER_SCRAPER_SLOT_MAX_ACTIVE_SIZE", "100000000")
)
USE_AUTOTHROTTLE = _env_bool("CRAWLER_USE_AUTOTHROTTLE", False)

if USE_AUTOTHROTTLE:
    CRAWLER_THROTTLE_MODE = "autothrottle"
    CONCURRENT_REQUESTS_PER_DOMAIN = int(
        os.getenv("CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN", "16")
    )
    DOWNLOAD_DELAY = float(os.getenv("CRAWLER_DOWNLOAD_DELAY", "0.25"))

    AUTOTHROTTLE_ENABLED = True
    AUTOTHROTTLE_START_DELAY = float(
        os.getenv("CRAWLER_AUTOTHROTTLE_START_DELAY", "0.5")
    )
    AUTOTHROTTLE_MAX_DELAY = float(
        os.getenv("CRAWLER_AUTOTHROTTLE_MAX_DELAY", "10.0")
    )
    AUTOTHROTTLE_TARGET_CONCURRENCY = float(
        os.getenv("CRAWLER_AUTOTHROTTLE_TARGET_CONCURRENCY", "2.0")
    )
    AUTOTHROTTLE_DEBUG = _env_bool("CRAWLER_AUTOTHROTTLE_DEBUG", False)
else:
    CRAWLER_THROTTLE_MODE = "fixed"
    with open("domain_qps.json") as f:
        _DOMAIN_QPS = json.load(f)
    _DEFAULT_QPS = _DOMAIN_QPS.pop("_default", {})

    CONCURRENT_REQUESTS_PER_DOMAIN = int(
        os.getenv(
            "CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN",
            str(_DEFAULT_QPS.get("concurrency", 4)),
        )
    )
    DOWNLOAD_DELAY = float(
        os.getenv("CRAWLER_DOWNLOAD_DELAY", str(_DEFAULT_QPS.get("delay", 1.0)))
    )
    DOWNLOAD_SLOTS = _DOMAIN_QPS
    AUTOTHROTTLE_ENABLED = False

# Keep a local request backlog so one slow domain does not block the next IPC batch.
IPC_PREFETCH_LOW_WATERMARK_REQUESTS = CONCURRENT_REQUESTS * 2
IPC_PREFETCH_TARGET_REQUESTS = CONCURRENT_REQUESTS * 8

# Reload domain files when active in-flight domains drop below this threshold.
IPC_DOMAIN_LOW_WATERMARK = 144

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
    "scrapy.extensions.logstats.LogStats": 500,
    "scrapy.extensions.memusage.MemoryUsage": 500,
}

ITEM_PIPELINES = {
    "crawler.pipelines.JsonPipeline": 500,
}

URL_QUEUE_TEMPLATE = "/data/ipc/url_queue/crawler_{id:02d}"
RESULT_DIR_TEMPLATE = "/data/ipc/crawl_result/crawler_{id:02d}"
# Crawl requests (bulk crawl v2). The crawler reads no API configuration; the API
# and crawlers share this root (inbox, frontier, segments, status).
def _manual(name: str, default: str, kind=int):
    return kind(os.getenv(f"CRAWLER_MANUAL_BULK_CRAWL_{name}", default))


MANUAL_BULK_CRAWL_ROOT = os.getenv("CRAWLER_MANUAL_BULK_CRAWL_ROOT", "/data/manual_bulk_crawls")
# Outstanding manual requests per host: latency / delay rounded up, plus this headroom.
MANUAL_BULK_CRAWL_HOST_HEADROOM = _manual("HOST_HEADROOM", "1")
MANUAL_BULK_CRAWL_POLL_S = _manual("POLL_S", "1", float)  # Inbox, cancel/expiry and quota checks
MANUAL_BULK_CRAWL_INGEST_FILES = _manual("INGEST_FILES", "8")  # Inbox files moved into SQLite per poll
MANUAL_BULK_CRAWL_CLOSE_CHUNK = _manual("CLOSE_CHUNK", "10000")  # Cancelled/expired rows per poll
MANUAL_BULK_CRAWL_SEGMENT_BYTES = _manual("SEGMENT_BYTES", str(64 * 1024 * 1024))
MANUAL_BULK_CRAWL_SEGMENT_S = _manual("SEGMENT_S", "60", float)
MANUAL_BULK_CRAWL_GZIP_LEVEL = _manual("GZIP_LEVEL", "6")
MANUAL_BULK_CRAWL_STATUS_S = _manual("STATUS_S", "60", float)
MANUAL_BULK_CRAWL_TOP_HOSTS = _manual("TOP_HOSTS", "10")
# Each crawler pauses a request once its unacked bytes exceed result_quota / SHARDS.
MANUAL_BULK_CRAWL_SHARDS = _manual("SHARDS", "16")
MANUAL_BULK_CRAWL_MIN_FREE_GB = _manual("MIN_FREE_GB", "300", float)  # Pause all manual dispatch below
# 429/503: block the slot for Retry-After or BLOCK_BASE_S * 2**n, at most MAX_BLOCK_S,
# and requeue the URL up to MAX_REQUEUES times before a terminal http_<code> record.
MANUAL_BULK_CRAWL_MAX_REQUEUES = _manual("MAX_REQUEUES", "5")
MANUAL_BULK_CRAWL_BLOCK_BASE_S = _manual("BLOCK_BASE_S", "30", float)
MANUAL_BULK_CRAWL_MAX_BLOCK_S = _manual("MAX_BLOCK_S", "3600", float)
INTERVAL_MINUTES = 10
