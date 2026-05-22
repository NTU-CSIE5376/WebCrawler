import json
import os

BOT_NAME = "crawler"
LOG_LEVEL = os.getenv("CRAWLER_LOG_LEVEL", "WARNING")

SPIDER_MODULES = ["crawler.spiders"]
NEWSPIDER_MODULE = "crawler.spiders"

ROBOTSTXT_OBEY = True

DUPEFILTER_CLASS = "scrapy.dupefilters.BaseDupeFilter" # Disable dupefilter


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
        os.getenv("CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN", "8")
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
INTERVAL_MINUTES = 10
