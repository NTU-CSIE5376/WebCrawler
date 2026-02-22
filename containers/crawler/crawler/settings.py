import json

BOT_NAME = "crawler"
LOG_LEVEL = "WARNING"

SPIDER_MODULES = ["crawler.spiders"]
NEWSPIDER_MODULE = "crawler.spiders"

ROBOTSTXT_OBEY = True

DUPEFILTER_CLASS = "scrapy.dupefilters.BaseDupeFilter" # Disable dupefilter

with open("domain_qps.json") as f:
    _DOMAIN_QPS = json.load(f)
_DEFAULT_QPS = _DOMAIN_QPS.pop("_default", {})

CONCURRENT_REQUESTS = 128
CONCURRENT_REQUESTS_PER_DOMAIN = _DEFAULT_QPS.get("concurrency", 4)
DOWNLOAD_DELAY = _DEFAULT_QPS.get("delay", 1.0)
DOWNLOAD_SLOTS = _DOMAIN_QPS
AUTOTHROTTLE_ENABLED = False

DNS_TIMEOUT = 15
DOWNLOAD_TIMEOUT = 15
DOWNLOAD_MAXSIZE = 10 * 1024 * 1024
RETRY_ENABLED = True
RETRY_TIMES = 1 # initial + retry = 2

REDIRECT_ENABLED = True
COOKIES_ENABLED = False

HTTPCACHE_ENABLED = True
HTTPCACHE_DIR = "httpcache"
HTTPCACHE_POLICY = "scrapy.extensions.httpcache.RFC2616Policy"

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

