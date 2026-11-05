import scrapy

class PageItem(scrapy.Item):
    url = scrapy.Field()
    # URL string as queued by the offerer (the url_state_current key). Differs
    # from `url` after a redirect or when canonicalize_url rewrote it.
    source_url = scrapy.Field()
    domain = scrapy.Field()
    fail_reason = scrapy.Field()
    content = scrapy.Field() # html
    outlinks = scrapy.Field() # [{"url", "domain", "anchor"}]
    title = scrapy.Field() # <title> text, trimmed to 500 chars; None on fail/non-HTML
    last_modified = scrapy.Field() # HTTP Last-Modified, ISO string when parseable
    etag = scrapy.Field() # HTTP ETag, trimmed to 500 chars
    cache_control = scrapy.Field() # HTTP Cache-Control, trimmed to 500 chars
    is_redirect = scrapy.Field() # whether Scrapy followed a redirect for this response
    redirect_hop_count = scrapy.Field() # number of redirect hops before final response
    hreflang_count = scrapy.Field() # number of alternate hreflang links in HTML
    has_json_ld = scrapy.Field()
    # True when the redirect target was fetched recently and was not fetched
    # again (see redirect_dedup.py); the item carries no content.
    redirect_deduped = scrapy.Field() # bool, page has >=1 <script type="application/ld+json">

