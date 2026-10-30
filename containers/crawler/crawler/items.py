import scrapy

class PageItem(scrapy.Item):
    url = scrapy.Field()
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
    has_json_ld = scrapy.Field() # bool, page has >=1 <script type="application/ld+json">
    requested_url = scrapy.Field() # url string as scheduled (url_state_current key); differs from url after a redirect or for a non-canonical row
    requested_domain_id = scrapy.Field() # domain_id of the scheduled row (from the offerer queue file)

