import scrapy

class PageItem(scrapy.Item):
    url = scrapy.Field()
    domain = scrapy.Field()
    fail_reason = scrapy.Field()
    content = scrapy.Field() # html
    outlinks = scrapy.Field() # [{"url", "domain", "anchor"}]
    title = scrapy.Field() # <title> text, trimmed to 500 chars; None on fail/non-HTML

