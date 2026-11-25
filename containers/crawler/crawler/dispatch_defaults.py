"""Repository-owned defaults shared by settings and scheduler construction.

Change the production dispatch share through a reviewed repository change,
not an untracked environment override. This is dispatch count, not CPU time.
"""

MANUAL_SHARE_PERCENT = 20


def is_api_request(request):
    return ("manual_bulk_crawl_job_id" in request.meta
            and request.meta.get("crawl_job_mode", "fetch") == "fetch")
