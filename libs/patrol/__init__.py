"""Golden parent patrol library.

Patrol logic that tracks parent pages with historical golden child output and
periodically re-enqueues them so the crawler keeps refetching the most
productive sources of golden URLs.
"""
