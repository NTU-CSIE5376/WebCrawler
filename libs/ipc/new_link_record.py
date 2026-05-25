"""Canonical "new outlink candidate" IPC record schema.

Both the scheduler_ingest `router` and the `sitemap_patroller / patrol`
worker write records of this shape into
`/data/ipc/crawl_result/ingestor_{NN}/{YYYYMMDD}/{HHMM}/*.jsonl`. The
ingestor consumes them through the same `_bulk_links` path regardless of
who wrote them; if the schema drifts between writers, the ingestor will
silently misread fields.

See docs/03-data-flow-and-ipc.md §3.3 "Router Output Record (new outlink
candidate)" for the prose contract.
"""
from __future__ import annotations

from typing import Optional


# Discovery-source provenance for URLs entering url_state_current.* via the
# "new" IPC record path. The ingestor stores this in
# `url_state_current_{shard}.discovery_source_type`.
#
#   0 — unknown / seed (default)
#   1 — discovered as a page outlink (router's path: crawler -> router -> ingestor)
#   2 — discovered in a sitemap <loc> (sitemap_patroller's path)
DISCOVERY_SOURCE_UNKNOWN = 0
DISCOVERY_SOURCE_PAGE_OUTLINK = 1
DISCOVERY_SOURCE_SITEMAP = 2


def build_new_link_record(
    *,
    url: str,
    shard_id: int,
    domain_id: int,
    domain_score: float,
    discovered_from: Optional[str],
    discovery_source_type: int,
    parent_page_score: Optional[float] = None,
    inlink_count_approx: int = 1,
    inlink_count_external: int = 0,
    anchor_text: Optional[str] = None,
) -> dict:
    """Return the JSON-serializable dict the ingestor's `_bulk_links` expects.

    Keep field names exactly as the ingestor reads them — adding a field here
    that the ingestor does not `.get(...)` is fine (it gets ignored), but
    renaming or dropping an existing field will break consumers.
    """
    return {
        "url": url,
        "status": "new",
        "shard_id": shard_id,
        "domain_id": domain_id,
        "domain_score": domain_score,
        "discovered_from": discovered_from,
        "discovery_source_type": discovery_source_type,
        "parent_page_score": parent_page_score,
        "inlink_count_approx": inlink_count_approx,
        "inlink_count_external": inlink_count_external,
        "anchor_text": anchor_text,
    }
