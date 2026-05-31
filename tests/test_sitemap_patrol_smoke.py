"""Unit tests for containers.sitemap_patroller.{discover,patrol}.service.

Covers the pure-Python pieces: sitemap XML parsing, robots.txt sitemap-
directive extraction, the "new outlink" IPC record schema, and the
IngestorEmitter file layout. No network or DB access.
"""
from __future__ import annotations

import gzip
import json

from containers.sitemap_patroller.discover import service as discover_service
from containers.sitemap_patroller.patrol import service as patrol_service
from libs.ipc.new_link_record import (
    DISCOVERY_SOURCE_SITEMAP,
    build_new_link_record,
)


# ----- patrol.parse_sitemap -----

def test_parse_urlset_extracts_loc_entries():
    xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://example.com/a</loc>
    <lastmod>2026-01-01</lastmod>
  </url>
  <url>
    <loc>https://example.com/b</loc>
  </url>
</urlset>
"""
    kind, urls = patrol_service.parse_sitemap(xml)
    assert kind == "urlset"
    assert urls == ["https://example.com/a", "https://example.com/b"]


def test_parse_sitemapindex_extracts_nested():
    xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>https://example.com/sitemap1.xml</loc></sitemap>
  <sitemap><loc>https://example.com/sitemap2.xml</loc></sitemap>
</sitemapindex>
"""
    kind, urls = patrol_service.parse_sitemap(xml)
    assert kind == "sitemapindex"
    assert urls == [
        "https://example.com/sitemap1.xml",
        "https://example.com/sitemap2.xml",
    ]


def test_parse_gzipped_urlset_decompresses_then_parses():
    # Big sites (MLB, news outlets) publish .xml.gz sitemaps; without the
    # gzip-magic sniff, the parser falls through to "unknown" / parse_error.
    xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://example.com/a</loc></url>
  <url><loc>https://example.com/b</loc></url>
</urlset>
"""
    kind, urls = patrol_service.parse_sitemap(gzip.compress(xml))
    assert kind == "urlset"
    assert urls == ["https://example.com/a", "https://example.com/b"]


def test_parse_gzipped_sitemapindex_extracts_nested():
    # The case that bit production: MLB's /sitemaps/weekly-news/index.xml.gz
    # is a sitemap-INDEX served gzipped. The fix has to live in parse_sitemap
    # (not just at fetch) because each <loc> in this index is ALSO an .xml.gz
    # that the next patrol cycle re-feeds to parse_sitemap.
    xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>https://example.com/sub-1.xml.gz</loc></sitemap>
  <sitemap><loc>https://example.com/sub-2.xml.gz</loc></sitemap>
</sitemapindex>
"""
    kind, urls = patrol_service.parse_sitemap(gzip.compress(xml))
    assert kind == "sitemapindex"
    assert urls == [
        "https://example.com/sub-1.xml.gz",
        "https://example.com/sub-2.xml.gz",
    ]


def test_parse_returns_unknown_for_corrupt_gzip():
    # Magic bytes match but the rest is junk — must degrade to "unknown",
    # never raise, so the patrol row gets parse_error instead of crashing
    # the worker. (This case raises gzip.BadGzipFile, an OSError subclass.)
    corrupt = b"\x1f\x8b" + b"not-actually-gzip"
    assert patrol_service.parse_sitemap(corrupt) == ("unknown", [])


def test_parse_returns_unknown_for_truncated_gzip():
    # A valid stream cut short raises EOFError (NOT OSError) from gzip.
    # The except clause must cover both or the worker crashes mid-batch.
    full = gzip.compress(b"<urlset>" + b"<url><loc>https://x/a</loc></url>" * 10000 + b"</urlset>")
    truncated = full[:20]
    assert patrol_service.parse_sitemap(truncated) == ("unknown", [])


def test_parse_returns_unknown_when_decompressed_too_large(monkeypatch):
    # Refuse to materialise a gzip bomb. Set the cap small for the test;
    # gzip.compress of a >100-byte payload is well over 100 decompressed.
    monkeypatch.setattr(patrol_service, "MAX_DECOMPRESSED_BYTES", 100)
    big_xml = (
        b"<urlset xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'>"
        + b"<url><loc>https://example.com/a</loc></url>" * 500
        + b"</urlset>"
    )
    assert patrol_service.parse_sitemap(gzip.compress(big_xml)) == ("unknown", [])


def test_parse_skips_non_http_loc():
    xml = b"""<?xml version="1.0"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>ftp://example.com/x</loc></url>
  <url><loc>/relative/path</loc></url>
  <url><loc>https://example.com/keepme</loc></url>
</urlset>
"""
    kind, urls = patrol_service.parse_sitemap(xml)
    assert kind == "urlset"
    assert urls == ["https://example.com/keepme"]


def test_parse_returns_unknown_for_non_sitemap_xml():
    assert patrol_service.parse_sitemap(b"<html><body>nope</body></html>") == ("unknown", [])
    assert patrol_service.parse_sitemap(b"")[0] == "unknown"


def test_parse_skips_loc_with_embedded_whitespace():
    # <loc> text with an embedded tab/newline would slip past .strip() and
    # later trip http.client.InvalidURL when the patrol worker fetched it.
    xml = b"""<?xml version="1.0"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://pittsburghpanthers.com\t/sitemap.xml</loc></url>
  <url><loc>https://example.com/\nbroken</loc></url>
  <url><loc>https://example.com/clean</loc></url>
</urlset>
"""
    kind, urls = patrol_service.parse_sitemap(xml)
    assert kind == "urlset"
    assert urls == ["https://example.com/clean"]


# ----- discover.parse_sitemap_directives -----

def test_parse_robots_sitemap_directives_case_insensitive():
    robots = (
        "User-agent: *\n"
        "Disallow: /private\n"
        "Sitemap: https://example.com/sitemap.xml\n"
        "sitemap: https://example.com/news.xml\n"
        "SITEMAP:   https://example.com/blog.xml  \n"
    )
    out = discover_service.parse_sitemap_directives(robots)
    assert out == [
        "https://example.com/sitemap.xml",
        "https://example.com/news.xml",
        "https://example.com/blog.xml",
    ]


def test_parse_robots_skips_relative_and_comments():
    robots = (
        "# Sitemap: https://example.com/commented.xml\n"
        "Sitemap: /relative.xml\n"
        "Sitemap: https://example.com/absolute.xml\n"
    )
    assert discover_service.parse_sitemap_directives(robots) == [
        "https://example.com/absolute.xml",
    ]


def test_parse_robots_skips_urls_with_embedded_whitespace():
    robots = (
        "Sitemap: https://example.com/\twith-tab.xml\n"
        "Sitemap: https://example.com/clean.xml\n"
    )
    assert discover_service.parse_sitemap_directives(robots) == [
        "https://example.com/clean.xml",
    ]


# ----- shared build_new_link_record (libs/ipc/new_link_record.py) -----
#
# The router and the patroller both emit IPC records through this single
# builder; these tests pin the schema for the sitemap-side caller. If the
# router-side caller ever drifts, route-side parity tests in the test stack
# will catch it.

def test_build_new_link_record_for_sitemap_matches_schema():
    rec = build_new_link_record(
        url="https://example.com/article",
        shard_id=98,
        domain_id=123,
        domain_score=0.95,
        discovered_from="https://example.com/sitemap.xml",
        discovery_source_type=DISCOVERY_SOURCE_SITEMAP,
        inlink_count_external=0,
    )
    assert rec["status"] == "new"
    assert rec["url"] == "https://example.com/article"
    assert rec["shard_id"] == 98
    assert rec["domain_id"] == 123
    assert rec["domain_score"] == 0.95
    assert rec["discovery_source_type"] == DISCOVERY_SOURCE_SITEMAP
    assert rec["discovered_from"] == "https://example.com/sitemap.xml"
    assert rec["inlink_count_approx"] == 1
    assert rec["inlink_count_external"] == 0
    assert rec["anchor_text"] is None


def test_build_new_link_record_marks_external():
    rec = build_new_link_record(
        url="https://cdn.example.net/img.jpg",
        shard_id=1, domain_id=2, domain_score=0.0,
        discovered_from="https://example.com/sitemap.xml",
        discovery_source_type=DISCOVERY_SOURCE_SITEMAP,
        inlink_count_external=1,
    )
    assert rec["inlink_count_external"] == 1


# ----- IngestorEmitter writes JSONL where the FolderReader expects it -----

def test_emitter_writes_to_time_bucket_folder(tmp_path):
    template = str(tmp_path / "ingestor_{id:02d}")
    emitter = patrol_service.IngestorEmitter(template, interval_minutes=10, run_tag="testtag")

    rec = build_new_link_record(
        url="https://example.com/a", shard_id=5, domain_id=7,
        domain_score=0.95, discovered_from="https://example.com/sitemap.xml",
        discovery_source_type=DISCOVERY_SOURCE_SITEMAP,
        inlink_count_external=0,
    )
    emitter.emit(ingestor_id=3, record=rec)

    ingestor_dir = tmp_path / "ingestor_03"
    assert ingestor_dir.exists()
    files = list(ingestor_dir.glob("*/*/*.jsonl"))
    assert len(files) == 1
    out_file = files[0]
    assert "sitemap_testtag" in out_file.name
    assert out_file.parent.name.isdigit() and len(out_file.parent.name) == 4   # HHMM
    assert out_file.parent.parent.name.isdigit() and len(out_file.parent.parent.name) == 8  # YYYYMMDD

    line = out_file.read_text(encoding="utf-8").strip()
    assert json.loads(line) == rec
