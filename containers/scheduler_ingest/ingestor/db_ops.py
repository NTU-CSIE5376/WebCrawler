from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone

from psycopg2.extras import execute_values
from sqlalchemy.orm import sessionmaker


@dataclass
class IngestResult:
    new_link: bool = False
    domain_id: int | None = None
    is_ok: bool = False
    is_upd: bool = False
    fail_reason: str | None = None


BATCH_SIZE = 500

# Catches already-queued oversized urls that bypassed the spider-side filter.
MAX_URL_LEN = 2500

ROBOTS_UNKNOWN = 0
ROBOTS_ALLOWED = 1
ROBOTS_DISALLOWED = 2
ROBOTS_FAIL_PREFIX = "IgnoreRequest Forbidden by robots.txt"


_CUR_INSERT_COLS = (
    "url",
    "domain_id",
    "last_fetch_ok",
    "last_content_update",
    "last_modified",
    "num_fetch_ok_90d",
    "num_fetch_fail_90d",
    "num_content_update_90d",
    "num_consecutive_fail",
    "last_fail_reason",
    "content_hash",
    "should_crawl",
    "title",
    "hreflang_count",
    "etag",
    "cache_control",
    "is_redirect",
    "redirect_hop_count",
    "robots_bits",
)

# snapshot_id / snapshot_at have DB defaults so they're omitted here.
_HIST_COLS = (
    "url",
    "domain_id",
    "first_seen",
    "last_scheduled",
    "last_fetch_ok",
    "last_content_update",
    "last_modified",
    "num_scheduled_90d",
    "num_fetch_ok_90d",
    "num_fetch_fail_90d",
    "num_content_update_90d",
    "num_consecutive_fail",
    "last_fail_reason",
    "content_hash",
    "should_crawl",
    "url_score",
    "domain_score",
    "source",
    "discovered_from",
    "title",
    "hreflang_count",
    "etag",
    "cache_control",
    "is_redirect",
    "redirect_hop_count",
    "discovery_source_type",
    "parent_page_score",
    "inlink_count_approx",
    "inlink_count_external",
    "anchor_text",
    "robots_bits",
)


class IngestDB:
    def __init__(self, Session: sessionmaker):
        self.Session = Session

    def _tcur(self, shard_id: int) -> str:
        return f"url_state_current_{shard_id:03d}"

    def _this(self, shard_id: int) -> str:
        return f"url_state_history_{shard_id:03d}"

    def _tevt(self, shard_id: int) -> str:
        return f"url_event_counter_{shard_id:03d}"

    @staticmethod
    def _parse_optional_datetime(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    @staticmethod
    def _robots_bits(is_ok: bool, fail_reason: str | None) -> int:
        if fail_reason and fail_reason.startswith(ROBOTS_FAIL_PREFIX):
            return ROBOTS_DISALLOWED
        if is_ok or fail_reason == "NonHTML content-type" or (fail_reason or "").startswith("HttpError "):
            return ROBOTS_ALLOWED
        return ROBOTS_UNKNOWN

    @staticmethod
    def _split_unique_urls(items: list[tuple[int, dict]]) -> list[list[tuple[int, dict]]]:
        """Split into sub-batches with unique URLs; ON CONFLICT DO UPDATE
        refuses to touch the same row twice in one statement."""
        seen: set[str] = set()
        for _, rec in items:
            url = rec["url"]
            if url in seen:
                break
            seen.add(url)
        else:
            return [items]

        sub_batches: list[list[tuple[int, dict]]] = []
        current: list[tuple[int, dict]] = []
        seen = set()
        for idx, rec in items:
            url = rec["url"]
            if url in seen:
                sub_batches.append(current)
                current = [(idx, rec)]
                seen = {url}
            else:
                current.append((idx, rec))
                seen.add(url)
        if current:
            sub_batches.append(current)
        return sub_batches

    def _bulk_results_unique(
        self, cur, shard_id: int, items: list[tuple[int, dict]],
    ) -> list[tuple[int, IngestResult]]:
        """Process one sub-batch of crawl results (unique URLs) for a shard."""
        if not items:
            return []

        tcur = self._tcur(shard_id)
        this = self._this(shard_id)
        tevt = self._tevt(shard_id)

        # Decode records and collect URLs needing an old-hash lookup in one pass.
        decoded = []
        check_urls = []
        for idx, rec in items:
            is_ok = rec["status"] == "ok"
            content_hash = rec.get("content_hash")
            inc_ok = int(is_ok)
            inc_fail = 1 - inc_ok
            url = rec["url"]
            decoded.append({
                "idx": idx,
                "url": url,
                "domain_id": int(rec["domain_id"]),
                "fetched_at": (
                    datetime.fromisoformat(rec["fetched_at"])
                    if rec.get("fetched_at")
                    else datetime.now(timezone.utc)
                ),
                "last_modified": self._parse_optional_datetime(rec.get("last_modified")),
                "fail_reason": rec.get("fail_reason"),
                "content_hash": content_hash,
                "is_ok": is_ok,
                "is_upd": False,
                "inc_ok": inc_ok,
                "inc_fail": inc_fail,
                "inc_upd": 0,
                "title": rec.get("title") if is_ok else None,
                "hreflang_count": rec.get("hreflang_count") if is_ok else None,
                "etag": rec.get("etag"),
                "cache_control": rec.get("cache_control"),
                "is_redirect": rec.get("is_redirect"),
                "redirect_hop_count": rec.get("redirect_hop_count"),
                "robots_bits": self._robots_bits(is_ok, rec.get("fail_reason")),
            })
            if is_ok and content_hash is not None:
                check_urls.append(url)

        if check_urls:
            cur.execute(
                f"SELECT url, content_hash FROM {tcur} WHERE url = ANY(%s)",
                (check_urls,),
            )
            old_hashes = dict(cur.fetchall())
            for d in decoded:
                if d["is_ok"] and d["content_hash"] is not None:
                    if old_hashes.get(d["url"]) != d["content_hash"]:
                        d["is_upd"] = True
                        d["inc_upd"] = 1

        # ON CONFLICT branch reads ok/upd from EXCLUDED.num_fetch_ok_90d /
        # num_content_update_90d so we avoid per-row CASE params in VALUES.
        upsert_rows = [
            (
                d["url"],
                d["domain_id"],
                d["fetched_at"] if d["is_ok"] else None,
                d["fetched_at"] if d["is_upd"] else None,
                d["last_modified"],
                d["inc_ok"],
                d["inc_fail"],
                d["inc_upd"],
                d["inc_fail"],  # num_consecutive_fail seed: 0 if ok else 1
                None if d["is_ok"] else d["fail_reason"],
                d["content_hash"],
                False,
                d["title"],
                d["hreflang_count"],
                d["etag"],
                d["cache_control"],
                d["is_redirect"],
                d["redirect_hop_count"],
                d["robots_bits"],
            )
            for d in decoded
        ]
        upsert_sql = f"""
        INSERT INTO {tcur} ({", ".join(_CUR_INSERT_COLS)})
        VALUES %s
        ON CONFLICT (url) DO UPDATE SET
          last_fetch_ok = COALESCE(EXCLUDED.last_fetch_ok, {tcur}.last_fetch_ok),
          last_content_update = COALESCE(EXCLUDED.last_content_update, {tcur}.last_content_update),
          num_fetch_ok_90d = {tcur}.num_fetch_ok_90d + EXCLUDED.num_fetch_ok_90d,
          num_fetch_fail_90d = {tcur}.num_fetch_fail_90d + EXCLUDED.num_fetch_fail_90d,
          num_content_update_90d = {tcur}.num_content_update_90d + EXCLUDED.num_content_update_90d,
          num_consecutive_fail = CASE
            WHEN EXCLUDED.num_fetch_ok_90d = 1 THEN 0
            ELSE {tcur}.num_consecutive_fail + 1
          END,
          last_fail_reason = EXCLUDED.last_fail_reason,
          content_hash = CASE
            WHEN EXCLUDED.num_content_update_90d = 1 THEN EXCLUDED.content_hash
            ELSE {tcur}.content_hash
          END,
          should_crawl = FALSE,
          title = COALESCE(EXCLUDED.title, {tcur}.title),
          hreflang_count = COALESCE(EXCLUDED.hreflang_count, {tcur}.hreflang_count),
          last_modified = COALESCE(EXCLUDED.last_modified, {tcur}.last_modified),
          etag = COALESCE(EXCLUDED.etag, {tcur}.etag),
          cache_control = COALESCE(EXCLUDED.cache_control, {tcur}.cache_control),
          is_redirect = COALESCE(EXCLUDED.is_redirect, {tcur}.is_redirect),
          redirect_hop_count = COALESCE(EXCLUDED.redirect_hop_count, {tcur}.redirect_hop_count),
          robots_bits = CASE
            WHEN EXCLUDED.robots_bits = {ROBOTS_UNKNOWN} THEN {tcur}.robots_bits
            ELSE EXCLUDED.robots_bits
          END
        RETURNING {", ".join(_HIST_COLS)}, (xmax = 0) AS inserted
        """
        returned = execute_values(
            cur, upsert_sql, upsert_rows, page_size=len(upsert_rows), fetch=True,
        )

        if returned:
            history_rows = [row[:-1] for row in returned]  # drop trailing 'inserted' flag
            execute_values(
                cur,
                f"INSERT INTO {this} ({', '.join(_HIST_COLS)}) VALUES %s",
                history_rows,
                page_size=len(history_rows),
            )

        counter_rows = [
            (d["url"], d["inc_ok"], d["inc_fail"], d["inc_upd"]) for d in decoded
        ]
        execute_values(
            cur,
            f"""
            INSERT INTO {tevt}
              (url, event_date, num_fetch_ok, num_fetch_fail, num_content_update, accounted)
            VALUES %s
            ON CONFLICT (url, event_date) DO UPDATE SET
              num_fetch_ok = {tevt}.num_fetch_ok + EXCLUDED.num_fetch_ok,
              num_fetch_fail = {tevt}.num_fetch_fail + EXCLUDED.num_fetch_fail,
              num_content_update = {tevt}.num_content_update + EXCLUDED.num_content_update
            """,
            counter_rows,
            template="(%s, CURRENT_DATE, %s, %s, %s, TRUE)",
            page_size=len(counter_rows),
        )

        # xmax = 0 in RETURNING means the row was inserted, not updated.
        inserted_flags = {row[0]: row[-1] for row in returned}
        return [
            (d["idx"], IngestResult(
                new_link=bool(inserted_flags.get(d["url"], False)),
                domain_id=d["domain_id"],
                is_ok=d["is_ok"],
                is_upd=d["is_upd"],
                fail_reason=d["fail_reason"],
            ))
            for d in decoded
        ]

    def _bulk_results(
        self, cur, shard_id: int, items: list[tuple[int, dict]],
    ) -> list[tuple[int, IngestResult]]:
        results: list[tuple[int, IngestResult]] = []
        for sub in self._split_unique_urls(items):
            results.extend(self._bulk_results_unique(cur, shard_id, sub))
        return results

    def _bulk_links(
        self, cur, shard_id: int, items: list[tuple[int, dict]],
    ) -> list[tuple[int, bool]]:
        if not items:
            return []

        tcur = self._tcur(shard_id)
        this = self._this(shard_id)

        by_url: dict[str, dict] = {}
        first_idx_by_url: dict[str, int] = {}
        dup_results: list[tuple[int, bool]] = []
        for idx, rec in items:
            url = rec["url"]
            inc_approx = int(rec.get("inlink_count_approx", 1))
            inc_external = int(rec.get("inlink_count_external", 0))
            if url in by_url:
                by_url[url]["inlink_count_approx"] += inc_approx
                by_url[url]["inlink_count_external"] += inc_external
                dup_results.append((idx, False))
            else:
                first_idx_by_url[url] = idx
                by_url[url] = {
                    **rec,
                    "inlink_count_approx": inc_approx,
                    "inlink_count_external": inc_external,
                }

        unique_items = [(first_idx_by_url[url], rec) for url, rec in by_url.items()]

        link_rows = [
            (
                rec["url"],
                int(rec["domain_id"]),
                float(rec.get("domain_score", 0.0)),
                rec.get("discovered_from"),
                int(rec.get("discovery_source_type", 0)),
                rec.get("parent_page_score"),
                int(rec.get("inlink_count_approx", 0)),
                int(rec.get("inlink_count_external", 0)),
                (rec.get("anchor_text") or None),
            )
            for _, rec in unique_items
        ]
        inserted_rows = execute_values(
            cur,
            f"""
            INSERT INTO {tcur}
              (url, domain_id, domain_score, discovered_from,
               discovery_source_type, parent_page_score,
               inlink_count_approx, inlink_count_external, anchor_text)
            VALUES %s
            ON CONFLICT (url) DO UPDATE SET
              inlink_count_approx = {tcur}.inlink_count_approx + EXCLUDED.inlink_count_approx,
              inlink_count_external = {tcur}.inlink_count_external + EXCLUDED.inlink_count_external,
              anchor_text = COALESCE({tcur}.anchor_text, EXCLUDED.anchor_text)
            RETURNING url, (xmax = 0) AS inserted
            """,
            link_rows,
            page_size=len(link_rows),
            fetch=True,
        )
        inserted_set = {row[0] for row in inserted_rows if row[1]}

        history_rows = [r for r in link_rows if r[0] in inserted_set]
        if history_rows:
            execute_values(
                cur,
                f"""
                INSERT INTO {this}
                  (url, domain_id, domain_score, discovered_from,
                   discovery_source_type, parent_page_score,
                   inlink_count_approx, inlink_count_external, anchor_text)
                VALUES %s
                """,
                history_rows,
                page_size=len(history_rows),
            )

        results = [(idx, rec["url"] in inserted_set) for idx, rec in unique_items]
        results.extend(dup_results)
        return results

    def process_batch(self, recs: list[dict]) -> list[IngestResult | bool | None]:
        """Group records by (kind, shard_id) and dispatch to bulk paths.

        All-or-nothing: any failure rolls back and re-raises.
        Returns IngestResult for results, bool for new links, in input order.
        """
        results: list[IngestResult | bool | None] = [None] * len(recs)

        results_by_shard: dict[int, list[tuple[int, dict]]] = defaultdict(list)
        links_by_shard: dict[int, list[tuple[int, dict]]] = defaultdict(list)
        for i, rec in enumerate(recs):
            if len(rec.get("url", "")) > MAX_URL_LEN:
                continue
            sid = int(rec["shard_id"])
            if rec.get("status") == "new":
                links_by_shard[sid].append((i, rec))
            else:
                results_by_shard[sid].append((i, rec))

        with self.Session.begin() as sess:
            with sess.connection().connection.cursor() as cur:
                # Links first so that a URL that appears as both a discovered
                # link and a crawl result in the same batch is inserted with
                # its domain_score before the result UPSERT touches it.
                for sid, items in links_by_shard.items():
                    for idx, ok in self._bulk_links(cur, sid, items):
                        results[idx] = ok
                for sid, items in results_by_shard.items():
                    for idx, ir in self._bulk_results(cur, sid, items):
                        results[idx] = ir
        return results
