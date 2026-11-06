from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict, Optional, Set


logger = logging.getLogger("router")

import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, InterfaceError

from libs.config.loader import load_yaml, require
from libs.db.sharding.key import load_sharding_config
from libs.db.sharding.router import ShardRouter, host_of
from libs.ipc.jsonio import read_json, read_jsonl, append_jsonl_many
from libs.ipc.new_link_record import (
    DISCOVERY_SOURCE_PAGE_OUTLINK,
    SOURCE_YOUTUBE_CHANNEL,
    build_new_link_record,
)
from libs.stats.delta_writer import StatsDeltaWriter
from libs.ipc.folder_reader import current_interval

from .domain_resolver import DomainResolver

# Crawler-assigned priorities (only YouTube channel links today) live on the
# manual tier: strictly above the ranker's [0, 1] range, below golden's 3.0.
MANUAL_PRIORITY_MIN = 1.0
MANUAL_PRIORITY_MAX = 2.0

# Process-lifetime cap on the domain_id/score cache. Without this it grows
# unboundedly across folders for a long-lived router process; a handful of
# hugely popular domains dominate most pages' outlinks, so even a full reset
# at the cap re-warms itself within the next few folders.
DOMAIN_CACHE_CAP = 500_000


def _priority_fields(link: Dict[str, Any]) -> Dict[str, Any]:
    """url_priority / source for a crawler outlink that carries a priority;
    {} (natural discovery) for everything else or an out-of-range value."""
    priority = link.get("priority")
    if not isinstance(priority, (int, float)) or isinstance(priority, bool):
        return {}
    if not MANUAL_PRIORITY_MIN < priority <= MANUAL_PRIORITY_MAX:
        return {}
    return {"url_priority": float(priority), "source": SOURCE_YOUTUBE_CHANNEL}


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="replace")).hexdigest()

@dataclass(frozen=True)
class RouterConfig:
    router_id: int

    crawler_dir_template: str
    ingestor_dir_template: str
    progress_template: str
    stats_dir: str

    interval_minutes: int
    scan_sleep_minutes: int

    num_shards: int
    shards_per_ingestor: int

    domain_overrides: Dict[str, int]
    split_subdomains: Set[str]

    postgres_dsn: str


class RouterService:
    def __init__(self, cfg: RouterConfig):
        self.cfg = cfg
        self.sharder = ShardRouter(
            num_shards=self.cfg.num_shards,
            shards_per_ingestor=self.cfg.shards_per_ingestor,
            domain_overrides=self.cfg.domain_overrides,
            split_subdomains=self.cfg.split_subdomains,
        )
        self.engine = create_engine(
            self.cfg.postgres_dsn,
            pool_pre_ping=True,
            pool_recycle=1800,
            pool_size=2,
            max_overflow=1,
            pool_timeout=30,
            future=True,
            connect_args={
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 5,
                "keepalives_count": 5
            },
        )
        self.Session = sessionmaker(bind=self.engine, autoflush=False, autocommit=False, future=True)
        self.stats = StatsDeltaWriter(self.cfg.stats_dir)
        # Persists across process_folder calls (not reset per folder) so
        # repeat domains across folders skip the DB entirely.
        self.domain_cache: Dict[str, "tuple[int, float]"] = {}

    def _out_dir(self, ingestor_id: int) -> Path:
        base = Path(self.cfg.ingestor_dir_template.format(id=ingestor_id))
        date, time = current_interval(self.cfg.interval_minutes)
        return base / date / time

    def process_folder(self, folder: Path) -> None:
        """
        Read all json files under folder; write transformed json to ingestor dirs.
        """
        logger.info(
            "route.folder_start",
            extra={"event": "route.folder_start", "folder": str(folder)},
        )
        error = 0
        file_cnt = 0

        if len(self.domain_cache) > DOMAIN_CACHE_CAP:
            self.domain_cache.clear()
        domain_cache = self.domain_cache

        for f in folder.iterdir():
            if not f.is_file():
                continue

            if f.suffix == ".json":
                recs = [read_json(f)]
                file_cnt += 1
            elif f.suffix == ".jsonl":
                recs = read_jsonl(f)
                file_cnt += 1
            else:
                continue

            for rec in recs:
                status = rec.get("status")  # "ok"/"fail"
                content = rec.get("content")
                outlinks = rec.get("outlinks", [])

                host = host_of(rec.get("url"))
                domain = self.sharder.domain_key(host)
                shard_id = self.sharder.domain_to_shard(host)
                ingestor_id = self.sharder.shard_to_ingestor(shard_id)

                content_hash = None
                if status == "ok" and isinstance(content, str):
                    content_hash = sha1_hex(content)

                for attempt in range(3):
                    try:
                        # Resolve every domain this record touches (its own +
                        # every outlink's) in one batched round trip instead
                        # of one DB call per outlink. host_of() can return
                        # None for a malformed link URL; such links are
                        # skipped when the batch result is consumed below,
                        # same as the old per-link code implicitly dropped
                        # them via the `if not url` guard.
                        outlink_hosts = [
                            (link, host_of(link.get("url")))
                            for link in outlinks if link.get("url")
                        ]
                        domains_needed: Dict[str, int] = {domain: shard_id}
                        for _, lhost in outlink_hosts:
                            if lhost is None:
                                continue
                            ldomain = self.sharder.domain_key(lhost)
                            domains_needed[ldomain] = self.sharder.domain_to_shard(lhost)

                        with self.Session() as sess:
                            domain_resolver = DomainResolver(sess, domain_cache)
                            with sess.begin():
                                resolved = domain_resolver.batch_ensure_and_get(domains_needed)
                                domain_id, _ = resolved[domain]

                                src_url = rec.get("url")
                                # Score of the page these outlinks were found on,
                                # recorded as each link's parent_page_score. Skip
                                # the lookup when there are no links to score.
                                parent_page_score = (
                                    self._parent_url_score(sess, shard_id, src_url)
                                    if outlinks else None
                                )
                                new_outlinks = []
                                # Buffer every outlink's output record by its
                                # target ingestor instead of writing (and
                                # opening/closing the file) once per link.
                                by_ingestor: Dict[int, list] = {}
                                for link, lhost in outlink_hosts:
                                    if lhost is None:
                                        continue
                                    built = self._build_link_record(
                                        resolved, link, lhost, src_url, domain, parent_page_score,
                                    )
                                    if built is None:
                                        continue
                                    link_ingestor_id, link_out, summary = built
                                    by_ingestor.setdefault(link_ingestor_id, []).append(link_out)
                                    new_outlinks.append(summary)

                        out = {
                            "url": rec.get("url"),
                            "status": status,
                            "fetched_at": rec.get("fetched_at"),
                            "fail_reason": rec.get("fail_reason"),
                            "content": content,
                            "outlinks": new_outlinks,
                            "shard_id": shard_id,
                            "domain_id": domain_id,
                            "content_hash": content_hash,
                            "title": rec.get("title"),
                            "hreflang_count": rec.get("hreflang_count"),
                            "has_json_ld": rec.get("has_json_ld"),
                            "last_modified": rec.get("last_modified"),
                            "etag": rec.get("etag"),
                            "cache_control": rec.get("cache_control"),
                            "is_redirect": rec.get("is_redirect"),
                            "redirect_hop_count": rec.get("redirect_hop_count"),
                        }
                        by_ingestor.setdefault(ingestor_id, []).append(out)

                        # DB work for this record has already succeeded at
                        # this point, so write every ingestor's batch now
                        # (one open/write/close per target file, not per
                        # record within it).
                        for out_ingestor_id, records in by_ingestor.items():
                            out_dir = self._out_dir(out_ingestor_id)
                            out_path = out_dir / f"{datetime.now(timezone.utc).strftime('%H%M')}_router{self.cfg.router_id:02d}.jsonl"
                            append_jsonl_many(out_path, records)
                        break # success

                    except (OperationalError, InterfaceError) as e:
                        # connection reset / server closed / broken pipe
                        if attempt == 2:
                            logger.error(
                                "route.db_error",
                                extra={
                                    "event": "route.db_error",
                                    "domain": domain,
                                    "error": str(e),
                                },
                            )
                            error += 1
                            break

                        try:
                            self.engine.dispose()
                        except Exception:
                            pass
                        time.sleep(0.2 * (2 ** attempt))

                    except Exception as e:
                        logger.error(
                            "route.domain_error",
                            extra={
                                "event": "route.domain_error",
                                "domain": domain,
                                "error": str(e),
                            },
                        )
                        error += 1
                        break

        if error:
            self.stats.write(
                source="router",
                counters={
                    "error_count": error,
                    "route_error": error,
                },
            )
        logger.info(
            "route.folder_done",
            extra={
                "event": "route.folder_done",
                "folder": str(folder),
                "file_cnt": file_cnt,
                "errors": error,
            },
        )

    def _parent_url_score(self, sess, shard_id: int, src_url: Optional[str]) -> Optional[float]:
        """url_score of the parent page (the crawled page that emitted these
        outlinks). Stored as each link's parent_page_score so the ingestor keeps
        the highest-scoring parent. None when the page is not in
        url_state_current yet, which the ingestor ranks lowest.
        """
        if not src_url:
            return None
        row = sess.execute(
            text(f"SELECT url_score FROM url_state_current_{shard_id:03d} WHERE url = :url"),
            {"url": src_url},
        ).first()
        return float(row.url_score) if row and row.url_score is not None else None

    def _build_link_record(
        self,
        resolved: Dict[str, "tuple[int, float]"],
        link: Dict[str, str],
        host: str,
        src_url: Optional[str],
        src_domain: str,
        parent_page_score: Optional[float],
    ) -> Optional["tuple[int, Dict[str, Any], Dict[str, Any]]"]:
        """Build one outlink's new-link record from an already-resolved
        domain map (see batch_ensure_and_get in process_folder). Pure:
        no DB calls, no file I/O, so a bad link can't leave a partial
        write behind.
        """
        url = link.get("url")
        anchor = link.get("anchor")
        if not url:
            return None

        domain = self.sharder.domain_key(host)
        shard_id = self.sharder.domain_to_shard(host)
        ingestor_id = self.sharder.shard_to_ingestor(shard_id)
        domain_id, domain_score = resolved[domain]

        out = build_new_link_record(
            url=url,
            shard_id=shard_id,
            domain_id=domain_id,
            domain_score=domain_score,
            discovered_from=src_url,
            discovery_source_type=DISCOVERY_SOURCE_PAGE_OUTLINK,
            parent_page_score=parent_page_score,
            inlink_count_external=int(src_domain != domain),
            anchor_text=anchor,
            **_priority_fields(link),
        )

        summary = {
            "url": url,
            "domain_id": domain_id,
            "anchor": anchor,
        }
        return ingestor_id, out, summary


def load_router_config(path: str, router_id: int) -> RouterConfig:
    raw = load_yaml(path)
    r = require(raw, "router")
    pg = require(raw, "postgres")

    dsn = str(require(pg, "dsn"))
    with psycopg2.connect(dsn.replace("postgresql+psycopg2://", "postgresql://", 1)) as conn:
        overrides, split_subdomains = load_sharding_config(path, conn)

    return RouterConfig(
        router_id=router_id,
        crawler_dir_template=str(require(r, "crawler_dir_template")),
        ingestor_dir_template=str(require(r, "ingestor_dir_template")),
        progress_template=str(require(r, "progress_template")),
        stats_dir=str(require(r, "stats_dir")),
        interval_minutes=int(r.get("interval_minutes", 30)),
        scan_sleep_minutes=int(r.get("scan_sleep_minutes", 5)),
        num_shards=int(require(r, "num_shards")),
        shards_per_ingestor=int(require(r, "shards_per_ingestor")),
        domain_overrides=overrides,
        split_subdomains=split_subdomains,
        postgres_dsn=dsn,
    )
