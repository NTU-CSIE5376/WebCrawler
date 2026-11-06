from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from collections import defaultdict
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
from libs.ipc.jsonio import read_json, read_jsonl
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

# domain_id never changes for a domain and domain_score moves slowly, so the
# domain cache outlives a folder. It is dropped every DOMAIN_CACHE_TTL_SEC (to
# pick up new domain scores) or when it grows past DOMAIN_CACHE_MAX entries.
# Memory: ~200-250 B per entry, so 200k entries is ~40-50 MB per router and
# ~0.8 GB worst case across 16 routers (the container has no memory limit and
# shares the host with a YB tserver). Raise it if the hit rate is too low.
DOMAIN_CACHE_TTL_SEC = 3600.0
DOMAIN_CACHE_MAX = 200_000


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
        self._domain_cache: Dict[str, tuple[int, float]] = {}
        self._domain_cache_born = time.monotonic()
        self._made_dirs: Set[Path] = set()

    def _out_dir(self, ingestor_id: int) -> Path:
        base = Path(self.cfg.ingestor_dir_template.format(id=ingestor_id))
        date, time = current_interval(self.cfg.interval_minutes)
        return base / date / time

    def _out_path(self, ingestor_id: int) -> Path:
        out_dir = self._out_dir(ingestor_id)
        if out_dir not in self._made_dirs:
            out_dir.mkdir(parents=True, exist_ok=True)
            if len(self._made_dirs) > 10_000:
                self._made_dirs.clear()
            self._made_dirs.add(out_dir)
        return out_dir / f"{datetime.now(timezone.utc).strftime('%H%M')}_router{self.cfg.router_id:02d}.jsonl"

    def _flush(self, pending: Dict[int, list]) -> None:
        """Write one crawl record's output lines with a single append per
        ingestor file (was mkdir + open/write/close per outlink)."""
        for ingestor_id, payloads in pending.items():
            path = self._out_path(ingestor_id)
            data = "".join(
                json.dumps(p, ensure_ascii=False, separators=(",", ":")) + "\n"
                for p in payloads
            ).encode("utf-8")
            fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
            try:
                os.write(fd, data)
            finally:
                os.close(fd)

    def _domain_cache_for_folder(self) -> Dict[str, tuple[int, float]]:
        if (
            time.monotonic() - self._domain_cache_born >= DOMAIN_CACHE_TTL_SEC
            or len(self._domain_cache) > DOMAIN_CACHE_MAX
        ):
            self._domain_cache = {}
            self._domain_cache_born = time.monotonic()
        return self._domain_cache

    def process_folder(self, folder: Path) -> None:
        """
        Read all json files under folder; write transformed json to ingestor dirs.
        """
        logger.info(
            "route.folder_start",
            extra={"event": "route.folder_start", "folder": str(folder)},
        )
        started = time.monotonic()
        error = 0
        file_cnt = 0
        records = 0
        links = 0

        domain_cache = self._domain_cache_for_folder()

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
                    # Output lines are buffered per record and written only
                    # after the DB work succeeds, so a retried record no
                    # longer leaves duplicate outlink lines behind.
                    pending: Dict[int, list] = defaultdict(list)
                    try:
                        with self.Session() as sess:
                            domain_resolver = DomainResolver(sess, domain_cache)
                            with sess.begin():
                                # resolve domain_id from DB (insert if missing)
                                domain_id, _ = domain_resolver.ensure_and_get(domain, shard_id)

                                src_url = rec.get("url")
                                # Score of the page these outlinks were found on,
                                # recorded as each link's parent_page_score. Skip
                                # the lookup when there are no links to score.
                                parent_page_score = (
                                    self._parent_url_score(sess, shard_id, src_url)
                                    if outlinks else None
                                )
                                routed = self._route_links(outlinks)
                                # One SELECT for every uncached outlink domain
                                # instead of INSERT + SELECT per domain.
                                domain_resolver.batch_ensure_and_get(
                                    {d: sid for _, d, sid, _ in routed}
                                )
                                new_outlinks = []
                                for link, link_domain, link_shard, link_ingestor in routed:
                                    l = self._process_link(
                                        domain_resolver, link, src_url, domain, parent_page_score,
                                        link_domain, link_shard, link_ingestor, pending,
                                    )
                                    if l:
                                        new_outlinks.append(l)

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

                        pending[ingestor_id].append(out)
                        self._flush(pending)
                        records += 1
                        links += len(new_outlinks)
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
                "records": records,
                "links": links,
                "duration_s": round(time.monotonic() - started, 1),
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

    def _route_links(self, outlinks) -> list[tuple[Dict[str, str], str, int, int]]:
        """(link, domain, shard_id, ingestor_id) for every outlink with a url."""
        routed = []
        for link in outlinks:
            url = link.get("url")
            if not url:
                continue
            host = host_of(url)
            shard_id = self.sharder.domain_to_shard(host)
            routed.append((
                link,
                self.sharder.domain_key(host),
                shard_id,
                self.sharder.shard_to_ingestor(shard_id),
            ))
        return routed

    def _process_link(
        self,
        domain_resolver: DomainResolver,
        link: Dict[str, str],
        src_url: Optional[str],
        src_domain: str,
        parent_page_score: Optional[float],
        domain: str,
        shard_id: int,
        ingestor_id: int,
        pending: Dict[int, list],
    ) -> Optional[Dict[str, Any]]:
        url = link.get("url")
        anchor = link.get("anchor")
        if not url:
            return None

        try:
            domain_id, domain_score = domain_resolver.ensure_and_get(domain, shard_id)
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

            pending[ingestor_id].append(out)

            return {
                "url": url,
                "domain_id": domain_id,
                "anchor": anchor,
            }
        except Exception as e:
            logger.error(
                "route.link_error",
                extra={
                    "event": "route.link_error",
                    "url": url,
                    "error": str(e),
                },
            )
            raise


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
