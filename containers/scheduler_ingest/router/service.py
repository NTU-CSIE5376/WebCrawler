from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict, Optional, Set, Tuple


logger = logging.getLogger("router")

import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, InterfaceError

from libs.config.loader import load_yaml, require
from libs.db.sharding.key import load_sharding_config
from libs.db.sharding.router import ShardRouter, host_of
from libs.ipc.jsonio import read_json, read_jsonl, append_jsonl
from libs.ipc.new_link_record import (
    DISCOVERY_SOURCE_PAGE_OUTLINK,
    build_new_link_record,
)
from libs.stats.delta_writer import StatsDeltaWriter
from libs.ipc.folder_reader import current_interval

from .domain_resolver import DomainResolver


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="replace")).hexdigest()


# Record status for "this fetch result belongs to the scheduled row too".
# The ingestor only UPDATEs an existing row with it (see IngestDB._bulk_requested).
STATUS_REQUESTED = "requested"


def build_requested_record(rec: Dict[str, Any], requested_url: str, shard_id: int) -> Dict[str, Any]:
    """Carry the fetch outcome back to the url_state_current row the spider was
    asked to fetch, when the result itself is keyed under another URL: after an
    HTTP redirect (result lands on canonicalize_url(response.url)), or when the
    scheduled row is stored under a non-canonical string (e.g. raw golden URLs
    injected before golden_inject canonicalized them). Without this the
    scheduled row never gets last_fetch_ok and reads as "never crawled"."""
    return {
        "status": STATUS_REQUESTED,
        "url": requested_url,
        "shard_id": shard_id,
        "domain_id": rec.get("requested_domain_id"),
        "fetched_at": rec.get("fetched_at"),
        "result_status": rec.get("status"),
        "fail_reason": rec.get("fail_reason"),
        "final_url": rec.get("url"),
        "is_redirect": rec.get("is_redirect"),
        "redirect_hop_count": rec.get("redirect_hop_count"),
    }

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

        domain_cache = {}
        # domain_id -> shard_id of the scheduled row, for STATUS_REQUESTED records
        requested_shard_cache: Dict[int, int] = {}

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
                                new_outlinks = []
                                for link in outlinks:
                                    l = self._process_link(
                                        domain_resolver, link, src_url, domain, parent_page_score
                                    )
                                    if l:
                                        new_outlinks.append(l)

                                requested = self._requested_row(sess, rec, requested_shard_cache)

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

                        out_dir = self._out_dir(ingestor_id)
                        out_dir.mkdir(parents=True, exist_ok=True)
                        out_path = out_dir / f"{datetime.now(timezone.utc).strftime('%H%M')}_router{self.cfg.router_id:02d}.jsonl"
                        append_jsonl(out_path, out)

                        if requested:
                            req_url, req_shard = requested
                            req_dir = self._out_dir(self.sharder.shard_to_ingestor(req_shard))
                            req_dir.mkdir(parents=True, exist_ok=True)
                            req_path = req_dir / f"{datetime.now(timezone.utc).strftime('%H%M')}_router{self.cfg.router_id:02d}.jsonl"
                            append_jsonl(req_path, build_requested_record(rec, req_url, req_shard))
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

    def _requested_row(
        self, sess, rec: Dict[str, Any], shard_cache: Dict[int, int]
    ) -> Optional[Tuple[str, int]]:
        """(url, shard_id) of the scheduled row when the result is written under
        a different URL, else None. The shard comes from the scheduled row's own
        domain_id (domain_state.shard_id), not from re-hashing its host: rows
        written by older inject scripts can sit in a different shard than the
        router would pick today."""
        req_url = rec.get("requested_url")
        if not req_url or req_url == rec.get("url"):
            return None

        domain_id = int(rec.get("requested_domain_id") or 0)
        shard_id = shard_cache.get(domain_id) if domain_id else None
        if shard_id is None and domain_id:
            row = sess.execute(
                text("SELECT shard_id FROM domain_state WHERE domain_id = :d"),
                {"d": domain_id},
            ).first()
            if row is not None:
                shard_id = int(row.shard_id)
                shard_cache[domain_id] = shard_id
        if shard_id is None:
            shard_id = self.sharder.domain_to_shard(host_of(req_url))
        return req_url, shard_id

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

    def _process_link(
        self,
        domain_resolver: DomainResolver,
        link: Dict[str, str],
        src_url: Optional[str],
        src_domain: str,
        parent_page_score: Optional[float],
    ) -> Optional[Dict[str, Any]]:
        url = link.get("url")
        anchor = link.get("anchor")
        if not url:
            return None

        host = host_of(url)
        domain = self.sharder.domain_key(host)
        shard_id = self.sharder.domain_to_shard(host)
        ingestor_id = self.sharder.shard_to_ingestor(shard_id)

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
            )

            out_dir = self._out_dir(ingestor_id)
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"{datetime.now(timezone.utc).strftime('%H%M')}_router{self.cfg.router_id:02d}.jsonl"
            append_jsonl(out_path, out)

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
