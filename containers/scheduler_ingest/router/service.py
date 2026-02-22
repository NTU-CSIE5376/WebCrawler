from __future__ import annotations

import hashlib
import uuid
import time
from datetime import datetime, timezone
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, InterfaceError

from libs.config.loader import load_yaml, require
from libs.ipc.jsonio import read_json, atomic_write_json
from libs.stats.delta_writer import StatsDeltaWriter
from libs.ipc.folder_reader import current_interval

from .routing import ShardRouter
from .domain_resolver import DomainResolver


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

    postgres_dsn: str


class RouterService:
    def __init__(self, cfg: RouterConfig):
        self.cfg = cfg
        self.sharder = ShardRouter(
            num_shards=self.cfg.num_shards,
            shards_per_ingestor=self.cfg.shards_per_ingestor,
            domain_overrides=self.cfg.domain_overrides,
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
        error = 0

        for f in sorted(folder.glob("*.json")):
            rec = read_json(f)

            domain = rec.get("domain")
            status = rec.get("status")  # "ok"/"fail"
            content = rec.get("content")
            outlinks = rec.get("outlinks", [])

            shard_id = self.sharder.domain_to_shard(domain)
            ingestor_id = self.sharder.shard_to_ingestor(shard_id)

            content_hash = None
            if status == "ok" and isinstance(content, str):
                content_hash = sha1_hex(content)

            for attempt in range(3):
                try:
                    with self.Session() as sess:
                        domain_resolver = DomainResolver(sess)
                        with sess.begin():
                            # resolve domain_id from DB (insert if missing)
                            domain_id, _ = domain_resolver.ensure_and_get(domain, shard_id)

                            new_outlinks = []
                            for link in outlinks:
                                l = self._process_link(domain_resolver, link)
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
                    }

                    out_dir = self._out_dir(ingestor_id)
                    out_dir.mkdir(parents=True, exist_ok=True)
                    out_path = out_dir / f.name
                    atomic_write_json(out_path, out)
                    break # success

                except (OperationalError, InterfaceError) as e:
                    # connection reset / server closed / broken pipe
                    if attempt == 2:
                        print(f"[router {self.cfg.router_id:02d}] db error {domain}: {e}", flush=True)
                        error += 1
                        break

                    try:
                        self.engine.dispose()
                    except Exception:
                        pass
                    time.sleep(0.2 * (2 ** attempt))

                except Exception as e:
                    print(f"[router {self.cfg.router_id:02d}] domain resolve error {domain}: {e}", flush=True)
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

    def _process_link(self, domain_resolver: DomainResolver, link: Dict[str, str]) -> Optional[Dict[str, Any]]:
        url = link.get("url")
        domain = link.get("domain")
        anchor = link.get("anchor")
        if not url:
            return None

        shard_id = self.sharder.domain_to_shard(domain)
        ingestor_id = self.sharder.shard_to_ingestor(shard_id)

        try:
            domain_id, domain_score = domain_resolver.ensure_and_get(domain, shard_id)
            out = {
                "url": url,
                "status": "new",
                "shard_id": shard_id,
                "domain_id": domain_id,
                "domain_score": domain_score,
            }

            out_dir = self._out_dir(ingestor_id)
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"{uuid.uuid4().hex}.json"

            atomic_write_json(out_path, out)

            return {
                "url": url,
                "domain_id": domain_id,
                "anchor": anchor,
            }
        except Exception as e:
            print(f"[router {self.cfg.router_id:02d}] process link error {link}: {e}", flush=True)
            raise


def load_router_config(path: str, router_id: int) -> RouterConfig:
    raw = load_yaml(path)
    r = require(raw, "router")
    pg = require(raw, "postgres")

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
        domain_overrides=r.get("domain_overrides", {}) or {},
        postgres_dsn=str(require(pg, "dsn")),
    )

