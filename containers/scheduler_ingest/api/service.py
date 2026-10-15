from __future__ import annotations

import json
import os
from pathlib import Path
from uuid import uuid4

from w3lib.url import canonicalize_url
from urllib.parse import urlsplit

from libs.db.sharding.key import load_sharding_config
from libs.db.sharding.router import ShardRouter
from libs.ipc.new_link_record import DISCOVERY_SOURCE_UNKNOWN, build_new_link_record
from containers.scheduler_ingest.router.domain_resolver import DomainResolver
from .queue_lock import queue_lock


MAX_URL_LEN = 2500


def validate_urls(payload: object, max_urls: int) -> list[str]:
    """Validate the whole request before any DB/file writes; deduplicate canonically."""
    if not isinstance(payload, dict) or set(payload) != {"urls"}:
        raise ValueError('Expected an object containing only "urls"')
    urls = payload["urls"]
    if not isinstance(urls, list) or not 1 <= len(urls) <= max_urls:
        raise ValueError(f'"urls" must contain 1 to {max_urls} URLs')
    normalized = {}
    for index, url in enumerate(urls):
        try:
            if not isinstance(url, str) or not url or len(url) > MAX_URL_LEN:
                raise ValueError()
            if any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in url):
                raise ValueError()
            parts = urlsplit(url)
            if parts.scheme.lower() not in {"http", "https"} or not parts.hostname:
                raise ValueError()
            if parts.username is not None or parts.password is not None or "\\" in parts.netloc:
                raise ValueError()
            # Accessing port also validates malformed / out-of-range ports.
            parts.port
            canonical = canonicalize_url(url)
            if len(canonical) > MAX_URL_LEN:
                raise ValueError()
        except (ValueError, UnicodeError):
            raise ValueError(f"Invalid HTTP(S) URL at index {index}") from None
        normalized[canonical] = None
    return list(normalized)


class SubmissionService:
    def __init__(self, session_factory, config: dict, config_path: str):
        self.session_factory = session_factory
        self.config = config["router"]
        self.config_path = config_path
        self.pending_dir = Path(config["api"]["pending_dir"])

    def submit(self, urls: list[str]) -> None:
        with queue_lock(self.pending_dir):
            self._submit_locked(urls)

    def _submit_locked(self, urls: list[str]) -> None:
        file_tag = uuid4().hex
        records = []
        # Domain writes commit before the entire batch becomes visible.
        with self.session_factory.begin() as session:
            conn = session.connection().connection
            overrides, split_subdomains = load_sharding_config(self.config_path, conn)
            router = ShardRouter(
                num_shards=int(self.config["num_shards"]),
                shards_per_ingestor=int(self.config["shards_per_ingestor"]),
                domain_overrides=overrides,
                split_subdomains=split_subdomains,
            )
            resolver = DomainResolver(session)
            for url in urls:
                domain, shard_id, _ = router.route(url)
                domain_id, domain_score = resolver.ensure_and_get(domain, shard_id)
                records.append(build_new_link_record(
                    url=url,
                    shard_id=shard_id,
                    domain_id=domain_id,
                    domain_score=domain_score,
                    discovered_from=None,
                    discovery_source_type=DISCOVERY_SOURCE_UNKNOWN,
                    inlink_count_approx=0,
                    inlink_count_external=0,
                ))

        # One rename is the acceptance boundary, regardless of shard count.
        # No fallible work follows it: a returned storage error means no batch
        # was published. The dispatcher handles downstream delivery separately.
        self.pending_dir.mkdir(parents=True, exist_ok=True)
        target = self.pending_dir / f"{file_tag}.json"
        temporary = target.with_suffix(".json.tmp")
        try:
            with temporary.open("x", encoding="utf-8") as handle:
                json.dump(records, handle, ensure_ascii=False)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
        except OSError:
            # A leftover .tmp is never accepted or read by the dispatcher.
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
            raise
