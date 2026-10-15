from __future__ import annotations

import json
import os
from collections import defaultdict
from pathlib import Path
from uuid import uuid4

from w3lib.url import canonicalize_url
from urllib.parse import urlsplit

from libs.db.sharding.key import load_sharding_config
from libs.db.sharding.router import ShardRouter
from libs.ipc.folder_reader import current_interval
from libs.ipc.new_link_record import DISCOVERY_SOURCE_UNKNOWN, build_new_link_record
from containers.scheduler_ingest.router.domain_resolver import DomainResolver


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

    def submit(self, urls: list[str]) -> None:
        file_tag = uuid4().hex
        groups = defaultdict(list)
        # Domains must commit before published files can be consumed.
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
                domain, shard_id, ingestor_id = router.route(url)
                domain_id, domain_score = resolver.ensure_and_get(domain, shard_id)
                groups[ingestor_id].append(build_new_link_record(
                    url=url,
                    shard_id=shard_id,
                    domain_id=domain_id,
                    domain_score=domain_score,
                    discovered_from=None,
                    discovery_source_type=DISCOVERY_SOURCE_UNKNOWN,
                    inlink_count_approx=0,
                    inlink_count_external=0,
                ))

        published_count = 0
        try:
            for ingestor_id, records in groups.items():
                # Choose the current bucket at publication, not before DB work.
                date, bucket = current_interval(int(self.config["interval_minutes"]))
                directory = Path(self.config["ingestor_dir_template"].format(id=ingestor_id)) / date / bucket
                directory.mkdir(parents=True, exist_ok=True)
                target = directory / f"api_{file_tag}.jsonl"
                temporary = target.with_suffix(".jsonl.tmp")
                try:
                    with temporary.open("x", encoding="utf-8") as handle:
                        for record in records:
                            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.replace(temporary, target)
                finally:
                    temporary.unlink(missing_ok=True)
                published_count += len(records)
        except OSError as exc:
            # A multi-shard submission is not atomic. Expose partial publication
            # instead of suggesting that a retry is idempotent.
            raise PublicationError(published_count) from exc


class PublicationError(Exception):
    def __init__(self, published_count: int):
        super().__init__("IPC publication failed")
        self.published_count = published_count
