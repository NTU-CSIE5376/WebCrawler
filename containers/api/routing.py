"""Read-only routing to the same crawler that owns scheduled domain work."""
from pathlib import Path
from threading import Lock
from time import monotonic

from libs.config.loader import load_yaml
from libs.db.sharding.key import load_sharding_config
from libs.db.sharding.router import ShardRouter
from libs.urls import host_of
from sqlalchemy import bindparam, create_engine, text


class ManualBulkCrawlRouter:
    def __init__(self, config, *, clock=monotonic):
        self.config = config
        self.ingest_path = Path(config["ingest_config_path"])
        if not self.ingest_path.is_absolute():
            self.ingest_path = Path(__file__).resolve().parents[2] / self.ingest_path
        self.sharding = load_yaml(str(self.ingest_path))["router"]
        self.engine = create_engine(config["postgres"]["dsn"], pool_pre_ping=True,
                                    pool_size=2, max_overflow=1, pool_timeout=10,
                                    connect_args={"connect_timeout": 5})
        self.ttl = config["crawl_requests"]["route_cache_ttl_s"]
        self.clock = clock
        # Host -> (crawler id, expiry). Only hosts never seen within the TTL hit the DB.
        self.cache: dict[str, tuple[int, float]] = {}
        self.next_purge = 0.0
        self.lock = Lock()

    def __call__(self, urls):
        hosts = [host_of(url) for url in urls]
        now = self.clock()
        with self.lock:
            if now >= self.next_purge:
                self.cache = {host: entry for host, entry in self.cache.items() if entry[1] > now}
                self.next_purge = now + self.ttl
            found = {host: self.cache[host][0] for host in set(hosts)
                     if host in self.cache and self.cache[host][1] > now}
        missing = [host for host in dict.fromkeys(hosts) if host not in found]
        if missing:
            resolved = self._resolve(missing)
            found.update(resolved)
            with self.lock:
                self.cache.update((host, (crawler_id, now + self.ttl)) for host, crawler_id in resolved.items())
        return [found[host] for host in hosts]

    def _resolve(self, hosts):
        settings = self.sharding
        with self.engine.connect() as connection:
            connection.execute(text("SET TRANSACTION READ ONLY"))
            overrides, split = load_sharding_config(
                self.ingest_path, connection.connection.driver_connection,
            )
            router = ShardRouter(
                num_shards=int(settings["num_shards"]),
                shards_per_ingestor=int(settings["shards_per_ingestor"]),
                domain_overrides=overrides, split_subdomains=split,
            )
            routed = {host: router.route(host) for host in hosts}
            domains = list(dict.fromkeys(domain for domain, _, _ in routed.values()))
            shards = {}
            query = text("SELECT domain, shard_id FROM domain_state WHERE domain IN :domains").bindparams(
                bindparam("domains", expanding=True),
            )
            for offset in range(0, len(domains), 1000):
                shards.update((row.domain, row.shard_id) for row in connection.execute(
                    query, {"domains": domains[offset:offset + 1000]},
                ))
        # Offerer ID equals crawler ID; both own the same contiguous shard range.
        return {host: int(shards.get(domain, shard)) // int(settings["shards_per_ingestor"])
                for host, (domain, shard, _) in routed.items()}
