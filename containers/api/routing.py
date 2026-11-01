"""Read-only routing to the same crawler that owns scheduled domain work."""
from pathlib import Path

from sqlalchemy import bindparam, create_engine, text

from libs.config.loader import load_yaml
from libs.db.sharding.key import load_sharding_config
from libs.db.sharding.router import ShardRouter


class ManualBulkCrawlRouter:
    def __init__(self, config):
        self.config = config
        self.ingest_path = Path(config["ingest_config_path"])
        if not self.ingest_path.is_absolute():
            self.ingest_path = Path(__file__).resolve().parents[2] / self.ingest_path
        self.sharding = load_yaml(str(self.ingest_path))["router"]
        self.engine = create_engine(config["postgres"]["dsn"], pool_pre_ping=True,
                                    pool_size=2, max_overflow=1, pool_timeout=10,
                                    connect_args={"connect_timeout": 5})

    def __call__(self, urls):
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
            routed = [router.route(url) for url in urls]
            domains = list(dict.fromkeys(domain for domain, _, _ in routed))
            shards = {}
            query = text("SELECT domain, shard_id FROM domain_state WHERE domain IN :domains").bindparams(
                bindparam("domains", expanding=True),
            )
            for offset in range(0, len(domains), 1000):
                shards.update((row.domain, row.shard_id) for row in connection.execute(
                    query, {"domains": domains[offset:offset + 1000]},
                ))
        # Offerer ID equals crawler ID; both own the same contiguous shard range.
        return [int(shards.get(domain, shard)) // int(settings["shards_per_ingestor"])
                for domain, shard, _ in routed]
