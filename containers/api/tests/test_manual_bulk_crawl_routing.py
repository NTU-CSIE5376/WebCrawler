from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import tldextract
import yaml
from containers.api import routing


@pytest.fixture
def router(tmp_path, monkeypatch):
    monkeypatch.setattr(tldextract, "extract", tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None))
    connection = MagicMock()
    connection.execute.side_effect = lambda query, *args: (
        [SimpleNamespace(domain="example.com", shard_id=98)] if "domain_state" in str(query) else None)
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = connection
    monkeypatch.setattr(routing, "create_engine", lambda *args, **kwargs: engine)
    ingest = tmp_path / "ingest.yaml"
    ingest.write_text(yaml.safe_dump({"router": {
        "num_shards": 128, "shards_per_ingestor": 16,
        "domain_overrides": {"example.com": 0, "example.org": 37, "special.example.com": 65},
    }}))
    monkeypatch.setattr(routing, "load_sharding_config", lambda path, connection: (
        yaml.safe_load(Path(path).read_text())["router"]["domain_overrides"], {"special.example.com"},
    ))
    now = [0.0]
    router = routing.ManualBulkCrawlRouter({
        "postgres": {"dsn": "unused"}, "ingest_config_path": str(ingest),
        "crawl_requests": {"route_cache_ttl_s": 100},
    }, clock=lambda: now[0])
    return router, connection, now


def queries(connection):
    return [str(call.args[0]) for call in connection.execute.call_args_list]


def test_routing_uses_existing_domain_ownership_and_only_reads_database(router):
    router, connection, _ = router
    assert router(["https://www.example.com/a", "https://example.org/b",
                   "https://special.example.com/c"]) == [6, 2, 4]
    assert queries(connection)[0] == "SET TRANSACTION READ ONLY"
    assert queries(connection)[1].startswith("SELECT domain, shard_id FROM domain_state")
    assert len(queries(connection)) == 2


def test_known_hosts_are_cached_until_the_ttl_expires(router):
    router, connection, now = router
    router(["https://www.example.com/a"])
    assert router(["https://www.example.com/b", "https://www.example.com/c"]) == [6, 6]
    assert len(queries(connection)) == 2, "Cached hosts need no database lookup"
    assert router(["https://www.example.com/d", "https://example.org/x"]) == [6, 2]
    assert len(queries(connection)) == 4, "Only the new host is resolved"
    now[0] = 100
    router(["https://www.example.com/e"])
    assert len(queries(connection)) == 6


def test_database_failure_propagates_and_caches_nothing(router):
    router, connection, _ = router
    connection.execute.side_effect = OSError("down")
    with pytest.raises(OSError):
        router(["https://www.example.com/a"])
    assert not router.cache
