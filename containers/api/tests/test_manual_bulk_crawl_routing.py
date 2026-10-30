from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import tldextract
import yaml

from containers.api import routing


def test_job_routing_uses_existing_domain_ownership_and_only_reads_database(tmp_path, monkeypatch):
    monkeypatch.setattr(tldextract, "extract", tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None))
    connection = MagicMock()
    connection.execute.side_effect = [None, [SimpleNamespace(domain="example.com", shard_id=98)]]
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
    router = routing.ManualBulkCrawlRouter({
        "postgres": {"dsn": "unused"}, "ingest_config_path": str(ingest),
    })
    assert router(["https://www.example.com/a", "https://example.org/b",
                   "https://special.example.com/c"]) == [6, 2, 4]
    queries = [str(call.args[0]) for call in connection.execute.call_args_list]
    assert queries[0] == "SET TRANSACTION READ ONLY"
    assert queries[1].startswith("SELECT domain, shard_id FROM domain_state")
    assert len(queries) == 2
