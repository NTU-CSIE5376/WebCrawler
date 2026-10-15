import json
from unittest.mock import Mock

import pytest

from containers.scheduler_ingest.ingestor.service import IngestService


def test_request_failure_prevents_folder_from_being_checkpointed(tmp_path):
    (tmp_path / "api.jsonl").write_text(json.dumps({
        "url": "https://example.com/a", "status": "crawl_request", "shard_id": 3,
    }) + "\n")
    db = Mock()
    db.aggregate_links.return_value = []
    db.process_batch.side_effect = RuntimeError("temporary failure")
    service = IngestService(0, db, Mock())
    with pytest.raises(RuntimeError, match="temporary failure"):
        service.process_folder(tmp_path)
