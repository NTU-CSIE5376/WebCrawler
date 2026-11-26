"""Bounded ingestion keeps database batches and discovery ordering intact."""
import json
from collections import Counter
from unittest.mock import MagicMock

import pytest

from containers.scheduler_ingest.ingestor import service
from containers.scheduler_ingest.ingestor.db_ops import IngestDB
from containers.scheduler_ingest.ingestor.trap_filter import TrapFilter, TrapFilterConfig


def link(url, shard=1, **fields):
    return {"url": url, "status": "new", "shard_id": shard, **fields}


def write_records(path, records):
    path.write_text("".join(json.dumps(rec) + "\n" for rec in records), encoding="utf-8")


def test_writes_full_batches_before_input_is_exhausted(tmp_path, monkeypatch):
    monkeypatch.setattr(service, "BATCH_SIZE", 4)
    (tmp_path / "records.jsonl").touch()
    consumed = 0
    writes = []

    def records(path):
        nonlocal consumed
        for i in range(40):
            consumed += 1
            yield link(f"https://example.test/{i}", shard=i % 2)

    db = MagicMock()
    db.aggregate_links.side_effect = IngestDB.aggregate_links
    db.process_batch.side_effect = lambda batch: writes.append((consumed, len(batch))) or []
    monkeypatch.setattr(service, "read_jsonl", records)
    service.IngestService(0, db, MagicMock()).process_folder(tmp_path)
    assert writes[0][0] == 7  # Four records for shard zero, three for shard one.
    assert all(size == 4 for _, size in writes)
    assert len(writes) == 10
    assert consumed == 80  # Two passes; the second skips all discovery records.


def test_duplicates_across_batches_preserve_additive_inlinks(tmp_path, monkeypatch):
    monkeypatch.setattr(service, "BATCH_SIZE", 2)
    shared = "https://example.test/shared"
    write_records(tmp_path / "records.jsonl", [
        link(shared, inlink_count_approx=2),
        link("https://example.test/other"),
        link(shared, inlink_count_approx=3),
        link(shared, inlink_count_approx=4),
    ])
    counts = Counter()
    batch_sizes = []
    db = MagicMock()
    db.aggregate_links.side_effect = IngestDB.aggregate_links

    def upsert(batch):
        batch_sizes.append(len(batch))
        results = []
        for rec in batch:
            results.append(rec["url"] not in counts)
            counts[rec["url"]] += rec["inlink_count_approx"]
        return results

    db.process_batch.side_effect = upsert
    stats = MagicMock()
    service.IngestService(0, db, stats).process_folder(tmp_path)
    assert batch_sizes == [2, 1]
    assert counts[shared] == 9
    assert stats.write.call_args.kwargs["counters"]["new_links"] == 2


def test_filtering_and_normalization_across_batch_boundaries(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(service, "BATCH_SIZE", 2)
    write_records(tmp_path / "records.jsonl", [
        link("https://example.test/a?srsltid=1"),
        link("https://example.test/a?srsltid=2"),
        link("https://example.test/a?srsltid=3"),
        link("https://example.test/x/x/x/"),
    ])
    # JSON and JSONL are both supported; unrelated files and directories aren't.
    result = {"url": "https://example.test/a", "status": "ok", "shard_id": 1}
    (tmp_path / "result.json").write_text(json.dumps(result), encoding="utf-8")
    (tmp_path / "ignore.txt").write_text("not JSON", encoding="utf-8")
    (tmp_path / "ignore.jsonl").mkdir()
    db = MagicMock()
    db.aggregate_links.side_effect = IngestDB.aggregate_links
    batches = []
    db.process_batch.side_effect = lambda batch: batches.append(batch) or []
    with caplog.at_level("INFO", logger="ingestor"):
        service.IngestService(0, db, MagicMock(), TrapFilter(TrapFilterConfig())).process_folder(tmp_path)
    assert [batch[0]["status"] for batch in batches] == ["new", "new", "ok"]
    assert sum(rec["inlink_count_approx"] for b in batches[:2] for rec in b) == 3
    assert batches[-1] == [result]
    done = next(r for r in caplog.records if r.msg == "ingest.folder_done")
    assert (done.file_cnt, done.trap_dropped, done.url_normalized) == (2, 1, 3)


def test_failed_batch_is_counted_and_later_batches_continue(tmp_path, monkeypatch):
    monkeypatch.setattr(service, "BATCH_SIZE", 2)
    write_records(tmp_path / "records.jsonl", [link(f"https://example.test/{i}") for i in range(5)])
    db = MagicMock()
    db.aggregate_links.side_effect = IngestDB.aggregate_links
    db.process_batch.side_effect = [RuntimeError("write failed"), [True, True], [True]]
    stats = MagicMock()
    service.IngestService(0, db, stats).process_folder(tmp_path)
    counters = stats.write.call_args.kwargs["counters"]
    assert db.process_batch.call_count == 3
    assert counters["ingest_error"] == counters["error_count"] == 2
    assert counters["new_links"] == 3


def test_byte_budget_flushes_large_records_across_shards(tmp_path, monkeypatch):
    monkeypatch.setattr(service, "BATCH_SIZE", 2000)
    (tmp_path / "records.jsonl").touch()
    consumed = 0
    writes = []

    def records(path):
        nonlocal consumed
        for i in range(20):
            consumed += 1
            yield {"url": f"https://example.test/{i}", "status": "ok",
                   "shard_id": i % 4, "content": "x" * 10000}

    db = MagicMock()
    db.process_batch.side_effect = lambda batch: writes.append((consumed, len(batch))) or []
    monkeypatch.setattr(service, "read_jsonl", records)
    service.IngestService(0, db, MagicMock(), max_buffer_bytes=25000).process_folder(tmp_path)
    # The first pass skips all results. The second writes after three records,
    # before even one per shard has been read, well below the count limit.
    assert writes[0][0] == 23
    assert sum(size for _, size in writes) == 20
    assert max(size for _, size in writes) < 2000


def test_single_oversized_record_is_processed_immediately(tmp_path):
    write_records(tmp_path / "records.jsonl", [
        {"url": "https://example.test/large", "status": "ok", "shard_id": 1,
         "content": "x" * 10000},
    ])
    db = MagicMock()
    db.process_batch.return_value = []
    service.IngestService(0, db, MagicMock(), max_buffer_bytes=1000).process_folder(tmp_path)
    assert len(db.process_batch.call_args.args[0]) == 1


def test_larger_discovery_batches_preserve_result_batch_size_and_order(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(service, "BATCH_SIZE", 2)
    results = [{"url": f"https://example.test/result/{i}", "status": "ok", "shard_id": 1} for i in range(5)]
    links = [link(f"https://example.test/link/{i}") for i in range(13)]
    write_records(tmp_path / "records.jsonl", [*results, *links])
    db = MagicMock()
    db.aggregate_links.side_effect = IngestDB.aggregate_links
    batches = []
    db.process_batch.side_effect = lambda batch: batches.append(batch) or []
    with caplog.at_level("INFO", logger="ingestor"):
        service.IngestService(0, db, MagicMock(), discovery_batch_size=4).process_folder(tmp_path)
    assert [len(b) for b in batches if b[0]["status"] == "new"] == [4, 4, 4, 1]
    assert [len(b) for b in batches if b[0]["status"] != "new"] == [2, 2, 1]
    assert [r for b in batches[4:] for r in b] == results
    logged = [r for r in caplog.records if r.msg == "ingest.batch_done"]
    assert [(r.batch_kind, r.input_records, r.records) for r in logged] == [
        ("discovery", 4, 4), ("discovery", 4, 4), ("discovery", 4, 4), ("discovery", 1, 1),
        ("result", 2, 2), ("result", 2, 2), ("result", 1, 1),
    ]
    assert all(r.duration_s >= 0 for r in logged)


def test_byte_budget_still_flushes_before_larger_discovery_limit(tmp_path):
    write_records(tmp_path / "records.jsonl", [
        link(f"https://example.test/{i}", anchor_text="x" * 10000) for i in range(5)
    ])
    db = MagicMock()
    db.aggregate_links.side_effect = IngestDB.aggregate_links
    batches = []
    db.process_batch.side_effect = lambda batch: batches.append(batch) or []
    service.IngestService(0, db, MagicMock(), max_buffer_bytes=25000, discovery_batch_size=4000).process_folder(tmp_path)
    assert [len(b) for b in batches] == [3, 2]


@pytest.mark.parametrize("size", [0, -1])
def test_invalid_discovery_batch_size_is_rejected(size):
    with pytest.raises(ValueError, match="discovery_batch_size must be positive"):
        service.IngestService(0, MagicMock(), MagicMock(), discovery_batch_size=size)


@pytest.mark.parametrize("records", [[], [link("https://example.test/x/x/x/")]])
def test_empty_or_entirely_filtered_input_never_writes_empty_batch(tmp_path, records):
    write_records(tmp_path / "records.jsonl", records)
    db = MagicMock()
    db.aggregate_links.side_effect = IngestDB.aggregate_links
    service.IngestService(0, db, MagicMock(), TrapFilter(TrapFilterConfig())).process_folder(tmp_path)
    db.process_batch.assert_not_called()
