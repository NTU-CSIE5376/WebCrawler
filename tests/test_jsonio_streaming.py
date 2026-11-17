"""JSONL input must not retain file-sized lists in long-running consumers."""
import json
import tracemalloc

import pytest

from libs.ipc.jsonio import read_jsonl


def test_stream_preserves_values_order_and_blank_line_handling(tmp_path):
    path = tmp_path / "records.jsonl"
    values = [{"title": "中文"}, None, [1, 2], 3]
    path.write_text(
        "\n \t\n" + "\n\n".join(json.dumps(v, ensure_ascii=False) for v in values),
        encoding="utf-8",
    )
    assert list(read_jsonl(path)) == values


def test_records_are_available_before_later_parse_errors(tmp_path):
    path = tmp_path / "records.jsonl"
    path.write_text('{"ok": true}\ninvalid json\n', encoding="utf-8")
    records = read_jsonl(path)
    assert next(records) == {"ok": True}
    with pytest.raises(json.JSONDecodeError):
        next(records)


def test_reading_large_file_keeps_memory_bounded(tmp_path):
    path = tmp_path / "large.jsonl"
    line = json.dumps({"content": "x" * 1024}) + "\n"
    count = 20_000
    with path.open("w", encoding="utf-8") as output:
        for _ in range(count):
            output.write(line)

    tracemalloc.start()
    try:
        assert sum(1 for _ in read_jsonl(path)) == count
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    # Input exceeds 20 MB; an eager list would retain all decoded payloads.
    assert peak < 2 * 1024 * 1024
