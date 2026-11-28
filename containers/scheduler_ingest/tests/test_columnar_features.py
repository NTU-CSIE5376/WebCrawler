"""Columnar extraction preserves scalar features and per-record failures."""
import json
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

from containers.scheduler_ingest.feature_extractor.extract_basic import extract_basic_batch
from containers.scheduler_ingest.feature_extractor.extract_youtube import extract_youtube
from containers.scheduler_ingest.feature_extractor.service import ExtractService
from containers.scheduler_ingest.tests.helpers.parquet_records import columns, write_records


def record(**values):
    result = dict(url='https://example.test/', status='ok', shard_id=0, domain_id=1,
                  fetched_at='2026-11-28T12:34:56.123456+08:00',
                  content='中文😀e\u0301', content_hash='hash',
                  outlinks=[dict(url='https://other.test/', domain_id=2, anchor='')])
    result.update(values)
    return result


def test_columnar_lengths_count_unicode_characters_and_nested_links():
    records = [record(), record(content='', outlinks=[]),
               record(content='😀'*1000, outlinks=[None]*100)]
    actual = extract_basic_batch(columns(records)['content']).to_pylist()
    for features, lengths in zip(actual, [(5, 1), (0, 0), (1000, 100)]):
        assert features == dict(url='https://example.test/', shard_id=0, domain_id=1,
                                fetched_at='2026-11-28T12:34:56.123456+08:00',
                                content_length=lengths[0], content_hash='hash', num_links=lengths[1])


def test_service_preserves_basic_and_youtube_features_in_input_order(tmp_path):
    html = 'ytInitialPlayerResponse = ' + json.dumps({'videoDetails': {
        'videoId': 'abc', 'channelId': 'xyz', 'viewCount': '123', 'title': '中文',
    }})
    records = [record(), record(url='https://WWW.YouTube.COM/watch?v=abc', content=html),
               record(url='https://youtube.com.evil.test/', content=html),
               record(status='fail'), record(url='https://youtu.be/abc', content=html)]
    write_records(tmp_path, records)
    db = MagicMock()
    ExtractService(0, db, MagicMock()).process_folder(tmp_path)
    assert [c.args[0] for c in db.process.call_args_list] == [
        dict(url=r['url'], shard_id=0, domain_id=1,
             fetched_at=datetime(2026, 11, 28, 12, 34, 56, 123456, timezone(timedelta(hours=8))),
             content_length=len(r['content']), content_hash='hash', num_links=1)
        for r in records if r['status'] == 'ok']
    assert [c.args[0] for c in db.process_youtube.call_args_list] == [
        yt for r in records if (yt := extract_youtube(r))]


def test_bad_rows_and_failed_db_write_do_not_skip_later_rows(tmp_path):
    records = [record(content=None), record(outlinks=None), record(shard_id=None),
               record(fetched_at='invalid'), record(url='http://['),
               record(url='https://db-failure.test/'), record(url='https://good.test/')]
    write_records(tmp_path, records)
    db, stats = MagicMock(), MagicMock()
    def process(row):
        if row['url'] == 'https://db-failure.test/':
            raise RuntimeError('db failure')
    db.process.side_effect = process
    ExtractService(0, db, stats).process_folder(tmp_path)
    assert [c.args[0]['url'] for c in db.process.call_args_list] == [
        'http://[', 'https://db-failure.test/', 'https://good.test/']
    assert stats.write.call_args.kwargs['counters']['extract_error'] == 6
