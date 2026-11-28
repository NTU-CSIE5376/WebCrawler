"""ingest.folder_done / extract.folder_done carry duration_s for the Grafana
pipeline row (folder_start -> folder_done can't be paired in LogQL)."""
from __future__ import annotations

import logging

from containers.scheduler_ingest.feature_extractor.service import ExtractService
from containers.scheduler_ingest.ingestor.content_ingestor.service import ContentIngestor
from containers.scheduler_ingest.ingestor.libs.worker import Checkpoint


class _Stats:
    def write(self, **kwargs):
        pass


class _IngestDB:
    aggregate_links = staticmethod(lambda recs: recs)

    def process_batch(self, batch):
        return [None] * len(batch)


def _done(caplog, event):
    (rec,) = [r for r in caplog.records if getattr(r, "event", None) == event]
    return rec


def test_ingest_folder_done_has_duration(tmp_path, caplog):
    svc = ContentIngestor(_IngestDB(), _Stats(),
                         Checkpoint(tmp_path/'content.json', tmp_path/'legacy.json', tmp_path/'discovery.json'))
    folder = tmp_path/'input'
    folder.mkdir()
    with caplog.at_level(logging.INFO, logger="content_ingestor"):
        svc.process_folder(folder, "20261128", "0000")
    assert _done(caplog, "ingest.folder_done").duration_s >= 0


def test_extract_folder_done_has_duration(tmp_path, caplog):
    svc = ExtractService(0, db=None, stats=_Stats())
    with caplog.at_level(logging.INFO):
        svc.process_folder(tmp_path)
    assert _done(caplog, "extract.folder_done").duration_s >= 0


def test_extract_folder_done_counts_crawl_results_not_links(tmp_path, caplog):
    from containers.scheduler_ingest.tests.helpers.parquet_records import write_records
    write_records(tmp_path, [
        {"url":"https://a.example/", "status":"fail", "fail_reason":"HttpError 404"},
        {"url":"https://a.example/x", "status":"new"},
        {"url":"https://a.example/y", "status":"new"},
    ])
    svc = ExtractService(0, db=None, stats=_Stats())
    with caplog.at_level(logging.INFO):
        svc.process_folder(tmp_path)
    done = _done(caplog, "extract.folder_done")
    assert (done.records, done.error) == (1, 0)
