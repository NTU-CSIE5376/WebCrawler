from __future__ import annotations
import logging
import time
from pathlib import Path
from datetime import datetime, timezone

import pyarrow.compute as pc

from libs.ipc.parquet import read_parquet_batches, routed_files
from libs.stats.delta_writer import StatsDeltaWriter

from .db_ops import FeatureDB
from .extract_basic import extract_basic_batch
from .extract_youtube import extract_youtube


logger = logging.getLogger("extractor")


class ExtractService:
    def __init__(self, extractor_id: int, db: FeatureDB, stats: StatsDeltaWriter):
        self.extractor_id = extractor_id
        self.db = db
        self.stats = stats

    def process_folder(self, folder: Path):
        logger.info(
            "extract.folder_start",
            extra={"event": "extract.folder_start", "folder": str(folder)},
        )
        started = time.monotonic()
        file_cnt = 0
        error = 0
        # Crawl results seen (the folder also holds status="new" link records),
        # so Grafana can line this stage up with crawler / router / ingestor.
        records = 0

        for path in routed_files(folder, 'content'):
            file_cnt += 1
            # Keep the existing bounded read size for potentially large HTML.
            for batch in read_parquet_batches(path, 'content', batch_records=64):
                records += batch.num_rows
                successful = batch.filter(pc.equal(batch['status'], 'ok'))
                features = extract_basic_batch(successful)
                # This is only a prefilter: extract_youtube retains its exact
                # hostname validation and embedded-JSON parsing behavior.
                youtube_candidates = pc.fill_null(pc.match_substring_regex(
                    successful['url'], r'youtube\.com|youtu\.be', ignore_case=True,
                ), False).to_pylist()
                # Only compact SQL features become Python rows. HTML/outlinks
                # stay in Arrow; YouTube HTML is materialized on demand below.
                for index, feat in enumerate(features.to_pylist()):
                    try:
                        feat['shard_id'] = int(feat['shard_id'])
                        feat['domain_id'] = int(feat['domain_id'])
                        feat['fetched_at'] = (
                            datetime.fromisoformat(feat['fetched_at'])
                            if feat['fetched_at'] else datetime.now(timezone.utc)
                        )
                        if feat['content_length'] is None or feat['num_links'] is None:
                            raise TypeError('content and outlinks must not be null')
                        self.db.process(feat)
                        rec = {
                            'status': 'ok', 'url': feat['url'],
                            'fetched_at': successful['fetched_at'][index].as_py(),
                        }
                        if youtube_candidates[index]:
                            rec['content'] = successful['content'][index].as_py()
                        yt = extract_youtube(rec)
                        if yt:
                            self.db.process_youtube(yt)
                    except Exception as e:
                        logger.error(
                            "extract.record_error",
                            extra={"event": "extract.record_error", "error": str(e)},
                        )
                        error += 1
        if error:
            self.stats.write(
                source="extractor",
                counters={
                    "error_count": error,
                    "extract_error": error,
                },
            )
        logger.info(
            "extract.folder_done",
            extra={
                "event": "extract.folder_done",
                "folder": str(folder),
                "file_cnt": file_cnt,
                "error": error,
                "records": records,
                "duration_s": round(time.monotonic() - started, 1),
            },
        )

