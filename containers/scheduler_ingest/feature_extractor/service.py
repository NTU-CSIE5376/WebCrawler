from __future__ import annotations
import logging
import time
from pathlib import Path

from libs.ipc.jsonio import read_json, read_jsonl
from libs.stats.delta_writer import StatsDeltaWriter

from .db_ops import FeatureDB
from .extract_basic import extract_basic
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

        for f in folder.iterdir():
            if not f.is_file():
                continue

            if f.suffix == ".json":
                recs = [read_json(f)]
                file_cnt += 1
            elif f.suffix == ".jsonl":
                recs = read_jsonl(f)
                file_cnt += 1
            else:
                continue

            for rec in recs:
                if rec.get("status") != "new":
                    records += 1
                try:
                    # Redirect-source and crawler-deduped records only carry
                    # a fetch outcome; content and features stay on the final
                    # URL. Any other ok record without content (e.g. written
                    # by a router that predates redirect dedup) has nothing to
                    # extract either, so skip it instead of erroring.
                    if (
                        rec.get("status") == "ok"
                        and not rec.get("redirect_source")
                        and not rec.get("redirect_deduped")
                        and isinstance(rec.get("content"), str)
                    ):
                        feat = extract_basic(rec)
                        self.db.process(feat)
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

