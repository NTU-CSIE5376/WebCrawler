from __future__ import annotations
import logging
from pathlib import Path

from libs.ipc.jsonio import read_json, read_jsonl
from libs.stats.delta_writer import StatsDeltaWriter

from .db_ops import FeatureDB
from .extract_basic import extract_basic


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
        file_cnt = 0
        error = 0

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
                try:
                    if rec.get("status") == "ok":
                        feat = extract_basic(rec)
                        self.db.process(feat)
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
            },
        )

