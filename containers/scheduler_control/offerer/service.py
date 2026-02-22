from __future__ import annotations

import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from libs.ipc.jsonio import atomic_write_json
from libs.ipc.queue_scan import count_ready_batches
from libs.stats.delta_writer import StatsDeltaWriter, now_iso
from .selection.base import SelectionStrategy
from .batching import round_robin_mix, chunk


@dataclass(frozen=True)
class OffererDerivation:
    """
    How to derive queue dir and shard range from offerer_id.
    """
    queue_dir_template: str
    total_shards: int
    shards_per_offerer: int

    def queue_dir(self, offerer_id: int) -> str:
        return self.queue_dir_template.format(id=offerer_id)

    def shard_range(self, offerer_id: int) -> tuple[int, int]:
        start = offerer_id * self.shards_per_offerer
        end = start + self.shards_per_offerer - 1
        if start < 0 or end >= self.total_shards:
            raise ValueError(f"Offerer {offerer_id} shard range out of bounds: {start}-{end}")
        return start, end


@dataclass(frozen=True)
class OffererConfig:
    offerer_id: int

    scan_interval_sec: int
    low_watermark_batches: int
    batch_size: int
    per_shard_select_cap: int

    stats_dir: str


class OffererService:
    def __init__(
        self,
        cfg: OffererConfig,
        deriv: OffererDerivation,
        selector: SelectionStrategy,
    ):
        self.cfg = cfg
        self.deriv = deriv
        self.selector = selector
        self.stats = StatsDeltaWriter(stats_dir=cfg.stats_dir)

    def _write_one_batch_file(self, queue_dir: str, urls: list[str]) -> str:
        """
        Writes url_queue schema:
          {"generated_at": "...", "urls": [...]}
        Filename: <unix_ts>_<uuid>.json
        """
        Path(queue_dir).mkdir(parents=True, exist_ok=True)
        name = f"{int(time.time())}_{uuid.uuid4()}.json"
        final_path = str(Path(queue_dir) / name)

        payload = {"generated_at": now_iso(), "urls": urls}
        atomic_write_json(final_path, payload)
        return final_path

    def _refill_once_if_needed(self) -> dict:
        offerer_id = self.cfg.offerer_id
        queue_dir = self.deriv.queue_dir(offerer_id)
        cur_batches = count_ready_batches(queue_dir)

        if cur_batches >= self.cfg.low_watermark_batches:
            return {"action": "noop", "queue_dir": queue_dir, "current_batches": cur_batches}

        shard_start, shard_end = self.deriv.shard_range(offerer_id)
        shard_ids = list(range(shard_start, shard_end + 1))

        # Pick up to cap per shard, then mix and batch.
        per_shard_urls = defaultdict(list)
        total_picked = 0
        domain_counter = defaultdict(int)

        for sid in shard_ids:
            picked = self.selector.select_and_update(sid, self.cfg.per_shard_select_cap)
            total_picked += len(picked)
            for url, domain_id in picked:
                per_shard_urls[sid].append(url)
                domain_counter[domain_id] += 1

        # If nothing picked, do nothing.
        if total_picked == 0:
            return {
                "action": "refill_empty",
                "queue_dir": queue_dir,
                "current_batches": cur_batches,
                "picked_urls": 0,
            }

        mixed = round_robin_mix(per_shard_urls)
        parts = chunk(mixed, self.cfg.batch_size)

        written = 0
        for part in parts:
            self._write_one_batch_file(queue_dir, part)
            written += 1

        # Stats delta: offerer only knows how many urls/batches it emitted.
        self.stats.write(
            source="offerer",
            counters={
                "num_scheduled": total_picked,
            },
            domains={
                int(domain_id): {
                    "num_scheduled": cnt
                }
                for domain_id, cnt in domain_counter.items()
            }
        )

        return {
            "action": "refill",
            "queue_dir": queue_dir,
            "current_batches": cur_batches,
            "picked_urls": total_picked,
            "written_batches": written,
            "shards": {"start": shard_start, "end": shard_end},
        }

    def run_forever(self) -> None:
        while True:
            try:
                res = self._refill_once_if_needed()
                print(f"[offerer {self.cfg.offerer_id:02d}] {res}", flush=True)
            except Exception as e:
                print(f"[offerer {self.cfg.offerer_id:02d}] ERROR: {e}", flush=True)
                self.stats.write(
                    source="offerer",
                    counters={
                        "offer_error": 1,
                        "error_count": 1,
                    },
                )
            time.sleep(self.cfg.scan_interval_sec)

