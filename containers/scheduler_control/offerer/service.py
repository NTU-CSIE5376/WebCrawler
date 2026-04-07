from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from libs.ipc.jsonio import atomic_write_json
from libs.ipc.queue_scan import list_queued_domain_ids, count_domain_files
from libs.stats.delta_writer import StatsDeltaWriter, now_iso
from .selection.base import SelectionStrategy


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
    max_domain_files: int
    low_watermark_domains: int
    per_domain_url_cap: int

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

    def _write_domain_file(self, queue_dir: str, domain_id: int, urls: list[str]) -> str:
        """
        Writes one per-domain queue file:
          {"generated_at": "...", "domain_id": N, "urls": [...]}
        Filename: domain_{domain_id:06d}.json
        """
        Path(queue_dir).mkdir(parents=True, exist_ok=True)
        name = f"domain_{domain_id:06d}.json"
        final_path = str(Path(queue_dir) / name)

        payload = {
            "generated_at": now_iso(),
            "domain_id": domain_id,
            "urls": urls,
        }
        atomic_write_json(final_path, payload)
        return final_path

    def _refill_once_if_needed(self) -> dict:
        offerer_id = self.cfg.offerer_id
        queue_dir = self.deriv.queue_dir(offerer_id)

        existing_domain_ids = list_queued_domain_ids(queue_dir)
        cur_count = len(existing_domain_ids)

        if cur_count >= self.cfg.low_watermark_domains:
            return {
                "action": "noop",
                "queue_dir": queue_dir,
                "current_domains": cur_count,
            }

        slots_to_fill = self.cfg.max_domain_files - cur_count
        if slots_to_fill <= 0:
            return {
                "action": "noop",
                "queue_dir": queue_dir,
                "current_domains": cur_count,
            }

        shard_start, shard_end = self.deriv.shard_range(offerer_id)
        shard_ids = list(range(shard_start, shard_end + 1))

        exclude = set(existing_domain_ids)
        new_domains: dict[int, list[str]] = {}
        domain_counter: dict[int, int] = defaultdict(int)
        total_picked = 0

        for sid in shard_ids:
            if slots_to_fill <= 0:
                break

            per_shard = self.selector.select_by_domain(
                shard_id=sid,
                exclude_domain_ids=exclude,
                per_domain_cap=self.cfg.per_domain_url_cap,
                max_domains=slots_to_fill,
            )

            for domain_id, urls in per_shard.items():
                if not urls:
                    continue
                new_domains[domain_id] = urls
                exclude.add(domain_id)
                domain_counter[domain_id] += len(urls)
                total_picked += len(urls)
                slots_to_fill -= 1

        if total_picked == 0:
            return {
                "action": "refill_empty",
                "queue_dir": queue_dir,
                "current_domains": cur_count,
                "picked_urls": 0,
            }

        written = 0
        for domain_id, urls in new_domains.items():
            self._write_domain_file(queue_dir, domain_id, urls)
            written += 1

        self.stats.write(
            source="offerer",
            counters={
                "num_scheduled": total_picked,
            },
            domains={
                int(domain_id): {"num_scheduled": cnt}
                for domain_id, cnt in domain_counter.items()
            },
        )

        return {
            "action": "refill",
            "queue_dir": queue_dir,
            "current_domains": cur_count,
            "new_domains": written,
            "picked_urls": total_picked,
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
