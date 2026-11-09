from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from libs.ipc.jsonio import atomic_write_json
from libs.ipc.queue_scan import list_queued_domain_ids
from libs.stats.delta_writer import StatsDeltaWriter, now_iso
from .selection.base import SelectionStrategy


logger = logging.getLogger("offerer")


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

    # Used only when the strategy supports peek_global_candidates.
    # peek_limit = min(slots_to_fill * peek_multiplier, peek_hard_cap).
    peek_multiplier: int = 3
    peek_hard_cap: int = 1000


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
        self._next_shard_offset = 0

    def _rotated_shard_ids(self, offerer_id: int) -> tuple[list[int], int, int, int]:
        shard_start, shard_end = self.deriv.shard_range(offerer_id)
        shard_ids = list(range(shard_start, shard_end + 1))
        if not shard_ids:
            return [], shard_start, shard_end, 0

        offset = self._next_shard_offset % len(shard_ids)
        ordered = shard_ids[offset:] + shard_ids[:offset]
        self._next_shard_offset = (offset + 1) % len(shard_ids)
        return ordered, shard_start, shard_end, offset

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
        if hasattr(self.selector, "peek_global_candidates"):
            return self._refill_global()
        return self._refill_per_shard()

    def _refill_per_shard(self) -> dict:
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
        slots_requested = slots_to_fill

        shard_ids, shard_start, shard_end, rotation_offset = self._rotated_shard_ids(offerer_id)

        exclude = set(existing_domain_ids)
        new_domains: dict[int, list[str]] = {}
        domain_counter: dict[int, int] = defaultdict(int)
        shard_domain_counter: dict[int, int] = defaultdict(int)
        shard_url_counter: dict[int, int] = defaultdict(int)
        visited_shards: list[int] = []
        total_picked = 0

        for sid in shard_ids:
            if slots_to_fill <= 0:
                break

            visited_shards.append(sid)
            try:
                per_shard = self.selector.select_by_domain(
                    shard_id=sid,
                    exclude_domain_ids=exclude,
                    per_domain_cap=self.cfg.per_domain_url_cap,
                    max_domains=slots_to_fill,
                )
            except Exception as e:
                logger.error(
                    "offer.shard_error",
                    extra={
                        "event": "offer.shard_error",
                        "shard_id": sid,
                        "error": str(e),
                    },
                )
                self.stats.write(
                    source="offerer",
                    counters={
                        "shard_error": 1,
                        "error_count": 1,
                    },
                )
                continue

            for domain_id, urls in per_shard.items():
                if not urls:
                    continue
                new_domains[domain_id] = urls
                exclude.add(domain_id)
                domain_counter[domain_id] += len(urls)
                shard_domain_counter[sid] += 1
                shard_url_counter[sid] += len(urls)
                total_picked += len(urls)
                slots_to_fill -= 1

        if total_picked == 0:
            self._log_shard_refill(
                visited_shards=visited_shards,
                shard_domain_counter=shard_domain_counter,
                shard_url_counter=shard_url_counter,
                rotation_offset=rotation_offset,
                slots_requested=slots_requested,
            )
            return {
                "action": "refill_empty",
                "queue_dir": queue_dir,
                "current_domains": cur_count,
                "picked_urls": 0,
                "slots_requested": slots_requested,
                "shards": {"start": shard_start, "end": shard_end},
                "shard_rotation_offset": rotation_offset,
                "shards_visited": visited_shards,
                "shard_picked_urls": {},
            }

        self._log_shard_refill(
            visited_shards=visited_shards,
            shard_domain_counter=shard_domain_counter,
            shard_url_counter=shard_url_counter,
            rotation_offset=rotation_offset,
            slots_requested=slots_requested,
        )

        written = 0
        for domain_id, urls in new_domains.items():
            self._write_domain_file(queue_dir, domain_id, urls)
            written += 1

        self.stats.write(
            source="offerer",
            counters={
                "num_scheduled": total_picked,
                "offer_refill_slots_requested": slots_requested,
                "offer_refill_slots_filled": written,
                "offer_refill_shards_visited": len(visited_shards),
            },
            domains={
                int(domain_id): {"num_scheduled": cnt}
                for domain_id, cnt in domain_counter.items()
            },
            shards={
                int(shard_id): {
                    "domains": shard_domain_counter[shard_id],
                    "num_scheduled": shard_url_counter[shard_id],
                }
                for shard_id in sorted(shard_url_counter)
            },
        )

        return {
            "action": "refill",
            "queue_dir": queue_dir,
            "current_domains": cur_count,
            "new_domains": written,
            "picked_urls": total_picked,
            "slots_requested": slots_requested,
            "shards": {"start": shard_start, "end": shard_end},
            "shard_rotation_offset": rotation_offset,
            "shards_visited": visited_shards,
            "shard_picked_urls": dict(shard_url_counter),
        }

    def _log_shard_refill(
        self,
        visited_shards: list[int],
        shard_domain_counter: dict[int, int],
        shard_url_counter: dict[int, int],
        rotation_offset: int,
        slots_requested: int,
    ) -> None:
        for shard_id in visited_shards:
            logger.info(
                "offer.refill_shard",
                extra={
                    "event": "offer.refill_shard",
                    "offerer_id": self.cfg.offerer_id,
                    "shard_id": shard_id,
                    "picked_domains": shard_domain_counter.get(shard_id, 0),
                    "picked_urls": shard_url_counter.get(shard_id, 0),
                    "shard_rotation_offset": rotation_offset,
                    "slots_requested": slots_requested,
                },
            )

    def _refill_global(self) -> dict:
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
        slots_requested = slots_to_fill

        shard_start, shard_end = self.deriv.shard_range(offerer_id)
        peek_limit = min(
            slots_to_fill * self.cfg.peek_multiplier,
            self.cfg.peek_hard_cap,
        )
        peek_started = time.monotonic()
        try:
            candidates = self.selector.peek_global_candidates(
                limit=peek_limit,
                exclude_domain_ids=set(existing_domain_ids),
                shard_start=shard_start,
                shard_end=shard_end,
            )
        except Exception as e:
            logger.error(
                "offer.peek_error",
                extra={
                    "event": "offer.peek_error",
                    "error": str(e),
                    "peek_limit": peek_limit,
                    "shard_start": shard_start,
                    "shard_end": shard_end,
                },
            )
            self.stats.write(
                source="offerer",
                counters={"peek_error": 1, "error_count": 1},
            )
            return {
                "action": "refill_empty",
                "queue_dir": queue_dir,
                "current_domains": cur_count,
                "picked_urls": 0,
                "slots_requested": slots_requested,
                "shards": {"start": shard_start, "end": shard_end},
                "shards_visited": [],
                "shard_picked_urls": {},
            }

        peek_elapsed_sec = time.monotonic() - peek_started
        claim_started = time.monotonic()
        new_domains: dict[int, list[str]] = {}
        domain_counter: dict[int, int] = defaultdict(int)
        shard_domain_counter: dict[int, int] = defaultdict(int)
        shard_url_counter: dict[int, int] = defaultdict(int)
        visited_shards: list[int] = []
        skipped_empty = 0
        total_picked = 0

        for cand in candidates:
            if len(new_domains) >= slots_to_fill:
                break
            if cand.domain_id in new_domains:
                continue
            try:
                urls = self.selector.claim_domain_urls(
                    shard_id=cand.shard_id,
                    domain_id=cand.domain_id,
                    per_domain_cap=self.cfg.per_domain_url_cap,
                )
            except Exception as e:
                logger.error(
                    "offer.claim_error",
                    extra={
                        "event": "offer.claim_error",
                        "shard_id": cand.shard_id,
                        "domain_id": cand.domain_id,
                        "error": str(e),
                    },
                )
                self.stats.write(
                    source="offerer",
                    counters={"claim_error": 1, "error_count": 1},
                )
                continue
            if not urls:
                skipped_empty += 1
                continue
            new_domains[cand.domain_id] = urls
            # Each claim is committed by the selector. Publish it immediately
            # so the crawler can consume it while later domains are claimed.
            self._write_domain_file(queue_dir, cand.domain_id, urls)
            if cand.shard_id not in shard_url_counter:
                visited_shards.append(cand.shard_id)
            domain_counter[cand.domain_id] += len(urls)
            shard_domain_counter[cand.shard_id] += 1
            shard_url_counter[cand.shard_id] += len(urls)
            total_picked += len(urls)

        logger.info(
            "offer.peek",
            extra={
                "event": "offer.peek",
                "offerer_id": offerer_id,
                "peek_limit": peek_limit,
                "candidates_returned": len(candidates),
                "skipped_empty": skipped_empty,
                "slots_requested": slots_requested,
                "slots_filled": len(new_domains),
                "peek_elapsed_sec": round(peek_elapsed_sec, 3),
                "claim_publish_elapsed_sec": round(time.monotonic() - claim_started, 3),
            },
        )

        if total_picked == 0:
            self._log_shard_refill(
                visited_shards=visited_shards,
                shard_domain_counter=shard_domain_counter,
                shard_url_counter=shard_url_counter,
                rotation_offset=0,
                slots_requested=slots_requested,
            )
            return {
                "action": "refill_empty",
                "queue_dir": queue_dir,
                "current_domains": cur_count,
                "picked_urls": 0,
                "slots_requested": slots_requested,
                "shards": {"start": shard_start, "end": shard_end},
                "shards_visited": visited_shards,
                "shard_picked_urls": {},
            }

        self._log_shard_refill(
            visited_shards=visited_shards,
            shard_domain_counter=shard_domain_counter,
            shard_url_counter=shard_url_counter,
            rotation_offset=0,
            slots_requested=slots_requested,
        )

        written = len(new_domains)
        self.stats.write(
            source="offerer",
            counters={
                "num_scheduled": total_picked,
                "offer_refill_slots_requested": slots_requested,
                "offer_refill_slots_filled": written,
                "offer_refill_shards_visited": len(visited_shards),
                "offer_peek_candidates_returned": len(candidates),
                "offer_claim_empty_skipped": skipped_empty,
            },
            domains={
                int(domain_id): {"num_scheduled": cnt}
                for domain_id, cnt in domain_counter.items()
            },
            shards={
                int(shard_id): {
                    "domains": shard_domain_counter[shard_id],
                    "num_scheduled": shard_url_counter[shard_id],
                }
                for shard_id in sorted(shard_url_counter)
            },
        )

        return {
            "action": "refill",
            "queue_dir": queue_dir,
            "current_domains": cur_count,
            "new_domains": written,
            "picked_urls": total_picked,
            "slots_requested": slots_requested,
            "shards": {"start": shard_start, "end": shard_end},
            "shards_visited": visited_shards,
            "shard_picked_urls": dict(shard_url_counter),
        }

    def run_forever(self) -> None:
        while True:
            try:
                started = time.monotonic()
                res = self._refill_once_if_needed()
                logger.info(
                    "offer.refill",
                    extra={
                        "event": "offer.refill",
                        "elapsed_sec": round(time.monotonic() - started, 3),
                        "action": res.get("action"),
                        "queue_dir": res.get("queue_dir"),
                        "current_domains": res.get("current_domains"),
                        "new_domains": res.get("new_domains", 0),
                        "picked_urls": res.get("picked_urls", 0),
                        "slots_requested": res.get("slots_requested", 0),
                        "shards": res.get("shards"),
                        "shard_rotation_offset": res.get("shard_rotation_offset"),
                        "shards_visited": res.get("shards_visited"),
                        "shard_picked_urls": res.get("shard_picked_urls"),
                    },
                )
            except Exception as e:
                logger.error(
                    "offer.error",
                    extra={"event": "offer.error", "error": str(e)},
                )
                self.stats.write(
                    source="offerer",
                    counters={
                        "offer_error": 1,
                        "error_count": 1,
                    },
                )
            time.sleep(self.cfg.scan_interval_sec)
