from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from libs.ipc.jsonio import atomic_write_json
from libs.ipc.queue_scan import list_queued_domain_ids
from libs.ipc.manual_bulk_crawl import write_json
from libs.stats.delta_writer import StatsDeltaWriter, now_iso
from .job_client import CrawlJobClient
from .latency_allocation import allocate_jobs
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
    job_url_budget: int = 10000
    job_domain_slots: int = 16
    job_api_url: str = ""
    max_outstanding_job_urls: int = 16384
    job_allocation_method: str = "mip"
    job_planning_horizon_sec: float = 60
    job_probe_urls: int = 8
    job_overload_penalty: float = .2
    job_mip_time_limit_sec: float = 1

    def __post_init__(self):
        if min(self.job_url_budget, self.job_domain_slots) <= 0:
            raise ValueError("Demand budgets must be positive")
        import math
        if self.job_allocation_method not in ("mip", "greedy"):
            raise ValueError("Job allocation must be mip or greedy")
        if any(not math.isfinite(v) or v <= 0 for v in
               (self.job_planning_horizon_sec,self.job_probe_urls,self.job_overload_penalty,self.job_mip_time_limit_sec)):
            raise ValueError("Job planning parameters must be finite and positive")
        if self.max_outstanding_job_urls <= 0:
            raise ValueError("max_outstanding_job_urls must be positive")


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
        self._cycle_remaining = cfg.job_url_budget
        self.job_client = CrawlJobClient(cfg.job_api_url, cfg.offerer_id) if cfg.job_api_url else None

    def _rotated_shard_ids(self, offerer_id: int) -> tuple[list[int], int, int, int]:
        shard_start, shard_end = self.deriv.shard_range(offerer_id)
        shard_ids = list(range(shard_start, shard_end + 1))
        if not shard_ids:
            return [], shard_start, shard_end, 0

        offset = self._next_shard_offset % len(shard_ids)
        ordered = shard_ids[offset:] + shard_ids[:offset]
        self._next_shard_offset = (offset + 1) % len(shard_ids)
        return ordered, shard_start, shard_end, offset

    def _publish_domain(self, queue_dir: str, domain_id: int, urls: list[str]) -> str:
        """
        Publish selected URLs through HTTP jobs or the legacy domain-file feed.
        The HTTP outbox contains URLs and domain IDs, never business scores.
        """
        if self.job_client is not None:
            # Preserve committed claims across an API outage or lost HTTP reply.
            outbox = Path(queue_dir) / "dispatch_outbox" / f"{domain_id}.json"
            outbox.parent.mkdir(parents=True, exist_ok=True)
            reference = self.job_client.new_reference(domain_id)
            write_json(outbox, {"domain_id": domain_id, "urls": urls, "reference": reference})
            job_id = self.job_client.submit(domain_id, urls, reference=reference)
            outbox.unlink()
            return job_id
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
        self._cycle_remaining = self.cfg.job_url_budget
        if self.job_client is not None:
            outbox = Path(self.deriv.queue_dir(self.cfg.offerer_id)) / "dispatch_outbox"
            for pending in sorted(outbox.glob("*.json"), key=lambda p: p.stat().st_mtime_ns):
                message = json.loads(pending.read_text())
                if "reference" not in message:
                    message["reference"] = self.job_client.new_reference(message["domain_id"])
                    write_json(pending,message)
                self.job_client.submit(message["domain_id"], message["urls"], reconcile=True,
                                       reference=message["reference"])
                pending.unlink()
            return self._refill_jobs()
        if hasattr(self.selector, "peek_global_candidates"):
            return self._refill_global()
        if self.job_client is not None:
            raise ValueError("Job dispatch requires a global peek/claim selection strategy")
        return self._refill_per_shard()

    def _refill_jobs(self):
        pending,outstanding = self.job_client.inventory_by_domain()
        budget = max(0,min(self.cfg.job_url_budget,self.cfg.max_outstanding_job_urls-outstanding))
        if not budget:
            return {"action":"noop","queue_dir":self.cfg.job_api_url,"current_domains":len(pending)}
        performance = self.job_client.performance()
        start,end = self.deriv.shard_range(self.cfg.offerer_id)
        limit = min(self.cfg.peek_hard_cap,max(200,self.cfg.job_domain_slots*self.cfg.peek_multiplier))
        candidates = self.selector.peek_global_candidates(limit=limit,exclude_domain_ids=set(),shard_start=start,shard_end=end)
        if candidates and hasattr(self.selector,"demand_candidates"):
            candidates = self.selector.demand_candidates(
                {c.domain_id:self.cfg.per_domain_url_cap for c in candidates},start,end)
        plan,details = allocate_jobs(candidates,pending,performance,url_budget=budget,
            domain_slots=self.cfg.job_domain_slots,per_domain_cap=self.cfg.per_domain_url_cap,
            max_active_domains=self.cfg.max_domain_files,horizon_sec=self.cfg.job_planning_horizon_sec,
            probe_urls=self.cfg.job_probe_urls,overload_penalty=self.cfg.job_overload_penalty,
            method=self.cfg.job_allocation_method,solve_seconds=self.cfg.job_mip_time_limit_sec)
        logger.info("offer.job_plan",extra={"event":"offer.job_plan","plan":plan,"pending":pending,
                    "url_budget":budget,**details})
        picked=0
        for c in sorted(candidates,key=lambda c:(-c.allocation_value,c.domain_id)):
            amount=plan.get(c.domain_id,0)
            if not amount:continue
            urls=self.selector.claim_domain_urls(shard_id=c.shard_id,domain_id=c.domain_id,per_domain_cap=amount)
            if urls:
                self._publish_domain(self.deriv.queue_dir(self.cfg.offerer_id),c.domain_id,urls)
                picked+=len(urls)
                self.stats.write(source="offerer",counters={"num_scheduled":len(urls)},domains={c.domain_id:{"num_scheduled":len(urls)}})
        return {"action":"refill","queue_dir":self.cfg.job_api_url,"current_domains":len(pending),
                "new_domains":sum(key not in pending for key in plan),"picked_urls":picked}

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
            self._publish_domain(queue_dir, domain_id, urls)
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
            self._publish_domain(queue_dir, cand.domain_id, urls)
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
