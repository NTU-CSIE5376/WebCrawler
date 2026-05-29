from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from psycopg2.extras import execute_values
from sqlalchemy.orm import sessionmaker

from libs.scoring.golden_discovery_runtime import GoldenDiscoveryRuntimeScorer


logger = logging.getLogger("golden_discovery_ranker_v1")


@dataclass(frozen=True)
class GoldenDiscoveryRankerConfig:
    total_shards: int
    num_workers: int
    worker_id: int
    batch_size: int
    scan_interval_sec: int
    max_batches_per_shard: int
    # Domain-priority steering: when enabled, each batch only picks URLs from
    # domains with `domain_state.domain_score > 0` (i.e. domains that have
    # appeared in a golden batch). Ordered by domain_score DESC so higher-tier
    # golden domains are scored first. When disabled, the scorer keeps the
    # legacy first_seen-ASC behavior.
    domain_priority_steering_enabled: bool = False
    # v2 continuous re-scoring (steering path only). The v2 ranker's features
    # (inlink_count_*, anchor_text) change over time, so url_score is no longer
    # a write-once value — it must be refreshed.
    #   rescore_ttl_days: re-pick a URL once its score is older than this many
    #     days (in addition to never-scored rows). Sized so one full pass over
    #     the steering-eligible pool finishes well inside the TTL; see deploy
    #     notes. 0 disables TTL refresh (write-once, never re-score).
    #   min_age_days: do NOT score a URL until it has been in the DB this long,
    #     letting inlinks/anchors accumulate first. Younger rows keep their v1
    #     inline score until they age in. 0 disables the gate.
    rescore_ttl_days: int = 5
    min_age_days: int = 3


class GoldenDiscoveryRankerService:
    def __init__(
        self,
        cfg: GoldenDiscoveryRankerConfig,
        Session: sessionmaker,
        scorer: GoldenDiscoveryRuntimeScorer,
    ):
        self.cfg = cfg
        self.Session = Session
        self.scorer = scorer

    @staticmethod
    def _table(shard_id: int) -> str:
        return f"url_state_current_{shard_id:03d}"

    def _shard_ids(self) -> list[int]:
        """Order in which this worker visits shards on each run_once.

        Every worker visits every shard. Workers stagger their starting
        offset by `total_shards // num_workers` so the four (or N) of
        them don't all queue up on shard 0 first — but the static
        partition is gone, so a worker that finishes its starting
        section keeps walking and picks up shards that another worker
        would previously have owned. URL-level FOR UPDATE SKIP LOCKED
        inside `_score_batch` handles the resulting concurrent claims
        on the same shard.

        Replaces the prior static partition (worker N saw shards
        [N, N + num_workers, ...]) which could not reassign work when
        one worker drew a disproportionate share of the very large
        (256M-row) shards while other workers sat idle after draining
        their own assignment.
        """
        if self.cfg.num_workers <= 0:
            return list(range(self.cfg.total_shards))
        step = max(1, self.cfg.total_shards // self.cfg.num_workers)
        offset = (self.cfg.worker_id * step) % self.cfg.total_shards
        return [
            (offset + i) % self.cfg.total_shards
            for i in range(self.cfg.total_shards)
        ]

    def _score_batch(self, shard_id: int) -> int:
        table = self._table(shard_id)

        with self.Session.begin() as sess:
            with sess.connection().connection.cursor() as cur:
                if self.cfg.domain_priority_steering_enabled:
                    # Pick due URLs from golden-tier domains, as a LATERAL
                    # top-N-per-domain.  WHY this shape and not a flat
                    # `JOIN domain_state ... ORDER BY d.domain_score DESC,
                    # u.url_score_updated_at`: the flat form forces a blocking
                    # Sort, because the lead ORDER BY key (domain_score) lives
                    # on domain_state and has only ~3 distinct values, so within
                    # a tier url_score_updated_at must be merged ACROSS all
                    # domains — no index can produce that.  The Sort blocks the
                    # LIMIT, so the planner materialises every matching row (in
                    # the backlog regime ~all rows) via a Seq Scan.  Measured on
                    # a 64M-row shard the flat plan cost ~11.5M and ignored the
                    # rescore index entirely.
                    #
                    # The LATERAL drives from domain_state (golden domains for
                    # this shard, via idx_domain_state_shard_score, ordered
                    # domain_score DESC) and for each domain does a plain ordered
                    # index scan on idx_..._golden_discovery_v2_rescore
                    # (domain_id, url_score_updated_at) to take its stalest due
                    # URLs.  No big scan; the only Sort is a top-N heapsort over
                    # the few-thousand rows actually pulled.
                    #
                    # `d.shard_id = %s` is required AND correct: every domain in
                    # url_state_current_{shard} has domain_state.shard_id=shard,
                    # and the equality lets the shard_score index yield
                    # domain_score DESC order (its lead column is shard_id).
                    #
                    # Gates (unchanged): min_age skips URLs younger than
                    # min_age_days (they keep the v1 inline score; 0 = no-op);
                    # TTL re-picks never-scored OR rows older than
                    # rescore_ttl_days. rescore_ttl_days<=0 = write-once (drop
                    # the OR branch, not `< NOW() - interval '0'` which would
                    # match every already-scored row).
                    params: list = [self.cfg.min_age_days]
                    if self.cfg.rescore_ttl_days > 0:
                        ttl_clause = (
                            "(u.url_score_updated_at IS NULL "
                            "OR u.url_score_updated_at < NOW() - make_interval(days => %s))"
                        )
                        params.append(self.cfg.rescore_ttl_days)
                    else:
                        ttl_clause = "u.url_score_updated_at IS NULL"
                    # %s order matches text order: min_age, [ttl], inner LIMIT,
                    # shard_id, outer LIMIT.
                    params.extend([self.cfg.batch_size, shard_id, self.cfg.batch_size])
                    cur.execute(
                        f"""
                        SELECT u.url, u.inlink_count_approx, u.inlink_count_external, u.anchor_text
                        FROM domain_state d
                        CROSS JOIN LATERAL (
                            SELECT url, inlink_count_approx, inlink_count_external,
                                   anchor_text, url_score_updated_at
                            FROM {table} u
                            WHERE u.domain_id = d.domain_id
                              AND u.should_crawl = TRUE
                              AND u.first_seen < NOW() - make_interval(days => %s)
                              AND {ttl_clause}
                            ORDER BY u.url_score_updated_at ASC NULLS FIRST
                            LIMIT %s
                            FOR UPDATE OF u SKIP LOCKED
                        ) u
                        WHERE d.shard_id = %s
                          AND d.domain_score > 0
                        ORDER BY d.domain_score DESC,
                                 u.url_score_updated_at ASC NULLS FIRST
                        LIMIT %s
                        """,
                        tuple(params),
                    )
                else:
                    cur.execute(
                        f"""
                        SELECT url, inlink_count_approx, inlink_count_external, anchor_text
                        FROM {table}
                        WHERE should_crawl = TRUE
                          AND url_score_updated_at IS NULL
                        ORDER BY first_seen ASC NULLS LAST
                        FOR UPDATE SKIP LOCKED
                        LIMIT %s
                        """,
                        (self.cfg.batch_size,),
                    )
                fetched = cur.fetchall()
                if not fetched:
                    return 0
                urls = [row[0] for row in fetched]

                # v2 ranker consumes prefetch features (inlink/anchor) via
                # score_many_rows; v1 only needs the URL string. Detect by
                # capability so this path stays drop-in for both.
                if hasattr(self.scorer, "score_many_rows"):
                    recs = [
                        {
                            "url": row[0],
                            "inlink_count_approx": row[1],
                            "inlink_count_external": row[2],
                            "anchor_text": row[3],
                        }
                        for row in fetched
                    ]
                    scores = self.scorer.score_many_rows(recs)
                else:
                    scores = self.scorer.score_many(urls)
                rows = list(zip(urls, scores))

                # Keep score history compact: the ranker refreshes
                # current.url_score in place and uses url_score_updated_at as
                # the only completion bit.
                execute_values(
                    cur,
                    f"""
                    UPDATE {table} AS u
                    SET
                        url_score = v.score::double precision,
                        url_score_updated_at = CURRENT_TIMESTAMP
                    FROM (VALUES %s) AS v(url, score)
                    WHERE u.url = v.url
                    """,
                    rows,
                    page_size=len(rows),
                )
                scored = len(rows)

        logger.info(
            "golden_discovery_ranker_v1.score_batch",
            extra={
                "event": "golden_discovery_ranker_v1.score_batch",
                "worker_id": self.cfg.worker_id,
                "shard_id": shard_id,
                "scored_urls": scored,
            },
        )
        return scored

    def run_once(self) -> dict[str, int]:
        totals = {"scored_urls": 0, "scored_batches": 0}

        for shard_id in self._shard_ids():
            batches = 0
            while batches < self.cfg.max_batches_per_shard:
                count = self._score_batch(shard_id)
                if count == 0:
                    break
                batches += 1
                totals["scored_batches"] += 1
                totals["scored_urls"] += count

        logger.info(
            "golden_discovery_ranker_v1.run_once",
            extra={
                "event": "golden_discovery_ranker_v1.run_once",
                "worker_id": self.cfg.worker_id,
                **totals,
            },
        )
        return totals

    def run_forever(self) -> None:
        while True:
            try:
                self.run_once()
            except Exception as e:
                logger.error(
                    "golden_discovery_ranker_v1.error",
                    extra={
                        "event": "golden_discovery_ranker_v1.error",
                        "worker_id": self.cfg.worker_id,
                        "error": str(e),
                    },
                )
            time.sleep(self.cfg.scan_interval_sec)
