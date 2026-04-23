# 01. System Architecture

## 1.1 Purpose

This system is a distributed crawler scheduler pipeline that:

- selects crawl candidates from sharded URL state tables,
- dispatches URL batches to crawler workers via filesystem queues,
- crawls pages and writes raw results,
- routes and enriches results with domain/shard metadata,
- ingests crawl outcomes into operational URL tables,
- extracts content features,
- aggregates service-level and domain-level stats.

## 1.2 High-Level Topology

There are four Docker services:

- `postgres`: persistent storage for all metadata and counters.
- `scheduler_control`: runs 16 `offerer` processes that schedule URLs from DB into crawler queues.
- `crawler`: runs 16 Scrapy spiders consuming queue batches and producing crawl result files.
- `scheduler_ingest`: runs 49 processes total:
  - 16 `router`
  - 16 `ingestor`
  - 16 `feature_extractor`
  - 1 `stats_aggregator`

Data exchange between non-DB services is file-based under `/data/ipc`.

## 1.3 Parallelism and Sharding Model

- Total shards: `256` (`0..255`).
- Offerer count: `16`, each owning `16` shards (`shards_per_offerer=16`).
- Accounting rolloff worker count: `1` (in `scheduler_control`), scans all `256` shards daily.
- Router/Ingestor/Extractor count: `16`, each responsible for `16` shards (`shards_per_ingestor=16`).
- Crawlers: `16` queue consumers (`crawler_00..15`).

Shard mapping rules:

- `domain -> shard_id`: explicit override for selected top domains, otherwise `md5(domain) % 256`. eTLD+1s listed in `containers/scheduler_ingest/config/shard_split.yaml` are hashed per-hostname instead, and their `domain_overrides` entry (if any) is auto-stripped at load; to preserve the intended inter-domain distribution, add a per-host override for the dominant subdomain (e.g. `www.facebook.com: 192`).
- `shard_id -> ingestor_id`: `shard_id // 16`.
- `offerer_id -> shard range`: `[offerer_id*16, offerer_id*16+15]`.

## 1.4 Design Characteristics

- **Loose coupling by filesystem IPC**: services can be restarted independently; progress is checkpointed by per-worker progress JSON files.
- **Sharded hot tables**: URL and feature state is split into 256 table families to reduce contention.
- **Eventual aggregation**: service components emit stats deltas to files, then one aggregator applies them to daily summary tables.
- **At-least-once file processing style**: folders become eligible only after a time lag (`2 * interval_minutes`) to avoid racing active writers.
- **Daily rolling-window maintenance**: event counters older than 90 days are rolled off from current URL counters and marked accounted.

## 1.5 Key Configuration Inputs

From `ingest.yaml` and `control.yaml`:

- interval windows (`interval_minutes=10`)
- queue/result/progress path templates under `/data/ipc`
- shard sizing (`num_shards=256`, `shards_per_ingestor=16`, `shards_per_offerer=16`)
- refill policy (`low_watermark_batches`, `batch_size`, `per_shard_select_cap`)
- accounting rolloff policy (`event_retention_days`, `batch_size`, daily UTC run time)
- database DSN
