# Separate discovery and content ingestion

`discovery_ingestor` and `content_ingestor` independently read the existing
router/sitemap output under `crawl_result/ingestor_NN`. The router already
routes discoveries by destination domain/shard; this change does not add new
shards or change URL ownership. Producers and the feature extractor retain
their existing file contract. Each component reads the stream once, selecting
its own record kind, rather than one worker completing both passes in order.

Discovery combines normalized URLs incrementally, across files and folders.
It sums approximate/external inlinks, keeps the first nonempty anchor, the
highest-scored parent and its provenance, and the highest applicable manual
priority. It flushes one shard at 64,000 distinct URLs, evicts the largest
shard buffer at the shared 2 GiB estimated-record budget, and flushes all tails
after 600 seconds without discovery input. A 600-second maximum pending age
also flushes during continuous traffic, bounding checkpoint delay. Shutdown
flushes all tails. Timers are cooperative: an in-flight SQL transaction can
finish after a deadline. The budget covers all shard buffers **within one
discovery process**, not the fleet or its full RSS. The bounded URL-policy LRU,
Python/driver allocations, filtering, and serialization need additional RAM.
The policy audit logs record cache misses; they are not per-occurrence counts.

Content independently processes parsed results in 2,000-record, single-shard
transactions with a 64 MiB estimated buffer budget and flushes every folder
tail. It excludes raw HTML and outlinks from its in-memory DB batches: the
router's content hash and page metadata are sufficient for this path, and the
extractor still reads the unmodified source records. Normal offered URLs
already exist in the frontier. For a new URL whose
result precedes its discovery, the result INSERT initializes natural source
and score defaults; new router records also supply the domain score. Existing
scores/source are not overwritten. Discovery metadata becomes eventually
consistent, and intermediate discovery history snapshots may be fewer after
aggregation. UPSERT semantics and background scoring remain in place.

Both workers acquire the same per-shard POSIX file lock around the database
transaction and commit. This prevents the new workers from writing the same
shard concurrently; unrelated shards can progress independently. Deploy them
on the same shared filesystem/lock directory. This is not a distributed lock
for separate hosts with independent filesystems. Offerers, scorers, injectors,
and other database writers continue to use database concurrency control.
`ingest.shard_write` records lock wait and statement wall times for lookups,
UPSERT, history, other SQL, optional inline scoring and commit, without logging SQL values.

## Checkpoints and failure behavior

Discovery's scan cursor can run ahead of its durable checkpoint. It advances
the checkpoint only when every buffered discovery has committed. Content has
its own checkpoint, so a discovery tail does not block parsed-result progress.
A failed batch aborts the worker without silently advancing past it. Source
folders remain durable until all consumers finish; cleanup now waits for both
components and the extractor. For compatibility with an existing cleanup cron,
the old combined checkpoint advances to the slower of the new checkpoints.

Recovery remains **at least once**, as in the old folder pipeline: restarting
from the last durable checkpoint can replay already committed batches from an
unfinished interval and increment counters again. The maximum-age flush bounds
this interval during normal operation; it does not make additive writes exactly
once. Commit-acknowledgment failures are not automatically retried by IngestDB.

## Rollout and rollback

1. Stop the combined ingestor for a worker ID after it finishes/checkpoints its
   current folder, preferably while idle. Never run it alongside the pair.
2. Start both components with the same worker ID, routing configuration, source
   directory and lock directory. Missing component checkpoints seed from the
   old combined checkpoint; existing checkpoints are never reset.
3. Keep the existing container memory cap initially. Monitor combined RSS,
   batch errors, stage timings, committed discovery progress and content lag.
4. To roll back, stop at a common folder boundary and drain both components.
   Verify their checkpoints agree before restarting the combined ingestor.
   If they disagree, replay from the slower checkpoint has at-least-once counter
   effects; do not advance it to discard unread work.

The checked-in `ingestor` configuration and old entry point remain for migration
and rollback. Supervisor normally runs only the split components. Do not remove
source folders or reset progress to start a canary. Benchmark scripts are not
part of the source change.
