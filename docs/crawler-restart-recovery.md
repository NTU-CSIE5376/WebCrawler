# Restarting crawlers without discarding queued work

Normal crawler queue files are removed when read, while their URLs ordinarily
remain in memory until fetched. Stopping a worker therefore requires a verified
checkpoint or a complete drain. API jobs already retain their per-index results
and unfinished input on disk; do not turn their pending requests into normal work.

`ops/production/crawler_checkpoint.py` provides bounded maintenance operations.
Execute them directly on the worker's main asyncio thread through the existing
control endpoint. Never call `blockingCallFromThread` from that endpoint.
After loading the helper, set its `EXPECTED_PID`, `EXPECTED_WORKER`, and a private
controller `NONCE` before calling it.

1. `operation(crawler, "freeze")` pauses new normal/API intake, scheduler
   dispatch and downloader queue dispatch. It preserves the existing queues.
   Its lease automatically restores the original methods and any held Request
   objects if the controller disappears.
2. Let existing HTTP transfers, callbacks and item processing finish. Call
   `await checkpoint(crawler, absolute_directory)` to hold the normal scheduler
   requests and capture those plus the original normal requests still in the
   engine. File serialization, checksum, fsync and directory fsync run outside
   the event loop. RPC output contains only the resulting path, count and hash.
3. Verify every expected worker's file independently, then call
   `ready_to_stop(crawler)` again before stopping anything. Unknown request
   types, active transfers, expiring leases, changed queues or pipeline objects
   stop this procedure. The owned-work integer is retained for investigation;
   it is not treated as another URL queue or evidence of successful fetching.
4. Publish the restart manifest atomically on the shared IPC volume. Stop the
   old workers before starting their replacements. When recreating a container,
   verify its old container is stopped from the host: `/proc` PID checks inside
   the new container cannot observe a different PID namespace.
5. Start the canonical configuration. Supervisor's `reloadConfig` only reads
   definitions; it does not replace cached process definitions. Replace the
   stopped group or recreate the service to apply the new startup profile.
6. Verify result-file writes, API continuation, source/profile values, recovery
   receipts and retained cooldowns. Preserve the checkpoints until investigation
   and rollout verification are complete.

## Runtime manifest

The canonical pointer is `/data/ipc/crawler_restart/active.json`; isolated tests
can override the `CRAWLER_RESTART_MANIFEST` Scrapy setting. This is work-recovery
data, not an alternative source of site limits or worker capacity settings.

```json
{
  "schema": 1,
  "release_id": "reviewed-release-id",
  "workers": {
    "0": {
      "path": "/data/ipc/crawler_restart/reviewed-release-id/worker-00.json",
      "sha256": "<checksum-of-the-exact-checkpoint-file>",
      "old_pid": 12345
    }
  },
  "source_sha256": {
    "containers/crawler/crawler/spiders/spider.py": "<target-source-sha256>",
    "containers/crawler/crawler/restart_recovery.py": "<target-source-sha256>",
    "containers/crawler/config/site_policies.yml": "<target-source-sha256>"
  },
  "cooldown_deadlines_epoch": {
    "shared-slot-or-hostname": 1900000000.0
  }
}
```

Render actual checksums after integrating the target release. Include every
worker being replaced, including those with zero normal requests. Merge cooldown
deadlines by taking the maximum for each configured slot key across workers.
Keep the checkpoint directory private: the files contain original URLs.

## Startup and completion

Spider construction validates the checkpoint before ordinary IPC queue files
are read. Initial handoff checks the target source hashes and that the original
PID has stopped. During `engine_started`, before Scrapy sets the engine running,
recovery restores existing RetryAfterMiddleware timers, queues normal tasks with
their original domain IDs, and writes one fsynced handoff receipt.

The handoff receipt is not a completion receipt. On a subsequent compatible
startup, it allows an unfinished checkpoint to replay through the current main
callbacks without permanently pinning application source or obsolete PID values.
The checksum, worker identity, checkpoint schema and request interface are still
validated. Malformed metadata fails closed.

Recovery identifies each normal result through Request metadata retained across
redirects and retries. A separate fsynced completion receipt is written only when
all checkpoint tasks emitted their single normal result and the item pipeline
accepted it (`item_scraped`). Both success and failure results count as handled
attempts. Item/parse errors do not silently complete a task. This does not prove
downstream database ingestion or power-loss durability of every output file.

A crash before the whole checkpoint finishes can replay already-fetched URLs.
This at-least-once behavior trades duplicate attempts for avoiding silent loss.
There is no per-request fsync, database query or additional network operation.
After completion, startup skips URL replay; when all saved cooldowns have also
expired, a completed manifest becomes inert history and does not block later
source changes. An operator can then archive it.

Scrapy can garbage-collect idle downloader slots. A temporary in-memory
`request_reached_downloader` guard therefore restores an unexpired saved timer
if a slot is recreated; this signal runs before queue dispatch. The guard is
removed after the saved deadlines expire, and the item-completion listener is
removed after its receipt is written.
