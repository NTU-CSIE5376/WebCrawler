# Bounded JSON output process

Set `CRAWLER_JSON_WRITER_PROCESSES=1` to serialize and append crawl-result JSONL
in one spawned worker. The default remains zero for a reversible rollout.
JSONL schema, append order and folder rotation stay unchanged.

The reactor batches at most eight records or 4 MiB of serialized transport
payload, flushing partial batches after 50 ms. At most two batches are in
flight. A record larger than the byte cap runs alone rather than being dropped.
Scrapy retains item backpressure until each write is acknowledged. Failed writes
are reported per item; successful writes alone increment heartbeat counters.

Shutdown drains buffered writes. A broken worker stops further admission and
closes the spider for Supervisor recovery. Spawned parser and writer processes
share parent-death handling so they do not outlive a killed crawler parent.

Controls: `CRAWLER_JSON_WRITER_BATCH_SIZE`, `CRAWLER_JSON_WRITER_BATCH_BYTES`,
and `CRAWLER_JSON_WRITER_FLUSH_INTERVAL_MS`. These bound transport batches;
they are not a total container-memory limit.
