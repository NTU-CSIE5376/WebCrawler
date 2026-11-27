# Fetch and ingest jobs

`POST /v1/manual-bulk-crawls` accepts `processing_mode: fetch | ingest`.
The default `fetch` preserves the existing behavior: retrieve requested pages
and save HTML job results, without extracting links or creating ingest records.
`ingest` uses the same downloader, parser and JSONL pipeline as ordinary crawling,
including failure records and link extraction. Router, ingestor and scorer keep
reading their existing crawl-result directories; they do not consume API results.

`validate_urls` defaults to true. False bypasses URL normalization and syntax
validation for already-selected frontier URLs, but preserves string, length,
list-count, request-size and TTL bounds. No URL value or score crosses this API.
An optional `client_reference` (up to 128 characters) supports publisher identity.

`GET /v1/manual-bulk-crawls` lists unfinished jobs, with optional `crawler_id`,
`client_prefix`, `limit` and `after` pagination. It scans pending partitions rather
than every historical job. Individual job status and result endpoints still work.

Ingest acknowledgements occur after JSONL fsync and job-result checkpointing.
A failed write keeps the URL unfinished for replay; delivery is at least once,
so a crash between output and acknowledgement may produce a duplicate record.
Fetch/ingest queues share domain limits, retain existing service-time scheduling,
and reserve the reviewed 20/80 dispatch share while borrowing unused capacity.

Set `IPC_JOB_INGEST_ENABLED=true` on a crawler after draining its legacy input to
use API jobs as its only input. The later retirement layer removes that migration
switch and the legacy domain-file reader. The offerer submits through HTTP; it
never needs the API's filesystem mount.
