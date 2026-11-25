# Retiring the legacy crawler input

The crawler now accepts fetch and ingest work only through API job partitions.
The domain-file QueueConsumer, domain watermark, periodic top-up timer, startup
reservation and legacy completion ownership bookkeeping are removed. The API
job consumer remains responsible for bounded intake, downloader-slot admission,
source sharing and persisted completion. No business score enters the crawler.
Heartbeat pending and active-domain metrics now describe ingest-job inventory.

Deploy the performance, writer, ingest API and offerer layers first. Pause the
legacy offerer, drain its published files and in-memory requests, then deploy the
crawler retirement layer with the matching offerer API configuration. The
reviewed default job API URL is `http://api:8080`; mixed old/new workers require
an explicit staged configuration until all matching pairs are upgraded.
Do not delete unfinished domain files: migrate or drain them before switching.

Rollback requires restoring the preceding crawler image and the file-feed
offerer configuration together, after draining or recovering unfinished API
claims. This PR does not delete existing queue data or deploy itself.
The offerer keeps the legacy file publisher for coordinated rollback, but it
is not an input supported by the crawler at this layer.
