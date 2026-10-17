# Manual Bulk Crawl API performance — 2026-10-17

Each request size ran once with **100,000 real URLs and four concurrent clients**
on an isolated stack on `172.16.191.2`. The same URL set was used for all cases.

| URLs per request | HTTP completion | Crawl completion | HTTP URLs/s | Crawl URLs/s |
| ---: | ---: | ---: | ---: | ---: |
| 1,000 | 5.47s | 1,127.29s | 18,283 | 88.7 |
| 5,000 | 5.08s | 840.27s | 19,697 | 119.0 |
| 10,000 | 5.85s | 690.39s | 17,092 | 144.8 |

Each throughput is 100,000 divided by the corresponding completion time in seconds.
Both times start at the first submission. HTTP completion means every job ID was
returned; crawl completion means every URL has a terminal success or failure result.
Crawl times include robots.txt, retries and politeness waits.

| URLs per request | Successful HTML responses | Failed URLs | HTML files saved |
| ---: | ---: | ---: | ---: |
| 1,000 | 68,935 | 31,065 | 85,576 |
| 5,000 | 69,040 | 30,960 | 85,538 |
| 10,000 | 69,116 | 30,884 | 85,634 |

HTML error pages are saved, so the HTML file count can exceed the success count.
All three cases processed 95% of URLs in approximately five minutes. Slow sites
then dominated the completion time; the 1k case's final URLs were concentrated
on `www.acba.am`. These single runs against live websites do not establish that
larger batches caused the lower crawl completion times.

## Configuration

- Flask/Gunicorn: four API workers, 8 CPU limit, 4 GiB memory limit.
- Existing HtmlSpider implementation: 16 worker processes, 8 CPU limit,
  16 GiB memory limit, 64 concurrent requests per worker and 16 per domain.
- Per worker: 256 prefetched manual URLs and at most 64 active job portions.
- AutoThrottle: enabled, target concurrency 2, start delay 0.5s, maximum delay
  10s; download delay 0.25s. Regular crawl requests retain higher priority.
- Isolated PostgreSQL 16: 2 CPU limit and 2 GiB memory limit.
- Runtime: Python 3.12, Scrapy 2.19.0 and Twisted 26.4.0.
- Cases ran in order 1k, 5k, 10k. API processes stayed running; crawler processes
  restarted between cases. API and DNS cache state may affect comparisons.

The benchmark crawler used an open-file soft/hard limit of **65,536**. An initial
trial inherited a soft limit of 1,024 and exhausted descriptors; that trial is
excluded. A descriptor snapshot contained up to 1,886 sockets in one worker.
The runtime's idle persistent-connection timeout is 240 seconds, so open sockets
can exceed the number of active downloads. Production descriptor defaults were
not changed; these measurements require the stated benchmark limits.

## Verification and cleanup

API tests (135) and crawler tests (32) passed on the remote host. Each case
verified unchanged routing database contents, exactly 16 normal crawler records,
separate manual-job output, attachment downloads and HTTP byte ranges.

Production queues and databases received no submitted work. Test containers,
volumes, network, image tags and the dedicated remote directory were removed.
Original production container IDs and start times were unchanged.

The URL-set SHA-256 was
`24b8301827023eb16dcd71438574fc6d6550264985f9a1fe7c04692ff7f9e662`.
Raw reports, source hashes and cleanup evidence are retained locally under
`artifacts/benchmarks/manual-bulk-api-20261017/`; URL data and downloaded pages
are excluded from Git.
