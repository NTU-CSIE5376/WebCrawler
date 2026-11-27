# Crawler processing feedback

The crawler publishes bounded, measured per-domain processing feedback in
`<pending_dir>/feedback/crawler_NN.json`. The API serves it through
`GET /v1/crawlers/{crawler_id}/performance`; the offerer can consume this public
contract without reading crawler settings or enforcing site policies.

A domain is identified by the job client reference `offerer/<worker>/domain/<id>/<unique>`.
This first layer instruments the existing fetch-job consumer; the ingest-job API
layer reuses it to include parsing and durable JSONL output before acknowledgement.
Latency runs from admission to the persisted job result. A rolling 120-second
window reports sample count, mean and p90 latency, and maximum observed active
requests. Estimated completion rate is maximum observed parallelism divided by
mean latency: it is an estimate, not a guaranteed capacity or configured QPS cap.

Storage is bounded to 2,000 domains and 512 samples per domain. Feedback is
published once per consumer poll (one second). The API returns `available: false`
for missing samples or reports older than 30 seconds. No URL scores, configured
concurrency limits, or domain-specific business rules are exposed.
