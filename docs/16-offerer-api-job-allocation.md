# Latency-aware offerer job allocation

The offerer owns URL value and allocation. It queries unfinished API jobs and
fresh processing feedback, peeks eligible frontier candidates, allocates URLs,
atomically claims scored-first URLs with skip-locked handling, and submits
`processing_mode=ingest` jobs over HTTP. No score enters the crawler contract.
Bounded per-shard frontier probes remove domains that have no eligible URLs.
For equal URL value, usable frontier and expected completion work affect the
allocation instead of taking only the first domain in score order.

For domain i, new URLs x_i are integer decisions. Let p_i be existing pending
URLs, v_i normalized frontier value, and T_i the soft target derived from
observed parallelism / mean end-to-end latency over a 60-second horizon.
Unknown domains bootstrap with eight URLs. Maximize value plus an exploration
bonus, minus a piecewise overload penalty (slopes 1, 4, 16). Three nonnegative
slack variables allow p_i+x_i to exceed T_i, so slow valuable domains can be
probed above their measured capacity. Observed capacity is never a hard ceiling.
Hard limits cover available frontier, per-job batch size, per-cycle URL budget,
16,384 total outstanding URLs and active-domain inventory.

SciPy MIP has a one-second solve budget. A feasible marginal-value greedy plan
is computed first; it remains the result if the MIP plan is unavailable,
infeasible after rounding, or worse under the same measured objective. Planner
logs report chosen method, solve status, objective, targets and allocations.
A reported fallback with solver status zero can therefore mean that the greedy
plan was retained after post-solve validation; it does not mean a timeout.

HTTP publishing uses a durable local outbox with a unique client reference.
After a lost reply, the offerer searches the API for that exact reference before
resubmitting. An unavailable API stops new claims instead of silently switching
to the old file feed. There remains a crash window between committing the DB
claim and writing its outbox; this series does not make those two stores atomic.

Before upgrading, drain existing file-fed work and configure `offerer.job_api_url`. The legacy file-feed fallback
remains selected only when that URL is empty. The final crawler-retirement PR
sets the job API as the reviewed deployment default. Controls are
`job_url_budget`, `job_domain_slots`, `max_outstanding_job_urls`,
`job_planning_horizon_sec`, `job_probe_urls`, `job_overload_penalty`,
`job_allocation_method` and `job_mip_time_limit_sec`. The offerer reads no crawler
QPS or concurrency policy settings.
