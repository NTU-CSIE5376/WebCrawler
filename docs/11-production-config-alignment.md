# Production configuration alignment

These are the operating choices approved on 2026-11-12. They describe the
desired state, not a claim that a particular production rollout is complete.
Record the deployed commit and verification results for each rollout.

## Approved values

| Component | Desired state |
| --- | --- |
| Normal/API dispatch | Weighted dispatch, API share 20%; the other source uses otherwise idle opportunities |
| Crawler site policies | Repository YAML, including its 38 exact-host rules; remove the obsolete external policy fork |
| General crawl policy | Nominal maximum 8 QPS, per-slot concurrency 16, AutoThrottle target 4 |
| Wikipedia | Nominal maximum 15 QPS, crawler shared concurrency 7; reserve two concurrent requests for the separate sitemap services |
| Crawler capacity | Workers 0–14: global concurrency 256 and scraper buffer 100,000,000 bytes; worker 15: 384 and 150,000,000 bytes, following #87 |
| Parser experiment | Disabled; do not deploy #142 until it is reviewed and merged |
| API database | Yugabyte, current endpoint `172.16.191.2:5433`, database `crawlerdb`; credentials remain outside Git |
| Discovery ranker | Preserve the deployed V2 model, V1 fallback, and domain priority steering; no retraining or artifact replacement |
| Scorer processes | 16, matching the application configuration and Supervisor process count |
| Sitemap patrol | `batch_limit: 1000`, `global_delay_sec: 0.5`, `loop_interval_sec: 60`, after checking the loaded configuration |
| Consumed result cleanup | Minimum folder age 2 hours, after every required consumer's progress has reached the folder |
| Unconsumed result cleanup | No automatic age-based deletion: `max_age_hours: 0` |

Wikipedia's seven shared crawler leases plus the two separate synchronous
sitemap services can total nine concurrent requests in this deployment.
Concurrency is not a shared QPS limit. Scrapy's pacing and AutoThrottle do not
account for the sitemap services' direct HTTP calls; aggregate rate enforcement
is tracked in [#145](https://github.com/NTU-CSIE5376/WebCrawler/issues/145).
Do not treat the configured 15 QPS as a demonstrated system-wide hard limit.

## Source, secrets, and runtime state

Operating values belong in the reviewed repository configuration. Credentials,
API tokens, and webhook URLs belong in protected external files or secret
references. Model files and queue contents are external data; retain the
existing artifacts and record their paths, versions, and hashes privately.
Do not print complete DSNs, environment variables, rendered Compose output, or
private configuration while collecting evidence.

An external secret file must not become an alternative place to tune crawler
capacity, model selection, dispatch share, website rules, or worker counts.
Audit any automatic `docker-compose.override.yml`: either version its approved
nonsecret deployment settings or remove those overrides. Do not keep a private
policy fork merely because the tracked checkout is clean.

The API endpoint is not a template for other services' database settings.
Different Yugabyte node addresses may refer to the same cluster. Preserve the
other services' existing credentials, database choices, and API token.

## Before changing production

1. Identify the current checkout, commit, Compose invocation, image IDs, source
   mounts, Supervisor configuration, cron entries, and running process IDs.
   Check for another deployment in progress before writing shared files.
2. Compare four views: repository configuration, deployed files, values loaded
   by running programs, and what a program restart or container recreation
   would load. A clean Git checkout proves only one of these views.
3. Back up the existing deployment metadata and external configuration to a
   protected directory. Keep secrets out of commits and review output.
4. Validate the candidate Compose configuration and parse the actual Supervisor
   configuration before applying it. Store any expanded configuration privately
   and report only whitelisted nonsecret values.
5. Preserve database containers, shared IPC, queue markers, model artifacts,
   and unrelated scheduled jobs. This alignment does not authorize database
   migrations, retraining, index publication, or deletion of unread results.

## Apply only the required service changes

Editing a bind-mounted file does not establish that an existing program has
reloaded it. Crawler policies, scorer configuration, model selection, and
sitemap configuration are loaded during startup. Container environment
changes require container recreation to affect a new process's inherited
environment; restarting a child process does not replace container metadata.

Supervisor `reread` parses definitions without applying them. `update` can stop
and restart a whole changed group. In particular, changing the crawler groups
from the historical worker-7 layout to the #87 worker-15 layout must not be
treated as a harmless file refresh.

Do not restart a memory-backed crawler or recreate its container until its
already accepted work has been preserved. Normal domain queue files are
deleted when read, so stopping a program can discard the only remaining copy
of normal requests. Use a tested drain/checkpoint-and-resume procedure, verify
durable recovery data, retain website cooldowns, and verify recovery receipts.
API work also needs its pending markers and persisted per-index results kept.
Confirm that every crawler ID has exactly one owner throughout the transition.
Do not restart other workers merely to replace one stopped experiment.

Remove the parser experiment's Supervisor program and startup configuration
after restoring the merged implementation. Retaining its benchmark files as
inactive evidence does not authorize running it again.

Changing the cleanup script and host cron does not require restarting the
ingestor container: the next invocation loads the deployed script. Keep the
progress-aware deletion and no-symlink/FD safeguards unchanged. Remove the
obsolete `domain_qps.json` copy only after checking that no supported startup
path still references it.

## Install the result cleanup schedule

The canonical host schedule is
[`ops/production/cron.d/crawl-result-cleanup`](../ops/production/cron.d/crawl-result-cleanup).
It runs every 30 minutes inside `scheduler_ingest`, with `/app` as the working
directory, using the shared IPC configuration. It explicitly passes
`--min-age-hours 2 --max-age-hours 0`.

Before installing:

1. Back up root's existing crontab and any existing cleanup file privately.
2. Review and remove only the previously identified `crawl_result` cleanup
   entry from its old location. Preserve every unrelated cron job. Do not run
   both the old root-crontab entry and the new `/etc/cron.d` entry.
3. Run the candidate script once without `--apply`; review its decisions and
   confirm that unconsumed deletion is zero. A dry run does not delete data.

Install the template as `/etc/cron.d/crawl-result-cleanup`, owned by root with
mode 0644. This is a system crontab with a `root` user column; it must not be
pasted unchanged into `crontab -e`. Confirm Docker and `scheduler_ingest` are
available on the IPC host and that there is exactly one cleanup schedule.

The script defaults are also 2 and 0. Its existing positive `--max-age-hours`
override remains available as an explicit data-deletion option, but is not
part of this production policy. Do not enable it as an unrecorded disk hotfix.

The two-hour age comes from the UTC date/time folder name, not from the last
write or consumption completion time. Progress files are the existing
consumer watermark contract; they do not independently prove every database
transaction succeeded. Keep ingestion errors and recovery separate from
cleanup eligibility.

Unread data will accumulate when a consumer stops. Disk alerts and their
notification delivery must be verified separately; this rollout does not
claim that Discord alerts are already connected. Handle capacity incidents
through explicit operator action instead of silently deleting unread data.

## Verify and record completion

- Compare the approved nonsecret values against the running programs and their
  restart configuration, not just file contents.
- Confirm exactly 16 crawler owners, the intended capacity on worker 15, no
  parser experiment, API share 20%, and the shared Wikipedia concurrency cap 7.
- Confirm API routing uses the intended Yugabyte endpoint, while retaining the
  existing API authentication. Exercise an isolated test job and clean up only
  that test job's data.
- Confirm 16 scorer processes and the preserved model identity/fallback;
  confirm the sitemap patrol's loaded values.
- Confirm normal results continue to be written and API result progress
  advances. Preserve checkpoint recovery receipts and investigate counter
  discrepancies instead of zeroing counters to make them appear healthy.
- Confirm a single cleanup schedule, explicit 2/0 arguments, and subsequent
  cleanup summaries with zero unconsumed deletions. Do not force deletion to
  manufacture a successful cleanup check.
- Record the deployed commit, image/config hashes, expected external secret
  references, process IDs, test results, recovery receipts, and any remaining
  differences. Do not call alignment complete while an unexplained policy
  override or a stale restart configuration remains.

Rollback must preserve the same queues, secrets, model artifacts, and the
approved no-force-deletion policy. Restore the prior operating profile through
the tested recovery procedure rather than blindly recreating all services from
an old Compose file.
