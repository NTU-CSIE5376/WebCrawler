# Bounded Yugabyte deadlock evidence capture

This opt-in operator tool records enough metadata to correlate transaction UUIDs,
backend PIDs and query IDs when another ingestor deadlock occurs. It does not fix
or retry application transactions. It neither restarts application/DB services
nor changes their configuration. No production deployment is triggered by merge.

The initial incident involved an ingestor crawl-result UPSERT and a concurrent
transaction, with waits observed on a current-state table and its re-scoring
index. Existing logs did not identify the second SQL. Do not attribute it to a
scorer merely because the index is named `golden_discovery_v2_rescore`.

## What is collected

- Three persistent read-only connections, one per configured YSQL node; the
  existing project's `psycopg2-binary==2.9.11` is sufficient.
- Activity metadata about once per second: transaction UUID, PID, query ID,
  client address/port, state, wait event and a server-side SQL-category hint.
  SQL text is not selected into the result. The category is a heuristic, not
  proof of the application or worker identity.
- Conflict/lock waits from the existing ASH ring's last 12 seconds every 10
  seconds, using its native time-window function; its sampling interval is not
  changed. Filtering avoids filling the evidence cap with unrelated RPC work.
- A 60-second RAM ring. Only incidents persist that ring and approximately ten
  seconds of following samples. Normal operation writes a small status file.
- On a new deadlock, up to four exact transaction-UUID lock lookups, at most
  one detailed incident per minute. No periodic all-cluster `pg_locks` query.
  Lock keys, URL values, SQL literals and raw SQL are not persisted.

`yb_deadlock_log_reader.py` follows the existing PostgreSQL log directory and
reconstructs only ERROR/DETAIL timestamps, PIDs and UUIDs. It follows newer
PostgreSQL filenames and stops on stdin EOF or its own duration limit. It does
not enable additional database logging. The initial operator run watches node 3,
the current application write endpoint; changing endpoints requires updating the
reader command. Other nodes still contribute activity/ASH snapshots.

## Verified version and observation cost

The interfaces were checked against the deployed **Yugabyte 2026.1.2.0**:
`yb_backend_xid` is a UUID and `yb_lock_status` accepts `(oid, uuid)`.

An ordinary `pg_stat_activity` read incurred roughly 500–680 ms locally even when
memory columns were not selected: this version takes per-backend smaps snapshots.
Each **diagnostic connection only** sets `yb_enable_memory_tracking=off` to omit
that optional work, while retaining `yb_enable_pg_locks=on` for transaction UUIDs.
No role defaults or other application sessions are modified. A five-sample
node-3 activity probe had a 4.38 ms median (first sample 97.8 ms). These are point
measurements, not a guarantee of zero production overhead. The bounded collector
uses a 250 ms statement timeout and backs off on errors or samples over 100 ms.
Its status exposes the actual sample age, latency, errors and truncation/capping.

Session memory tracking must remain off for these diagnostic queries: selecting
fewer columns or adding an outer LIMIT does not avoid that version's hidden
snapshot cost. UUID-specific lock calls still contact TServers and are therefore
rare, time-bounded probes on a separate connection.

## Execution and limits

Run under a private operator account with a 0700 output directory; JSON files
are 0600. Configuration or webhook/password files are not inputs to this tool.
An exclusive output-directory lock prevents accidental duplicate ownership.

```sh
python ops/diagnostics/yb_deadlock_capture.py \
  --nodes 172.16.191.3 172.16.191.2 172.16.191.4 \
  --output /private/deadlock-run \
  --interval 1 --duration-seconds 21600 --max-bytes 134217728 \
  --log-command-json '<exact argv JSON for the sanitized log reader>'
```

For a Docker-hosted DB, the reader argv can be assembled without changing its
container image or files:

```python
reader_argv = [
    "docker", "exec", "-i", "yugabyte", "python3", "-u", "-c",
    Path("ops/diagnostics/yb_deadlock_log_reader.py").read_text(),
    "/data/data/yb-data/tserver/logs", "--duration-seconds", "21660",
]
```

Check the current real log directory first. stdin must remain open: the collector
owns that pipe and closes it at exit, allowing the reader to terminate. The
initial operator deployment also uses a separate `timeout --kill-after` wrapper
and a low CPU scheduling priority. Stop only this diagnostic process group;
never restart a crawler or database as a way to stop observation.

Default collection lasts **six hours**, with a hard **128 MiB** output budget.
It stops rather than deleting previous evidence when that budget is exhausted.
A timestamped PID/start-identity receipt and actual command/source hashes belong
in the private operator directory, alongside status and event files.

## How to interpret evidence

Match node + transaction UUID + PID + query ID at the relevant time. A client
port may help map the backend to a worker; NAT, port reuse and PID reuse require
checking the timestamp and process identity. A SQL-category hint alone is not
sufficient attribution.

Very short transactions can fall between samples. Post-deadlock lock probes may
return no rows because the victim has already been aborted; ASH does not itself
contain transaction UUIDs. Empty evidence does not exonerate a writer, and a
`blocked_by` list may be capped. Always inspect sample errors, age, capping,
incident cooldown drops and the time window before claiming a complete cycle.

The sampler retains no normalized SQL text. A captured query ID can be looked up
separately in `pg_stat_statements`, with a privacy review before exporting its
normalized statement. A complete real-world counterparty capture is still needed
before blaming a component or declaring a Yugabyte implementation bug.

## Local verification

```sh
python -m pytest -q ops/diagnostics/tests
```

Tests use fake database adapters, real disposable log-reader subprocesses and
local temporary files. They do not connect to a production database. The
scheduler-ingest CI job runs these tests using its existing dependencies.
