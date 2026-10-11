# DB Perf Test Scripts

Core upsert queries we test regularly, one folder per database. Each file is a **single worker** script; real tests run multiple workers in parallel (see "Running" below).

> Files here are trimmed to **3 sample iterations** to keep the repo small. The real test loops the same `BEGIN; ... ROLLBACK;` block 20000x per worker. See "Regenerating full workers" below to rebuild the full version.

## Methodology

1. **Persistent connection** — each worker runs `docker exec -i postgres psql <connstr> < worker.sql`, one connection for the whole run, not reconnecting per query. Verify with repeated `SELECT pg_backend_pid();` in the same session.
2. **Safe** — everything wrapped in `BEGIN; ... ROLLBACK;`, no real writes persisted.
3. **Concurrency** — multiple worker scripts run with `&` + `wait` to simulate concurrent connections.
4. **Random domain_id** — each INSERT uses a random `domain_id` so results aren't skewed by always hitting the same shard/tablet.

## Layout

```
postgresql/
  upsert_worker.sql                 baseline (plain SQL, fixed shard table)

yugabytedb/
  upsert_worker.sql                 baseline (plain SQL, merged table)
  upsert_worker_concurrency30.sql   concurrency ablation (one of the 30 workers)
  upsert_worker_concurrency50.sql   concurrency ablation (one of the 50 workers)
  upsert_worker_batched20.sql       batched insert (20 rows per INSERT)

cockroachdb/
  upsert_worker_prepared.sql        PREPARE/EXECUTE (required, see below)
  upsert_worker_concurrency50.sql   concurrency ablation (PREPARE/EXECUTE)
  upsert_worker_batched20.sql       batched insert (plain SQL, 20 rows per INSERT)
```

## Why CockroachDB needs PREPARE/EXECUTE

CockroachDB's plan cache only hits on prepared statements or literal-identical queries. Plain SQL with different literal values each time (like the PostgreSQL/YugabyteDB scripts) gets re-planned from scratch every call — about 30% lower throughput and 60% worse p99. PostgreSQL and YugabyteDB don't have this issue; plain SQL and prepared statements perform about the same on them.

`upsert_worker_prepared.sql` runs `PREPARE ins_stmt (...) AS INSERT ... VALUES ($1,$2,...) ON CONFLICT ...;` once, then `EXECUTE ins_stmt('url', domid, now(), ...);` per iteration — this mirrors what SQLAlchemy/psycopg2 do under the hood when sending parameterized queries.

## Regenerating full workers

To rebuild a full 20000-iteration worker file (Python example, YugabyteDB):

```python
import random
template = '''BEGIN;
\\timing on
INSERT INTO url_state_current (url, domain_id, last_fetch_ok, last_content_update, last_modified, num_fetch_ok_90d, num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail, last_fail_reason, content_hash, should_crawl, title, hreflang_count, has_json_ld, etag, cache_control, is_redirect, redirect_hop_count, robots_bits)
VALUES ('https://perftest.example.com/w{i}', {domid}, now(), now(), now(), 1, 0, 1, 0, NULL, 'abc123hash', FALSE, 'Test Page Title', 2, TRUE, 'etag123', 'no-cache', FALSE, 0, 1)
ON CONFLICT (domain_id, url) DO UPDATE SET
  last_fetch_ok = COALESCE(EXCLUDED.last_fetch_ok, url_state_current.last_fetch_ok),
  num_fetch_ok_90d = url_state_current.num_fetch_ok_90d + EXCLUDED.num_fetch_ok_90d;
\\timing off
ROLLBACK;'''
N = 20000
for w in range(1, 31):  # 51 for the 50-worker version
    lines = [template.format(i=f'{w}_{i}', domid=random.randint(1, 999999999)) for i in range(N)]
    with open(f'worker_{w}.sql', 'w') as f:
        f.write('\n'.join(lines))
```

For CockroachDB, add the `PREPARE` block once at the top and use `EXECUTE ins_stmt(...)` inside the loop instead of embedding literal values.

## Running

```bash
for w in $(seq 1 N); do
  docker exec -i postgres psql <connstr> < worker_${w}.sql > log_${w}.txt 2>&1 &
done
wait
```

Connection strings:
- PostgreSQL: `psql -h <host> -p 5432 -U <user> -d crawlerdb`
- YugabyteDB: `psql -h <host> -p 5434 -U yugabyte -d crawlerdb`
- CockroachDB: `psql "postgresql://root@<host>:26257/crawlerdb?sslmode=disable"`

## Analyzing logs

Each log has one `Time: X.XXX ms` line per statement. Since `\timing on` covers `BEGIN; INSERT; ROLLBACK;`, every iteration prints 3 lines — **take the 2nd of every 3** (the INSERT):

```python
import re, statistics
times = []
with open('all_logs.txt') as f:
    for line in f:
        m = re.match(r'^Time: ([0-9.]+) ms', line)
        if m:
            times.append(float(m.group(1)))
inserts = times[1::3]
inserts.sort()
n = len(inserts)
print('mean:', statistics.mean(inserts))
print('p95:', inserts[int(n*0.95)])
```

Throughput = total iterations / total wall-clock time for all workers to finish.
