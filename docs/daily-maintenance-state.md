# Daily maintenance completion state

GC and accounting record the UTC date on which a completed daily pass started.
The next process loads that date before entering its database work loop, so
recreating `scheduler_control` does not repeat a pass already completed today.
Explicit `--once` executions also record completion after returning normally.
Schedules, catch-up settings, retention rules and maintenance SQL are unchanged.

The optional `state_path` settings default to these tracked deployment paths:

- GC: `/data/ipc/progress/daily/gc.json`
- Accounting: `/data/ipc/progress/daily/accounting.json`

For example, GC's completion file is:

```json
{"version":1,"service":"gc","last_success_day":"2026-11-12"}
```

The date is always UTC, not the host's local calendar date. Accounting uses
`"service":"accounting"`. State is replaced atomically, with file and directory
fsync once after each completed pass. Missing state retains the existing catch-up
behavior; malformed, mismatched-service or future-dated state stops startup
before maintenance work. Do not delete bad state merely to make startup pass:
check the prior run evidence first, since deletion permits catch-up work.

For the first deployment, seed a date only when existing logs establish that
the corresponding pass completed, and use that pass's start date in UTC.
Never infer success from a start event or from process uptime. Preserve these
files during recreation and rollback; they live in the existing IPC mount.

"Completed" retains the services' existing `run_once()` contract: the method
returned without raising. Existing per-shard errors that the method logs and
handles internally do not become new failure/retry policies in this change.
An unhandled run failure leaves the previous completion date intact. If state
persistence fails after the database pass, the same process retains its in-memory
date and does not immediately repeat the work; operators must repair persistence.
A crash between the database work and the durable state update can still permit
a repeated pass on restart. This file checkpoint is not a transaction spanning
the database work.
