# ADR 0050: Run plain SELECT jobs on parallel read cursors

- **Status**: Proposed

## Context

[ADR 0049](0049-query-statements-off-event-loop.md) moved single-statement
query jobs off the event loop onto one worker thread with one DuckDB cursor.
Every statement, `SELECT` or not, therefore runs one at a time in submission
order. A client that fires several independent `SELECT` jobs (a dashboard
backend such as Cube, or a test suite polling report pages) waits for them
in a queue: four parallel `SELECT` jobs of about 0.9 s each finish at
0.9, 1.8, 2.7 and 3.6 s, and the slowest caller sees a timeout that the
statement itself does not cause.

## Decision

A new setting, `query_workers` (`BQEMU_QUERY_WORKERS`, default `4`, range
1-64), adds a second lane for read-only statements.

- **On by default, one switch to turn it off.** The default is 4 read
  threads. With `query_workers=1` no read pool exists and every statement
  runs on the single writer lane as before.
- **Read pool.** With `query_workers=N` above 1, `DuckDBEngine.start` creates
  `N` further cursors from the main connection and a second thread pool of
  `N` threads. Each read statement borrows a cursor from a queue and returns
  it in a `finally`. The pool has as many cursors as threads, so a borrow
  never waits.
- **Explicit opt-in per call.** `fetch_arrow_off_loop` takes a keyword
  `read_only`, default `False`. Only `execute_query_job` sets it, and only
  for a single-statement job whose statement type is `SELECT`. DML, DDL,
  `MERGE`, scripts, loads, `EXPORT DATA` and `CREATE MODEL` stay on the
  writer lane.
- **No new isolation work.** DuckDB MVCC gives each cursor a consistent
  snapshot, and a commit on one cursor is visible to the next statement on
  another. UDFs registered with `create_function` after the cursors exist
  are visible to them.

## Consequences

- **Positive**: independent `SELECT` jobs overlap, so latency for small
  queries and for a `SELECT` behind a long DML job no longer includes the
  queue. A heavy `SELECT` already uses most cores, so the throughput of
  several heavy `SELECT` jobs on a CPU-bound host improves little.
- **Negative**: with `query_workers` above 1, `TEMP` tables created by a
  single-statement job live on the writer cursor and are not visible to a
  later `SELECT` on a read cursor (`Table ... was not found`). This
  narrows the single-statement `TEMP` behavior that ADR 0049 describes. It
  matters only for a client that creates a `TEMP` table in one job and
  reads it in a later job. With `query_workers=1` the ADR 0049 behavior is
  unchanged.
- **Negative**: the submission order across lanes is gone. A `SELECT` can
  finish before a DML job that was submitted earlier and is still queued on
  the writer lane. A client that needs to read its own write waits for the
  write job to finish first.
- **Negative**: `SELECT ... UNION`, `EXCEPT`, `INTERSECT` and a
  parenthesized `SELECT` have an empty statement type, so they stay on the
  writer lane.
- **Follow-up**: scripts and loads stay on the main connection, on the
  loop (ADR 0049 follow-up).
