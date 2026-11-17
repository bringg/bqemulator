# ADR 0052: Copy AUTO snapshots and write job rows on dedicated lanes

- **Status**: Proposed

## Context

[ADR 0049](0049-query-statements-off-event-loop.md) and
[ADR 0050](0050-parallel-select-workers.md) moved query statements off the
event loop. Two pieces of work still run on the main connection, on the loop:

- the AUTO snapshot copy after every DML job (`CREATE TABLE ... AS SELECT *`
  of the whole target table), which takes 29 ms for 10,000 rows and 740 ms for
  1,000,000 rows, and longer while readers compete for the CPU;
- the persistent job-row write (`INSERT OR REPLACE` into the catalog `jobs`
  table), about 4 ms per job in persistent mode.

While they run, no request is served. The main connection must stay loop-only:
two threads on one DuckDB connection deadlock
(`grpc_api/loop_bridge.py`), so `asyncio.to_thread` on the main connection is
not an option.

## Decision

`DuckDBEngine.start` creates two more lanes. Each is one worker thread with its
own cursor, the pattern of ADR 0049:

- the **snapshot lane** (`bqemu-snapshot`), `run_on_snapshot_lane(fn)`;
- the **job lane** (`bqemu-jobs`), `run_on_job_lane(fn)`.

They are separate so a job-row write never waits behind a table copy. Neither
uses the writer lane of ADR 0049: its FIFO queue would put the copy behind
queued DML, so the copied rows could be newer than `snapshot_time`, and job
rows would wait behind DML.

**Snapshots.** `SnapshotManager.record_change_off_loop` is the async sibling of
`record_change`. The executor uses it for DML targets while it holds the write
lock, so captures stay serialized against the other write-lock holders.

- The lane reads `snapshot_time` right before the copy starts, so the copy is
  not newer than the timestamp by the lane wait.
- The catalog row and the `TableDataChanged` event stay on the loop, because
  the event handlers write the catalog.
- The lane gives the loop back during the copy, so a `DROP` can win the race.
  The lane reads the DuckDB object id of the source table in the same
  transaction as the copy. If the source table is gone when the copy starts,
  the lane returns nothing, the snapshot is skipped, the event is still
  published and the committed DML job stays `DONE`. After the copy, the
  manager reads the object id again on the loop. It drops the copy and writes
  no catalog row when the catalog entry is gone, its `creation_time` changed,
  or the object id changed. The object id changes when the table is dropped
  and created again, also with an equal clock. It stays the same across
  inserts, `ALTER TABLE` and metadata updates (`tables.patch`, `tables.update`),
  which run without the write lock: those keep the copy.
- If the awaiting task is cancelled during the copy, the manager publishes the
  event, so a dependent materialized view is still marked stale, and drops the
  copy on the lane after the copy in flight.
- The synchronous `capture` and `record_change` stay for `insertAll`, loads,
  copy jobs and the Storage Write API. These paths stay on the loop by scope;
  only the Storage Write handler is synchronous and cannot await.

**Job rows.** `CatalogRepository` gets `upsert_job_async` and
`delete_job_async`. The DuckDB repository updates its in-memory cache first,
without suspending, so a read right after the call sees the job. It then writes
or deletes the row on the job lane. The job routes and the upload route await
the async calls, so the response still leaves after the row is written.

Every write to the `jobs` table goes through the job lane, in call order. Two
connections writing the same row fail with a transaction conflict or lose the
row, so the two delete routes are now `async def`. They used to run on a
worker thread of the web framework and touched the main connection from there.
Work on the job lane finishes even if the awaiting task is cancelled.

`DuckDBEngine.stop` finishes work already queued on both lanes before it closes
their cursors.

## Consequences

- **Positive**: the loop no longer stalls for the length of a snapshot copy,
  which grows with the table size, or for a job-row write.
- **Negative**: two more threads and two more DuckDB connections.
- **Negative**: a snapshot copy can overlap a DML statement on the writer lane
  that targets the same table. This is MVCC read against write, the same class
  of overlap that ADR 0049 accepts.
- **Negative**: the windows between "DML committed" and "snapshot row written"
  last as long as the copy. In that window a `FOR SYSTEM_TIME AS OF` query for
  a time after the commit resolves to the previous snapshot, and a read of a
  dependent materialized view does not refresh it yet. Both windows used to
  last one loop iteration.
- **Negative**: a `DROP` that wins the race for a DML target now skips the
  snapshot for that change. The job result is unchanged.
- **Negative**: `stop` waits for a copy in flight.
- **Negative**: if the awaiting task is cancelled during the copy, the DML
  stays committed but gets no snapshot row, so time travel to a time after
  it reads the previous snapshot. No job path cancels these tasks today.
- **Not changed**: copies from `insertAll`, loads, copy jobs and the Storage
  Write API stay on the loop. There is still no switch to turn AUTO snapshots
  off.
