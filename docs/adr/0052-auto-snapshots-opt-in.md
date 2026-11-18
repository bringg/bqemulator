# ADR 0052: Capture AUTO snapshots only when asked

- **Status**: Proposed

## Context

[ADR 0009](0009-snapshot-layer-time-travel.md) and
[ADR 0016](0016-snapshot-time-semantics.md) capture an `AUTO` snapshot after
every write to a table: a `CREATE TABLE … AS SELECT *` copy of the whole table
plus one row in the `snapshots` catalog table. `FOR SYSTEM_TIME AS OF` reads
these copies.

The capture is on the write path of every statement that changes a table (DML,
`MERGE`, load, copy, `tabledata.insertAll`, Storage Write API). Its cost grows
with the table size and the write rate, and most users never read a snapshot.

A change-data-capture client that applies each transaction with one load query
and one `MERGE` per touched table, in parallel, shows the cost. The replay used
30 tables of 2,000 rows each, 5 `MERGE` statements per transaction, a
persistent data directory and a 300 s run:

- With the capture: 50 transactions per minute, `MERGE` p50 about 1.15 s. A
  60 s `py-spy` profile showed the event loop about 81% busy. 68% of the loop
  time was in `SnapshotManager.record_change`: 61% in the catalog write
  (`create_snapshot`) and 7% in the table copy.
- Without the capture: 116 transactions per minute (2.3 times more), `MERGE`
  p50 about 0.38 s, and the rate stays flat over time.

## Decision

A new setting, `auto_snapshots` (`BQEMU_AUTO_SNAPSHOTS`, default `false`),
turns the `AUTO` snapshot capture on or off.

- **Off (default).** `SnapshotManager.record_change` does not capture. It still
  publishes `TableDataChanged`, so materialized views and other subscribers
  behave as before, and it returns `None`.
- **On.** The behavior of ADR 0009 and ADR 0016 is unchanged.
- **Time travel when off.** `SnapshotManager.resolve_time_travel` raises
  `InvalidQueryError` (reason `invalidQuery`; clients raise `BadRequest`) with the message
  `FOR SYSTEM_TIME AS OF needs AUTO snapshots: set BQEMU_AUTO_SNAPSHOTS=true`.
  The query never falls back to the live table, because that would return
  current rows where the client asked for old rows. Every
  `FOR SYSTEM_TIME AS OF` on a `dataset.table` reference goes through this
  method.
- **Not affected.** `USER` snapshots (`CREATE SNAPSHOT TABLE`), clones,
  materialized views and the snapshot garbage collection work the same with
  either value.

## Alternatives considered

- **Keep the capture on and make it cheaper** (batch the catalog writes,
  capture lazily). Rejected for this change: it keeps a copy of every table on
  every write, and it is a larger change to the persistence format.
- **Fall back to the live table when the setting is off.** Rejected: a silent
  wrong answer is worse than an error.
- **Leave the default on and let users turn it off.** Rejected: most clients do
  not use time travel and would keep paying for it.

## Consequences

- **Breaking**: the default changes. A client that uses
  `FOR SYSTEM_TIME AS OF` must set `BQEMU_AUTO_SNAPSHOTS=true`, or it gets an
  `invalidQuery` error. The release operator lists it in the changelog.
- **Positive**: write-heavy workloads stop paying for a table copy and a
  catalog write per statement.
- **Positive**: the error names the setting, so the fix is visible.
- **Negative**: a persistent data directory that was written with the setting
  on keeps its old snapshots, but `FOR SYSTEM_TIME AS OF` on it fails while the
  setting is off. Turn the setting on to read them again.
- **Negative**: on a persistent data directory, writes made while the setting
  was off have no snapshot. After the setting is turned on again, a
  `FOR SYSTEM_TIME AS OF` for a time in that period returns the last older
  snapshot, without an error. Time-travel results for that period are not
  valid. Recreate the data directory to start a clean history.
- **Negative**: the test suites that exercise time travel and the end-to-end
  container runs must set the setting explicitly.
