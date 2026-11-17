# ADR 0051: Skip DDL/DML bookkeeping for plain SELECT jobs and cache translation

- **Status**: Proposed

## Context

A single-statement `SELECT` job spends most of its event-loop CPU time before
DuckDB sees the statement. Profiling a CI workload (about 1,500 `SELECT` jobs
of a BI backend) showed about 70 ms of loop CPU per request. Two parts of it
are avoidable:

- About 10 helpers, each of which parses the same BigQuery text again with
  `sqlglot.parse_one`, only to find that the tree is not DDL or DML and return.
  Together they cost about 15-27 ms per request and 10 of the 18 parses.
- BigQuery to DuckDB translation (about 20 ms) and table-reference
  qualification (about 2-3 ms). BI backends repeat the same statement text:
  23-38% of the texts in the profiled runs were exact repeats.

## Decision

**Plain `SELECT` fast path.** `execute_query_job` computes
`is_plain_select = not is_scripted and statement_type == "SELECT"` once, with
the same predicate that routes a statement to the read lane
([ADR 0050](0050-parallel-select-workers.md)). For such a statement it skips
the helpers that can only act on DDL or DML:

- the versioning, row-access-policy and routine-drop fast paths;
- the immutable-table DML guard and the pre-execution DDL guard;
- the five `sync_*` catalog syncs after execution;
- the AUTO snapshot capture;
- the `CREATE TABLE` detection in `_build_query_configuration`.

Each skipped helper returns early for an `exp.Select` tree today, so the result
is the same. Every other statement keeps the full path. A test over `SELECT`
texts with tricky comments and literals asserts that each skipped helper is a
no-op for them, so a future side effect added to one of these helpers fails a
test instead of being skipped silently. `UNION`, `EXCEPT`, `INTERSECT` and a
parenthesized `SELECT` have an empty statement type and keep the full path.

**Translation cache.** Each `AppContext` owns a `TranslationCache`, a bounded
LRU with two namespaces. `_run_single_sql` uses it around the two pure steps:

- translation, keyed by `(BigQuery SQL after the rewrite passes, catalog schema
  snapshot, caller identity)`;
- table-reference qualification, keyed by `(translated SQL, project id)`.

Every input of each step is in its key, so no invalidation is needed. All
rewrite passes before translation (materialized-view refresh, `FOR SYSTEM_TIME`
resolution, row-access enforcement, `INFORMATION_SCHEMA` and wildcard
expansion, catalog schema build) still run on every call, because they read
live state and have no generation counter to invalidate on. Only successful
results are stored, so an error is raised again on each call. Texts longer
than 64 KiB are not stored.

The cache size is `translation_cache_size` (`BQEMU_TRANSLATION_CACHE_SIZE`,
default 2048, range 0-100000; one statement uses up to 2 entries). `0` turns the cache off.

The scripting interpreter does not use the cache. This cache is unrelated to
the query result cache setting (`query_cache_ttl_seconds`).

## Alternatives considered

- **Cache the whole rewrite chain.** Rejected: the chain reads snapshots, the
  catalog, DuckDB views and the caller, and nothing signals when they change.
- **Parse once and pass the tree to every helper.** It would speed up DDL and
  DML too, but it changes about 12 helper signatures. The guard keeps the
  other paths unchanged and gets the full `SELECT` gain.
- **A module-level cache shared with the scripting interpreter.** Rejected: it
  needs a lock and test isolation, and scripts are a small share of the load.

## Consequences

- **Positive**: less loop CPU for every plain `SELECT` job, and none at all for
  the translation of a repeated statement.
- **Negative**: memory for up to `translation_cache_size` entries. A text with
  an inlined `INFORMATION_SCHEMA` or row-access expansion can be large.
- **Negative**: `SQLTranslator.translate` must stay a pure function of its
  arguments. A rule that reads global or clock state would return stale cached
  output.
- **Negative**: the fast path skips helpers without a warning if one of them
  gains a `SELECT`-relevant side effect. The soundness test guards this.
