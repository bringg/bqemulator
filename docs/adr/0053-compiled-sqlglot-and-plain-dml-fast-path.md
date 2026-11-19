# ADR 0053: Use compiled SQLGlot and skip DDL-only helpers for plain DML jobs

- **Status**: Proposed

## Context

[ADR 0051](0051-plain-select-fast-path-and-translation-cache.md) removed the
repeated parses from the plain `SELECT` path. A write-heavy client still pays
them. A change-data-capture client that applies each batch with one `MERGE`
or `INSERT` per table sends large statements: the captured workload had 406
`MERGE` and `INSERT` jobs on tables of 115 to 344 columns, with a median SQL
size of 5.4 KB and a maximum of 24 KB.

The workload was replayed 3 times on 16 client threads against the image of
[ADR 0052](0052-auto-snapshots-opt-in.md) (`BQEMU_AUTO_SNAPSHOTS` off) with
pure-Python SQLGlot 30.11.0. The result was 526 jobs per minute with a median
job time of 1.67 s. A `py-spy` profile of the same run showed:

- The event loop was 71% busy, and 81% of the loop time was inside SQLGlot
  (35.6% in the tokenizer).
- Each DML statement is parsed many times. The shares of loop time were:
  `_detect_catalog_drop` 6.9%, `_detect_plain_create_table` 6.1%,
  `_detect_create_schema` 3.8%, `_detect_plain_create_view` 3.3%, the
  `_capture_dml_snapshots` parse 3.8%, the `_reject_dml_on_immutable` parse
  3.8%, the classification parse 3.4%, `rewrite_create_table_schema_ctas` 3.4%
  and the row-access rewrite 3.3%. Translation took about 20%.
- The first four helpers and `rewrite_create_table_schema_ctas` only act on
  DDL. Together they took 23.5% of the loop time.

A count of `sqlglot.parse_one` calls for one small `MERGE`, `INSERT`, `UPDATE`
or `DELETE` job showed 16 calls per statement before this change.

## Decision

**Compiled SQLGlot.** A new optional extra, `compiled`, installs
`sqlglot[c]` in the same version range as the core dependency
(`>=30.9,<30.12`). SQLGlot then pulls `sqlglotc`, the mypyc-compiled build of
the same parser, tokenizer and generator, pinned to the exact SQLGlot
version. The `all` extra includes `compiled`, so the Docker image
(`pip install ".[all]"`) and `pip install "bqemulator[all]"` use it. Plain
`pip install bqemulator` keeps the pure-Python SQLGlot, and the core
dependency range is unchanged. `uv.lock` records `sqlglotc`. With `sqlglotc`
30.11.0 installed, the same replay gave 866 jobs per minute with a median job
time of 1.05 s and no errors.

The CI test jobs that sync `--extra all` run on compiled SQLGlot. One unit and
property job removes `sqlglotc` after the sync, so the pure-Python SQLGlot of a
plain install stays tested. The fuzz job removes `sqlglotc` too, because
Atheris instruments only Python bytecode and a compiled parser would give the
fuzzer no coverage feedback.

The `sqlglotc` 30.11.0 wheels cover CPython 3.11 to 3.14 on manylinux x86_64
and aarch64, macOS universal2 and Windows amd64. There is no musllinux wheel,
so a plain install on Alpine would build from source. This is why `compiled`
is an extra and not a core dependency.

The code base does not subclass SQLGlot classes (`Dialect`, `Parser`,
`Generator`, `Tokenizer`, `Expression` and its subclasses) and does not assign
attributes on them. It reads the tree, copies nodes, sets `node.meta` and
passes its own `expression_metadata` and `binary_coercions` to
`TypeAnnotator`. The unit, integration, property, chaos and conformance tiers
pass on an image with `sqlglotc` and on one without it.

**Plain DML fast path.** `execute_query_job` computes
`skip_ddl_helpers = is_plain_select or (not is_scripted and statement_type in
{INSERT, UPDATE, DELETE, MERGE, TRUNCATE_TABLE})`. `statement_type` comes from
`classify_statement_type`. It tests the row access policy DDL regexes first,
then takes the single `parse_one` tree. A statement is classified as DML only
when that tree is an `Insert`, `Update`, `Delete`, `Merge`, `TruncateTable` or
a `Command` for `TRUNCATE`. For such a statement the job skips the steps
below. Each step is a no-op for a DML tree, on every branch:

- **`_run_query_fast_paths`.** The routine-drop detector matches the text
  `DROP TABLE FUNCTION`, or an `exp.Drop` tree. The row access DDL detector
  uses the same two regular expressions on the same normalized text that the
  classification already tested, so it cannot match a statement that was
  classified as DML. The versioning DDL detector tests that the text contains
  `SNAPSHOT`, `CLONE` or `MATERIALIZED VIEW`, then matches regular expressions
  anchored at `CREATE`, `DROP`, `CALL` or `REFRESH`. A DML text starts with
  its own verb, so no pattern matches.
- **`_guard_and_resolve_single_ddl`.** `assert_drop_schema_allowed` returns
  unless the tree is an `exp.Drop` of kind `SCHEMA`. `resolve_ddl_operation`
  branches on the CREATE and DROP statement types and otherwise returns
  `ddl_operation_for(statement_type)`, which is `""` for the five DML types.
  The statistics builder treats `None` and `""` alike, so
  `ddlOperationPerformed` stays absent.
- **The five `sync_*` catalog syncs** in `_run_query_body`. Each requires a
  `Create` of a schema, a `Create` of a table (not a clone), a `Create` of a
  view, a catalog `Drop` or an `Alter` of a table. A DML tree is none of them,
  and each helper returns before it reads the catalog.
- **`detect_create_table` in `_build_query_configuration`.**
  `_detect_plain_create_table` returns `None` unless the tree is a `Create` of
  kind `TABLE`, so the anonymous destination table is used, as before.
- **`rewrite_create_table_schema_ctas`.** It only changes `Create` nodes of
  kind `TABLE`. The translator calls it for every statement, for scripts and
  for `EXECUTE IMMEDIATE` too, so the guard sits in the rewriter. A text
  without the word `CREATE` is returned unchanged without a parse, because a
  `Create` node needs that keyword in the text. The guard applies to plain
  `SELECT` statements as well. A DML text that contains the word (for example
  a quoted column named `create`) is parsed and rewritten as before.

The job keeps these steps for DML:

- `_reject_dml_on_immutable`: BigQuery refuses DML on snapshot and
  materialized view tables.
- `_capture_dml_snapshots`: it publishes `TableDataChanged` for each target
  table with `BQEMU_AUTO_SNAPSHOTS` off or on, and materialized views depend on
  that event. It captures a snapshot only when the setting is on
  ([ADR 0052](0052-auto-snapshots-opt-in.md)).
- The materialized-view refresh, row access rewrite, translation, parameter
  binding and execution, and `numDmlAffectedRows`.
- The write lane: DML still runs under the write lock and never on the
  parallel read workers ([ADR 0050](0050-parallel-select-workers.md)).

`classify_statement_type` now has an internal variant that also returns the
parsed tree. `_reject_dml_on_immutable` and `_capture_dml_snapshots` accept
that tree, so the job parses the text once for these three steps instead of
three times. Both helpers only read the tree. A caller that gives no tree (the
scripting path) gets the previous behavior, a parse inside the helper. After
this change a plain DML job makes 5 `parse_one` calls instead of 16.

A test over DML texts with DDL words in comments, literals and identifiers
asserts that each skipped helper is a no-op for them. A second test asserts
that every DDL form still reaches every helper. A future side effect added to
one of these helpers fails a test instead of being skipped silently.

## Alternatives considered

- **Put `sqlglotc` in the core dependencies.** Rejected: a platform without a
  wheel would need a C compiler to install a library, and library users did
  not ask for compiled code.
- **Add the Rust tokenizer (`sqlglotrs`) too.** Not measured. `sqlglotc`
  already compiles the Python tokenizer, which was the largest single cost.
- **Pass a flag through the translator to skip
  `rewrite_create_table_schema_ctas` for DML.** Rejected: it adds a parameter
  to the translator and the translation cache key for no gain over the keyword
  guard, which is correct for every input.
- **Parse once and pass the tree to every helper.** It would also cut the
  translator parses, but it changes about 12 signatures. The same reason
  applied in ADR 0051. This change shares the tree only where the executor
  already holds it.
- **Skip `_reject_dml_on_immutable` for DML on tables that are not
  immutable.** Rejected: the check needs the catalog entry of each target, and
  that is the work the helper does.

## Consequences

- **Positive**: the replay above ran 1.65 times faster with `sqlglotc`
  (526 to 866 jobs per minute). The fast path removes 11 of the 16 parses of a
  small DML statement. Its throughput effect alone was not measured.
- **Negative**: the installed SQLGlot grows from 8 MB to 115 MB (CPython 3.14,
  aarch64), because each compiled module is a separate shared library.
- **Negative**: `sqlglotc` must match the SQLGlot version exactly, so the
  `compiled` extra moves with the SQLGlot range. A range change needs
  `make lock`.
- **Negative**: the fast path skips helpers without a warning if one of them
  gains a DML-relevant side effect. The soundness test guards this.
- **Negative**: code that subclasses or patches SQLGlot classes can fail with
  compiled classes. Nothing in the code base does this today.
