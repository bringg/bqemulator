"""A plain single-statement DML job skips the DDL-only helpers (ADR 0053)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
import inspect
from types import SimpleNamespace
from typing import Any

import pytest
import pytest_asyncio
import sqlglot

from bqemulator.api.dependencies import AppContext
from bqemulator.catalog import ddl_sync
from bqemulator.catalog.etag import generate_etag
from bqemulator.catalog.memory_repository import MemoryCatalogRepository
from bqemulator.catalog.models import DatasetMeta, TableMeta
from bqemulator.config import Settings
from bqemulator.domain.clock import FrozenClock
from bqemulator.domain.errors import InvalidQueryError
from bqemulator.domain.events import EventBus, TableDataChanged
from bqemulator.jobs import executor
from bqemulator.jobs.executor import classify_statement_type, execute_query_job
from bqemulator.observability.metrics import MetricsRegistry
from bqemulator.row_access.policy import RowAccessPolicyManager
from bqemulator.sql.rewriter import create_table_schema_ctas
from bqemulator.storage.engine import DuckDBEngine
from bqemulator.udf.runtime import UDFRegistry
from bqemulator.versioning.snapshots import SnapshotManager

pytestmark = [
    pytest.mark.unit,
    pytest.mark.filterwarnings("ignore:Arrow output on a DB-API result"),
]

_NOW = datetime(2026, 5, 16, tzinfo=UTC)

# (module, attribute) of every helper a plain DML statement must not reach.
_SKIPPED_HELPERS = [
    (executor, "_run_query_fast_paths"),
    (executor, "_guard_and_resolve_single_ddl"),
    (executor, "sync_created_schema"),
    (executor, "sync_created_table"),
    (executor, "sync_created_view"),
    (executor, "sync_dropped_object"),
    (executor, "sync_altered_table"),
    (ddl_sync, "_detect_plain_create_table"),
    (ddl_sync, "_detect_plain_create_view"),
    (ddl_sync, "_detect_create_schema"),
    (ddl_sync, "_detect_catalog_drop"),
]

# Helpers a plain DML statement still runs.
_KEPT_HELPERS = [
    (executor, "_reject_dml_on_immutable"),
    (executor, "_capture_dml_snapshots"),
]

# (sql, statementType, numDmlAffectedRows) on the fixture table ds.t = {1, 2}.
_DML_CASES = [
    ("INSERT INTO ds.t (id) VALUES (3)", "INSERT", "1"),
    ("INSERT INTO ds.t (id) SELECT id + 100 FROM ds.t", "INSERT", "2"),
    ("UPDATE ds.t SET id = id + 10 WHERE id > 0", "UPDATE", "2"),
    ("DELETE FROM ds.t WHERE id = 1", "DELETE", "1"),
    (
        (
            "MERGE ds.t AS T USING (SELECT 1 AS id) AS S ON T.id = S.id "
            "WHEN MATCHED THEN UPDATE SET id = 100 "
            "WHEN NOT MATCHED THEN INSERT (id) VALUES (S.id)"
        ),
        "MERGE",
        "1",
    ),
    ("TRUNCATE TABLE ds.t", "TRUNCATE_TABLE", "2"),
]


def _add_table(ctx: AppContext, table_id: str, table_type: str) -> None:
    ctx.catalog.create_table(
        TableMeta(
            project_id="p",
            dataset_id="ds",
            table_id=table_id,
            table_type=table_type,  # type: ignore[arg-type]
            creation_time=_NOW,
            last_modified_time=_NOW,
            etag=generate_etag("p", "ds", table_id, str(_NOW)),
        ),
    )


@pytest_asyncio.fixture(params=[False, True], ids=["snapshots-off", "snapshots-on"])
async def ctx(
    request: pytest.FixtureRequest, ephemeral_settings: Settings
) -> AsyncIterator[AppContext]:
    """In-process ``AppContext`` with a table ``p.ds.t`` holding ids 1 and 2."""
    engine = DuckDBEngine(ephemeral_settings)
    await engine.start()
    catalog = MemoryCatalogRepository()
    clock = FrozenClock(_NOW)
    events = EventBus()
    catalog.create_dataset(
        DatasetMeta(
            project_id="p",
            dataset_id="ds",
            creation_time=_NOW,
            last_modified_time=_NOW,
            etag=generate_etag("p", "ds", str(_NOW)),
        ),
    )
    context = AppContext(
        settings=ephemeral_settings,
        clock=clock,
        engine=engine,
        catalog=catalog,
        metrics=MetricsRegistry(),
        events=events,
        udf_registry=UDFRegistry(ephemeral_settings),
        snapshots=SnapshotManager(
            engine=engine,
            catalog=catalog,
            clock=clock,
            events=events,
            retention_days=7,
            auto_snapshots=request.param,
        ),
        row_access=RowAccessPolicyManager(catalog=catalog, clock=clock),
    )
    _add_table(context, "t", "TABLE")
    engine.execute('CREATE SCHEMA IF NOT EXISTS "p__ds"')
    engine.execute('CREATE TABLE "p__ds"."t" (id BIGINT)')
    engine.execute('INSERT INTO "p__ds"."t" VALUES (1), (2)')
    try:
        yield context
    finally:
        await engine.stop()


def _install_spies(
    monkeypatch: pytest.MonkeyPatch, targets: list[tuple[Any, str]]
) -> list[tuple[str, dict[str, Any]]]:
    """Record ``(name, kwargs)`` of every call to *targets*; each still calls through."""
    seen: list[tuple[str, dict[str, Any]]] = []
    for module, name in targets:
        original = getattr(module, name)

        def make_spy(helper_name: str, helper: Any) -> Any:
            if inspect.iscoroutinefunction(helper):

                async def async_spy(*args: Any, **kwargs: Any) -> Any:
                    seen.append((helper_name, kwargs))
                    return await helper(*args, **kwargs)

                return async_spy

            def spy(*args: Any, **kwargs: Any) -> Any:
                seen.append((helper_name, kwargs))
                return helper(*args, **kwargs)

            return spy

        monkeypatch.setattr(module, name, make_spy(name, original))
    return seen


@pytest.fixture
def skipped_calls(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict[str, Any]]]:
    """Calls of the helpers a plain DML statement must skip."""
    return _install_spies(monkeypatch, _SKIPPED_HELPERS)


@pytest.fixture
def kept_calls(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict[str, Any]]]:
    """Calls of the helpers a plain DML statement must still run."""
    return _install_spies(monkeypatch, _KEPT_HELPERS)


@pytest.fixture
def ctas_parses(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """SQL texts that the CTAS-with-schema rewriter parses."""
    parsed: list[str] = []

    def counting_parse_one(sql: str, **kwargs: Any) -> Any:
        parsed.append(sql)
        return sqlglot.parse_one(sql, **kwargs)

    monkeypatch.setattr(
        create_table_schema_ctas,
        "sqlglot",
        SimpleNamespace(parse_one=counting_parse_one),
    )
    return parsed


@pytest.mark.parametrize(("sql", "statement_type", "affected"), _DML_CASES)
async def test_plain_dml_skips_ddl_helpers(
    ctx: AppContext,
    skipped_calls: list[tuple[str, dict[str, Any]]],
    ctas_parses: list[str],
    sql: str,
    statement_type: str,
    affected: str,
) -> None:
    job = await execute_query_job("p", "job_dml", sql, None, ctx)
    assert skipped_calls == []
    assert ctas_parses == []
    stats = job.statistics["query"]
    assert stats["statementType"] == statement_type
    assert stats["numDmlAffectedRows"] == affected
    assert "ddlOperationPerformed" not in stats
    destination = job.configuration["query"]["destinationTable"]
    assert destination["datasetId"] == "_bqemu_anonymous"


@pytest.mark.parametrize(("sql", "statement_type", "affected"), _DML_CASES)
async def test_plain_dml_keeps_immutable_guard_and_snapshot_capture(
    ctx: AppContext,
    kept_calls: list[tuple[str, dict[str, Any]]],
    sql: str,
    statement_type: str,
    affected: str,
) -> None:
    del statement_type, affected
    await execute_query_job("p", "job_dml", sql, None, ctx)
    assert [name for name, _ in kept_calls] == [
        "_reject_dml_on_immutable",
        "_capture_dml_snapshots",
    ]
    assert all(kwargs["tree"] is not None for _, kwargs in kept_calls)


async def test_plain_dml_parses_the_statement_once_for_the_executor_helpers(
    ctx: AppContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen = _install_spies(
        monkeypatch,
        [(executor, "_parse_for_classification"), (executor, "_parse_dml_candidate")],
    )
    await execute_query_job("p", "job_dml", "INSERT INTO ds.t (id) VALUES (3)", None, ctx)
    assert [name for name, _ in seen] == ["_parse_for_classification"]


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE TABLE ds.t3 AS SELECT id FROM ds.t",
        "CREATE TABLE ds.t3 (id INT64) AS SELECT id FROM ds.t",
        "CREATE VIEW ds.v AS SELECT id FROM ds.t",
        "CREATE SCHEMA ds2",
        "DROP TABLE ds.t",
        "ALTER TABLE ds.t ADD COLUMN extra INT64",
    ],
)
async def test_ddl_keeps_every_ddl_helper(
    ctx: AppContext,
    skipped_calls: list[tuple[str, dict[str, Any]]],
    ctas_parses: list[str],
    sql: str,
) -> None:
    job = await execute_query_job("p", "job_ddl", sql, None, ctx)
    called = {name for name, _ in skipped_calls}
    assert {
        "_run_query_fast_paths",
        "_guard_and_resolve_single_ddl",
        "sync_created_schema",
        "sync_created_table",
        "sync_created_view",
        "sync_dropped_object",
        "sync_altered_table",
    } <= called
    assert bool(ctas_parses) == ("CREATE" in sql)
    assert job.statistics["query"]["ddlOperationPerformed"]


@pytest.mark.parametrize(("sql", "statement_type", "affected"), _DML_CASES)
async def test_dml_publishes_table_data_changed_and_matches_snapshot_setting(
    ctx: AppContext,
    sql: str,
    statement_type: str,
    affected: str,
) -> None:
    del statement_type, affected
    received: list[TableDataChanged] = []
    ctx.events.subscribe(TableDataChanged, received.append)  # type: ignore[arg-type]
    await execute_query_job("p", "job_dml", sql, None, ctx)
    assert received == [TableDataChanged("p", "ds", "t")]
    snapshots = ctx.catalog.list_snapshots_for_table("p", "ds", "t")
    assert len(snapshots) == (1 if ctx.snapshots._auto_snapshots else 0)


async def test_dml_changes_the_rows(ctx: AppContext) -> None:
    await execute_query_job("p", "j1", "INSERT INTO ds.t (id) VALUES (3)", None, ctx)
    await execute_query_job("p", "j2", "UPDATE ds.t SET id = id * 10 WHERE id = 3", None, ctx)
    await execute_query_job("p", "j3", "DELETE FROM ds.t WHERE id = 1", None, ctx)
    rows = ctx.engine.execute('SELECT id FROM "p__ds"."t" ORDER BY id').fetchall()
    assert rows == [(2,), (30,)]


@pytest.mark.parametrize("table_type", ["SNAPSHOT", "MATERIALIZED_VIEW"])
@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO ds.imm (id) VALUES (1)",
        "UPDATE ds.imm SET id = 1 WHERE TRUE",
        "DELETE FROM ds.imm WHERE TRUE",
        "MERGE ds.imm AS T USING (SELECT 1 AS id) AS S ON T.id = S.id WHEN MATCHED THEN DELETE",
        "TRUNCATE TABLE ds.imm",
    ],
)
async def test_dml_on_an_immutable_table_is_rejected(
    ctx: AppContext,
    table_type: str,
    sql: str,
) -> None:
    _add_table(ctx, "imm", table_type)
    with pytest.raises(InvalidQueryError, match=f"Cannot run DML against {table_type} table"):
        await execute_query_job("p", "job_imm", sql, None, ctx)


def _boom(*_args: Any, **_kwargs: Any) -> Any:
    raise AssertionError("the statement must not be parsed again")


async def test_reject_dml_on_immutable_uses_the_given_tree(
    ctx: AppContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _add_table(ctx, "imm", "SNAPSHOT")
    sql = "DELETE FROM ds.imm WHERE TRUE"
    tree = sqlglot.parse_one(sql, read="bigquery")
    monkeypatch.setattr(sqlglot, "parse_one", _boom)
    with pytest.raises(InvalidQueryError, match="immutable"):
        executor._reject_dml_on_immutable("p", sql, ctx, tree=tree)


async def test_capture_dml_snapshots_uses_the_given_tree(
    ctx: AppContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sql = "INSERT INTO ds.t (id) VALUES (3)"
    tree = sqlglot.parse_one(sql, read="bigquery")
    received: list[TableDataChanged] = []
    ctx.events.subscribe(TableDataChanged, received.append)  # type: ignore[arg-type]
    monkeypatch.setattr(sqlglot, "parse_one", _boom)
    await executor._capture_dml_snapshots("p", sql, ctx, tree=tree)
    assert received == [TableDataChanged("p", "ds", "t")]


@pytest.mark.parametrize("sql", ["SELECT 1", "CREATE TABLE ds.x (id INT64)", "not sql ("])
async def test_helpers_without_a_tree_still_parse_and_ignore_non_dml(
    ctx: AppContext,
    sql: str,
) -> None:
    executor._reject_dml_on_immutable("p", sql, ctx)
    await executor._capture_dml_snapshots("p", sql, ctx)


# DML whose text carries DDL words in comments, literals and identifiers.
_TRICKY_DML = [
    "/* CREATE SNAPSHOT TABLE a.b CLONE c.d */ INSERT INTO ds.t (id) VALUES (3)",
    "-- DROP TABLE ds.t\nDELETE FROM ds.t WHERE id = 1",
    "UPDATE ds.t SET id = 1 WHERE 'CREATE ROW ACCESS POLICY p ON ds.t' != ''",
    "INSERT INTO ds.t (id) SELECT 1 /* DROP SCHEMA ds CASCADE */",
    "INSERT INTO ds.t (id) SELECT 1 -- CREATE MATERIALIZED VIEW ds.v AS SELECT 1",
    "INSERT INTO `p.ds.t` (id) VALUES (3);",
    "DELETE FROM ds.t WHERE CAST(id AS STRING) NOT IN ('SNAPSHOT', 'CLONE', 'DROP TABLE x')",
    "MERGE ds.t AS T USING (SELECT 1 AS id) AS S ON T.id = S.id WHEN MATCHED THEN DELETE",
    "TRUNCATE TABLE ds.t",
]


@pytest.mark.parametrize("sql", _TRICKY_DML)
async def test_skipped_helpers_are_no_ops_for_a_dml_statement(ctx: AppContext, sql: str) -> None:
    """Fails if a skipped helper gains a side effect for a statement classified as plain DML."""
    statement_type = classify_statement_type(sql)
    assert statement_type in executor._PLAIN_DML_TYPES
    tables_before = ctx.catalog.list_tables("p", "ds")
    datasets_before = ctx.catalog.list_datasets("p")
    assert (
        await executor._run_query_fast_paths(
            project_id="p",
            job_id="j",
            bq_sql=sql,
            statement_type=statement_type,
            now=_NOW,
            ctx=ctx,
        )
        is None
    )
    assert executor._guard_and_resolve_single_ddl(sql, statement_type, "p", ctx) == ""
    assert ddl_sync._detect_plain_create_table(sql) is None
    assert ddl_sync._detect_plain_create_view(sql) == (None, None)
    assert ddl_sync._detect_create_schema(sql, "p") is None
    assert ddl_sync._detect_catalog_drop(sql) is None
    for sync in (
        ddl_sync.sync_created_schema,
        ddl_sync.sync_created_table,
        ddl_sync.sync_created_view,
        ddl_sync.sync_dropped_object,
        ddl_sync.sync_altered_table,
    ):
        sync(sql, "p", ctx)
    assert create_table_schema_ctas.rewrite_create_table_schema_ctas(sql) == sql
    assert ctx.catalog.list_tables("p", "ds") == tables_before
    assert ctx.catalog.list_datasets("p") == datasets_before
    assert not ctx.catalog.list_snapshots_for_table("p", "ds", "t")


async def test_set_operation_is_not_plain_dml(
    ctx: AppContext,
    skipped_calls: list[tuple[str, dict[str, Any]]],
) -> None:
    await execute_query_job("p", "job_union", "SELECT 1 UNION ALL SELECT 2", None, ctx)
    assert {name for name, _ in skipped_calls} >= {
        "_run_query_fast_paths",
        "_guard_and_resolve_single_ddl",
    }


async def test_script_of_dml_statements_is_not_plain_dml(
    ctx: AppContext,
    skipped_calls: list[tuple[str, dict[str, Any]]],
) -> None:
    sql = "INSERT INTO ds.t (id) VALUES (3); INSERT INTO ds.t (id) VALUES (4)"
    await execute_query_job("p", "job_script", sql, None, ctx)
    assert "_run_query_fast_paths" not in {name for name, _ in skipped_calls}
    rows = ctx.engine.execute('SELECT id FROM "p__ds"."t" ORDER BY id').fetchall()
    assert rows == [(1,), (2,), (3,), (4,)]
