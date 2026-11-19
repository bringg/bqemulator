"""A plain single-statement SELECT skips the DDL/DML bookkeeping helpers (ADR 0051)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import pytest
import pytest_asyncio

from bqemulator.api.dependencies import AppContext
from bqemulator.catalog import ddl_sync
from bqemulator.catalog.etag import generate_etag
from bqemulator.catalog.memory_repository import MemoryCatalogRepository
from bqemulator.catalog.models import DatasetMeta, TableMeta
from bqemulator.config import Settings
from bqemulator.domain.clock import FrozenClock
from bqemulator.domain.events import EventBus
from bqemulator.jobs import executor
from bqemulator.jobs.executor import execute_query_job
from bqemulator.observability.metrics import MetricsRegistry
from bqemulator.row_access.policy import RowAccessPolicyManager
from bqemulator.storage.engine import DuckDBEngine
from bqemulator.udf.runtime import UDFRegistry
from bqemulator.versioning.snapshots import SnapshotManager

pytestmark = [
    pytest.mark.unit,
    pytest.mark.filterwarnings("ignore:Arrow output on a DB-API result"),
]

_NOW = datetime(2026, 5, 16, tzinfo=UTC)

# (module, attribute) of every helper a plain SELECT must not reach.
_SKIPPED_HELPERS = [
    (executor, "_run_query_fast_paths"),
    (executor, "_reject_dml_on_immutable"),
    (executor, "_guard_and_resolve_single_ddl"),
    (executor, "_capture_dml_snapshots"),
    (executor, "sync_created_schema"),
    (executor, "sync_created_table"),
    (executor, "sync_created_view"),
    (executor, "sync_dropped_object"),
    (executor, "sync_altered_table"),
    (ddl_sync, "_detect_plain_create_table"),
]


@pytest_asyncio.fixture
async def ctx(ephemeral_settings: Settings) -> AsyncIterator[AppContext]:
    """In-process ``AppContext`` with a table ``p.ds.t``."""
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
    catalog.create_table(
        TableMeta(
            project_id="p",
            dataset_id="ds",
            table_id="t",
            table_type="TABLE",
            creation_time=_NOW,
            last_modified_time=_NOW,
            etag=generate_etag("p", "ds", "t", str(_NOW)),
        ),
    )
    engine.execute('CREATE SCHEMA IF NOT EXISTS "p__ds"')
    engine.execute('CREATE TABLE "p__ds"."t" (id BIGINT)')
    engine.execute('INSERT INTO "p__ds"."t" VALUES (1), (2)')
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
            auto_snapshots=True,
        ),
        row_access=RowAccessPolicyManager(catalog=catalog, clock=clock),
    )
    try:
        yield context
    finally:
        await engine.stop()


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record the name of every skippable helper that runs; each still calls through."""
    seen: list[str] = []
    for module, name in _SKIPPED_HELPERS:
        original = getattr(module, name)

        def make_spy(helper_name: str, helper: Any) -> Any:
            if helper_name in ("_run_query_fast_paths", "_capture_dml_snapshots"):

                async def async_spy(*args: Any, **kwargs: Any) -> Any:
                    seen.append(helper_name)
                    return await helper(*args, **kwargs)

                return async_spy

            def spy(*args: Any, **kwargs: Any) -> Any:
                seen.append(helper_name)
                return helper(*args, **kwargs)

            return spy

        monkeypatch.setattr(module, name, make_spy(name, original))
    return seen


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id FROM ds.t",
        "WITH x AS (SELECT 1 AS id) SELECT * FROM x",
        "SELECT id FROM ds.t;",
    ],
)
async def test_plain_select_skips_ddl_dml_helpers(
    ctx: AppContext,
    calls: list[str],
    sql: str,
) -> None:
    job = await execute_query_job("p", "job_sel", sql, None, ctx)
    assert calls == []
    stats = job.statistics["query"]
    assert stats["statementType"] == "SELECT"
    assert "ddlOperationPerformed" not in stats
    destination = job.configuration["query"]["destinationTable"]
    assert destination["datasetId"] == "_bqemu_anonymous"


@pytest.mark.parametrize("sql", ["SELECT 1 UNION ALL SELECT 2", "(SELECT 1)"])
async def test_non_plain_select_keeps_every_helper(
    ctx: AppContext,
    calls: list[str],
    sql: str,
) -> None:
    await execute_query_job("p", "job_union", sql, None, ctx)
    assert set(calls) == {name for _, name in _SKIPPED_HELPERS}


async def test_script_keeps_dml_bookkeeping(ctx: AppContext, calls: list[str]) -> None:
    await execute_query_job("p", "job_script", "SELECT 1; SELECT 2", None, ctx)
    assert "_reject_dml_on_immutable" in calls
    assert "_capture_dml_snapshots" in calls
    assert "_run_query_fast_paths" not in calls


async def test_insert_still_captures_snapshot_and_syncs(ctx: AppContext, calls: list[str]) -> None:
    job = await execute_query_job("p", "job_ins", "INSERT INTO ds.t (id) VALUES (3)", None, ctx)
    assert "_capture_dml_snapshots" in calls
    assert "_reject_dml_on_immutable" in calls
    assert job.statistics["query"]["statementType"] == "INSERT"
    assert len(ctx.catalog.list_snapshots_for_table("p", "ds", "t")) == 1


async def test_create_table_as_select_still_syncs_catalog(
    ctx: AppContext, calls: list[str]
) -> None:
    job = await execute_query_job(
        "p",
        "job_ctas",
        "CREATE TABLE ds.t3 AS SELECT id FROM ds.t",
        None,
        ctx,
    )
    assert "sync_created_table" in calls
    assert "_detect_plain_create_table" in calls
    assert ctx.catalog.get_table("p", "ds", "t3") is not None
    destination = job.configuration["query"]["destinationTable"]
    assert destination["tableId"] == "t3"


_TRICKY_SELECTS = [
    "/* CREATE SNAPSHOT TABLE a.b CLONE c.d */ SELECT 1",
    "-- DROP TABLE ds.t\nSELECT 1",
    "SELECT 'CREATE ROW ACCESS POLICY p ON ds.t' AS s",
    "SELECT 1 /* DROP SCHEMA ds CASCADE */",
    "SELECT 1 -- CREATE MATERIALIZED VIEW ds.v AS SELECT 1",
    "SELECT `id` FROM `p.ds.t`;",
    "SELECT id FROM ds.t FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP()",
]


@pytest.mark.parametrize("sql", _TRICKY_SELECTS)
async def test_skipped_helpers_are_no_ops_for_a_select(ctx: AppContext, sql: str) -> None:
    """Fails if a skipped helper gains a side effect for a statement classified SELECT."""
    assert executor.classify_statement_type(sql) == "SELECT"
    tables_before = ctx.catalog.list_tables("p", "ds")
    assert (
        await executor._run_query_fast_paths(
            project_id="p",
            job_id="j",
            bq_sql=sql,
            statement_type="SELECT",
            now=_NOW,
            ctx=ctx,
        )
        is None
    )
    executor._reject_dml_on_immutable("p", sql, ctx)
    assert executor._guard_and_resolve_single_ddl(sql, "SELECT", "p", ctx) == ""
    assert ddl_sync._detect_plain_create_table(sql) is None
    assert ddl_sync._detect_catalog_drop(sql) is None
    for sync in (
        ddl_sync.sync_created_schema,
        ddl_sync.sync_created_table,
        ddl_sync.sync_created_view,
        ddl_sync.sync_dropped_object,
        ddl_sync.sync_altered_table,
    ):
        sync(sql, "p", ctx)
    assert ctx.catalog.list_tables("p", "ds") == tables_before
    assert not ctx.catalog.list_snapshots_for_table("p", "ds", "t")
