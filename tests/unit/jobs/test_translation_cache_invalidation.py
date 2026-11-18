"""A cached translation never hides a change of live state (ADR 0051).

The passes before the cached step read the catalog, the row-access
policies and the snapshots on every call, so the same query text must
follow each change.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import pytest_asyncio

from bqemulator.api.dependencies import AppContext
from bqemulator.catalog.etag import generate_etag
from bqemulator.catalog.memory_repository import MemoryCatalogRepository
from bqemulator.catalog.models import DatasetMeta
from bqemulator.config import Settings
from bqemulator.domain.clock import FrozenClock
from bqemulator.domain.events import EventBus
from bqemulator.jobs.executor import _run_single_sql, execute_query_job
from bqemulator.observability.metrics import MetricsRegistry
from bqemulator.row_access.identity import CallerIdentity
from bqemulator.row_access.policy import RowAccessPolicyManager
from bqemulator.storage.engine import DuckDBEngine
from bqemulator.udf.runtime import UDFRegistry
from bqemulator.versioning.snapshots import SnapshotManager

pytestmark = [
    pytest.mark.unit,
    pytest.mark.filterwarnings("ignore:Arrow output on a DB-API result"),
]

_NOW = datetime(2026, 5, 16, tzinfo=UTC)
_ANON = CallerIdentity(principal="user:anon@bqemulator.local", is_authenticated=False)


@pytest_asyncio.fixture
async def clock() -> FrozenClock:
    return FrozenClock(_NOW)


@pytest_asyncio.fixture
async def ctx(ephemeral_settings: Settings, clock: FrozenClock) -> AsyncIterator[AppContext]:
    """In-process ``AppContext`` with an empty dataset ``p.ds``."""
    engine = DuckDBEngine(ephemeral_settings)
    await engine.start()
    catalog = MemoryCatalogRepository()
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
    engine.execute('CREATE SCHEMA IF NOT EXISTS "p__ds"')
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
        ),
        row_access=RowAccessPolicyManager(catalog=catalog, clock=clock),
    )
    try:
        yield context
    finally:
        await engine.stop()


async def _rows(ctx: AppContext, sql: str) -> list[dict[str, Any]]:
    table = await _run_single_sql("p", sql, None, ctx, caller=_ANON)
    return table.to_pylist()  # type: ignore[no-any-return]


async def _job(ctx: AppContext, job_id: str, sql: str) -> None:
    await execute_query_job("p", job_id, sql, None, ctx)


async def test_row_access_policy_created_after_a_cached_run_applies(ctx: AppContext) -> None:
    await _job(ctx, "j1", "CREATE TABLE ds.t AS SELECT 1 AS id UNION ALL SELECT 2")
    sql = "SELECT id FROM ds.t ORDER BY id"
    assert await _rows(ctx, sql) == [{"id": 1}, {"id": 2}]
    assert await _rows(ctx, sql) == [{"id": 1}, {"id": 2}]
    await _job(
        ctx,
        "j2",
        'CREATE ROW ACCESS POLICY only_alice ON ds.t GRANT TO ("user:alice@example.com") '
        "FILTER USING (id = 1)",
    )
    assert await _rows(ctx, sql) == []


async def test_new_table_shows_in_information_schema_after_a_cached_run(ctx: AppContext) -> None:
    await _job(ctx, "j1", "CREATE TABLE ds.a AS SELECT 1 AS id")
    sql = "SELECT table_name FROM ds.INFORMATION_SCHEMA.TABLES ORDER BY table_name"
    assert await _rows(ctx, sql) == [{"table_name": "a"}]
    assert await _rows(ctx, sql) == [{"table_name": "a"}]
    await _job(ctx, "j2", "CREATE TABLE ds.b AS SELECT 1 AS id")
    assert await _rows(ctx, sql) == [{"table_name": "a"}, {"table_name": "b"}]


async def test_new_wildcard_shard_is_read_after_a_cached_run(ctx: AppContext) -> None:
    await _job(ctx, "j1", "CREATE TABLE ds.events_1 AS SELECT 1 AS id")
    sql = "SELECT COUNT(*) AS n FROM ds.events_*"
    assert await _rows(ctx, sql) == [{"n": 1}]
    assert await _rows(ctx, sql) == [{"n": 1}]
    await _job(ctx, "j2", "CREATE TABLE ds.events_2 AS SELECT 2 AS id")
    assert await _rows(ctx, sql) == [{"n": 2}]


async def test_time_travel_follows_the_snapshot_for_each_target(
    ctx: AppContext,
    clock: FrozenClock,
) -> None:
    await _job(ctx, "j1", "CREATE TABLE ds.t AS SELECT 1 AS id")
    clock.advance(seconds=10)
    await _job(ctx, "j2", "INSERT INTO ds.t (id) VALUES (2)")
    clock.advance(seconds=10)
    await _job(ctx, "j3", "INSERT INTO ds.t (id) VALUES (3)")
    clock.advance(seconds=10)

    def at(offset_seconds: int) -> str:
        stamp = (_NOW + timedelta(seconds=offset_seconds)).strftime("%Y-%m-%d %H:%M:%S")
        return f"SELECT COUNT(*) AS n FROM ds.t FOR SYSTEM_TIME AS OF TIMESTAMP '{stamp}'"

    assert await _rows(ctx, at(12)) == [{"n": 2}]
    assert await _rows(ctx, at(22)) == [{"n": 3}]
    assert await _rows(ctx, at(12)) == [{"n": 2}]
