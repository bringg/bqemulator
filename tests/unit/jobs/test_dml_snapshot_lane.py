"""AUTO snapshots of DML targets are copied on the snapshot lane (ADR 0052)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import pytest
import pytest_asyncio

from bqemulator.api.dependencies import AppContext
from bqemulator.catalog.etag import generate_etag
from bqemulator.catalog.memory_repository import MemoryCatalogRepository
from bqemulator.catalog.models import DatasetMeta, TableMeta
from bqemulator.config import Settings
from bqemulator.domain.clock import FrozenClock
from bqemulator.domain.events import EventBus, TableDataChanged
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


@pytest_asyncio.fixture
async def ctx(ephemeral_settings: Settings) -> AsyncIterator[AppContext]:
    """In-process ``AppContext`` with tables ``p.ds.t1`` and ``p.ds.t2``."""
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
    engine.execute('CREATE SCHEMA IF NOT EXISTS "p__ds"')
    for name in ("t1", "t2"):
        catalog.create_table(
            TableMeta(
                project_id="p",
                dataset_id="ds",
                table_id=name,
                table_type="TABLE",
                creation_time=_NOW,
                last_modified_time=_NOW,
                etag=generate_etag("p", "ds", name, str(_NOW)),
            ),
        )
        engine.execute(f'CREATE TABLE "p__ds"."{name}" (id BIGINT)')
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


async def test_insert_copies_one_auto_snapshot_on_the_lane(
    ctx: AppContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published: list[TableDataChanged] = []
    ctx.events.subscribe(TableDataChanged, published.append)  # type: ignore[arg-type]
    lane_calls = 0
    original = ctx.engine.run_on_snapshot_lane

    async def spy(fn: Any) -> Any:
        nonlocal lane_calls
        lane_calls += 1
        return await original(fn)

    monkeypatch.setattr(ctx.engine, "run_on_snapshot_lane", spy)

    await execute_query_job("p", "j1", "INSERT INTO ds.t1 (id) VALUES (1), (2)", None, ctx)

    assert lane_calls == 1
    (snap,) = ctx.catalog.list_snapshots_for_table("p", "ds", "t1")
    assert snap.kind == "AUTO"
    rows = ctx.engine.fetch_arrow(
        f'SELECT id FROM "{snap.duckdb_schema}"."{snap.duckdb_table}" ORDER BY id',
    ).to_pylist()
    assert rows == [{"id": 1}, {"id": 2}]
    assert [(e.dataset_id, e.table_id) for e in published] == [("ds", "t1")]


async def test_concurrent_dml_jobs_capture_one_table_at_a_time(
    ctx: AppContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    original = ctx.engine.run_on_snapshot_lane

    async def spy(fn: Any) -> Any:
        events.append("start")
        try:
            return await original(fn)
        finally:
            events.append("end")

    monkeypatch.setattr(ctx.engine, "run_on_snapshot_lane", spy)

    await asyncio.gather(
        execute_query_job("p", "j1", "INSERT INTO ds.t1 (id) VALUES (1)", None, ctx),
        execute_query_job("p", "j2", "INSERT INTO ds.t2 (id) VALUES (2)", None, ctx),
    )

    assert events == ["start", "end", "start", "end"]
    assert len(ctx.catalog.list_snapshots_for_table("p", "ds", "t1")) == 1
    assert len(ctx.catalog.list_snapshots_for_table("p", "ds", "t2")) == 1


async def test_insert_into_a_table_dropped_before_the_copy_stays_done(ctx: AppContext) -> None:
    """The DML already committed; a vanished source must not fail the job."""
    await execute_query_job("p", "j1", "INSERT INTO ds.t1 (id) VALUES (1)", None, ctx)
    original = ctx.engine.run_on_snapshot_lane

    async def drop_then_copy(fn: Any) -> Any:
        ctx.engine.execute('DROP TABLE "p__ds"."t1"')
        return await original(fn)

    ctx.engine.run_on_snapshot_lane = drop_then_copy  # type: ignore[method-assign]
    job = await execute_query_job("p", "j2", "DELETE FROM ds.t1 WHERE id = 1", None, ctx)
    assert job.state == "DONE"
