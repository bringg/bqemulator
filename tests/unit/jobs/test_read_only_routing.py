"""``execute_query_job`` sends only plain SELECTs to the parallel read lane."""

from __future__ import annotations

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
from bqemulator.domain.events import EventBus
from bqemulator.jobs.executor import execute_query_job
from bqemulator.observability.metrics import MetricsRegistry
from bqemulator.row_access.policy import RowAccessPolicyManager
from bqemulator.storage.engine import DuckDBEngine
from bqemulator.udf.runtime import UDFRegistry
from bqemulator.versioning.snapshots import SnapshotManager

# DuckDB 2.x warns on the Arrow conversion, and the repo filter makes warnings errors.
pytestmark = [
    pytest.mark.unit,
    pytest.mark.filterwarnings("ignore:Arrow output on a DB-API result"),
]

_NOW = datetime(2026, 5, 16, tzinfo=UTC)


@pytest_asyncio.fixture
async def ctx(ephemeral_settings: Settings) -> AsyncIterator[AppContext]:
    """In-process ``AppContext`` with a table ``p.ds.t`` and a parallel read pool."""
    settings = ephemeral_settings.model_copy(update={"query_workers": 2})
    engine = DuckDBEngine(settings)
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
    context = AppContext(
        settings=settings,
        clock=clock,
        engine=engine,
        catalog=catalog,
        metrics=MetricsRegistry(),
        events=events,
        udf_registry=UDFRegistry(settings),
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


@pytest.fixture
def lanes(ctx: AppContext, monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, bool]]:
    """Record ``(sql, read_only)`` for every statement sent to the engine's off-loop path."""
    seen: list[tuple[str, bool]] = []
    original = ctx.engine.fetch_arrow_off_loop

    async def spy(sql: str, parameters: list[Any] | None = None, *, read_only: bool = False) -> Any:
        seen.append((sql, read_only))
        return await original(sql, parameters, read_only=read_only)

    monkeypatch.setattr(ctx.engine, "fetch_arrow_off_loop", spy)
    return seen


async def test_select_job_uses_read_lane(ctx: AppContext, lanes: list[tuple[str, bool]]) -> None:
    await execute_query_job("p", "job_sel", "SELECT id FROM ds.t", None, ctx)
    assert [read_only for _, read_only in lanes] == [True]


async def test_dml_job_uses_writer_lane(ctx: AppContext, lanes: list[tuple[str, bool]]) -> None:
    await execute_query_job("p", "job_ins", "INSERT INTO ds.t (id) VALUES (1)", None, ctx)
    assert [read_only for _, read_only in lanes] == [False]
    rows = ctx.engine.fetch_arrow('SELECT id FROM "p__ds"."t"').to_pylist()
    assert rows == [{"id": 1}]


async def test_ddl_job_uses_writer_lane(ctx: AppContext, lanes: list[tuple[str, bool]]) -> None:
    await execute_query_job("p", "job_ddl", "CREATE TABLE ds.t2 (id INT64)", None, ctx)
    assert [read_only for _, read_only in lanes] == [False]


async def test_script_never_uses_read_lane(
    ctx: AppContext,
    lanes: list[tuple[str, bool]],
) -> None:
    await execute_query_job("p", "job_script", "SELECT 1; SELECT id FROM ds.t;", None, ctx)
    assert not any(read_only for _, read_only in lanes)
