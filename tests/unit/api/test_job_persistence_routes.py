"""Job routes persist rows through the job lane with a DuckDB-backed catalog (ADR 0052)."""

from __future__ import annotations

from collections.abc import AsyncIterator

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest
import pytest_asyncio

from bqemulator.api.app import create_app
from bqemulator.api.dependencies import AppContext
from bqemulator.catalog.duckdb_repository import DuckDBCatalogRepository
from bqemulator.config import Settings
from bqemulator.domain.clock import FrozenClock
from bqemulator.domain.events import EventBus
from bqemulator.observability.metrics import MetricsRegistry
from bqemulator.row_access.policy import RowAccessPolicyManager
from bqemulator.storage.engine import DuckDBEngine
from bqemulator.udf.runtime import UDFRegistry
from bqemulator.versioning.snapshots import SnapshotManager

pytestmark = [
    pytest.mark.unit,
    pytest.mark.filterwarnings("ignore:Arrow output on a DB-API result"),
]


@pytest_asyncio.fixture
async def app_and_engine(
    persistent_settings: Settings,
) -> AsyncIterator[tuple[FastAPI, DuckDBEngine]]:
    engine = DuckDBEngine(persistent_settings)
    await engine.start()
    catalog = DuckDBCatalogRepository(engine)
    catalog.ensure_ready()
    clock = FrozenClock()
    events = EventBus()
    ctx = AppContext(
        settings=persistent_settings,
        clock=clock,
        engine=engine,
        catalog=catalog,
        metrics=MetricsRegistry(),
        events=events,
        udf_registry=UDFRegistry(persistent_settings),
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
        yield create_app(ctx), engine
    finally:
        await engine.stop()


def _stored(engine: DuckDBEngine) -> list[tuple[str, str]]:
    return engine.execute(
        'SELECT job_id, state FROM "_bqemulator_catalog"."jobs" ORDER BY job_id',
    ).fetchall()


def test_jobs_insert_persists_the_job_and_get_reads_it(
    app_and_engine: tuple[FastAPI, DuckDBEngine],
) -> None:
    app, engine = app_and_engine
    client = TestClient(app)
    body = {
        "jobReference": {"projectId": "p", "jobId": "job_a"},
        "configuration": {"query": {"query": "SELECT 1 AS n", "useLegacySql": False}},
    }
    assert client.post("/bigquery/v2/projects/p/jobs", json=body).status_code == 200
    got = client.get("/bigquery/v2/projects/p/jobs/job_a")
    assert got.status_code == 200
    assert got.json()["status"]["state"] == "DONE"
    assert _stored(engine) == [("job_a", "DONE")]


def test_jobs_query_persists_success_and_failure(
    app_and_engine: tuple[FastAPI, DuckDBEngine],
) -> None:
    app, engine = app_and_engine
    client = TestClient(app)
    ok = client.post("/bigquery/v2/projects/p/queries", json={"query": "SELECT 1 AS n"})
    assert ok.status_code == 200
    failed = client.post(
        "/bigquery/v2/projects/p/queries",
        json={"query": "SELECT * FROM ds.missing_table"},
    )
    assert failed.status_code == 200
    assert failed.json().get("errors")
    states = sorted(state for _, state in _stored(engine))
    assert states == ["DONE", "DONE"]
    assert len(_stored(engine)) == 2


@pytest.mark.parametrize("suffix", ["/delete", ""])
def test_job_delete_routes_remove_the_persisted_row(
    app_and_engine: tuple[FastAPI, DuckDBEngine],
    suffix: str,
) -> None:
    app, engine = app_and_engine
    client = TestClient(app)
    body = {
        "jobReference": {"projectId": "p", "jobId": "job_d"},
        "configuration": {"query": {"query": "SELECT 1 AS n", "useLegacySql": False}},
    }
    client.post("/bigquery/v2/projects/p/jobs", json=body)
    assert _stored(engine) == [("job_d", "DONE")]
    assert client.delete(f"/bigquery/v2/projects/p/jobs/job_d{suffix}").status_code == 200
    assert _stored(engine) == []
    assert client.get("/bigquery/v2/projects/p/jobs/job_d").status_code == 404
