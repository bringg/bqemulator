"""Tests for the DuckDB-backed CatalogRepository.

Phase 0 implementation delegates to an in-memory cache; the DuckDB
tables are created via migrations but not yet written through. These
tests exercise the delegation path and the migration hook.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
import threading
from typing import Any

import pytest

from bqemulator.catalog.duckdb_repository import DuckDBCatalogRepository
from bqemulator.catalog.models import (
    DatasetMeta,
    JobMeta,
    MaterializedViewMeta,
    ModelMeta,
    RoutineMeta,
    RowAccessPolicyMeta,
    SnapshotMeta,
    TableMeta,
)
from bqemulator.config import Settings
from bqemulator.domain.errors import NotFoundError
from bqemulator.storage.engine import DuckDBEngine

pytestmark = pytest.mark.unit

NOW = datetime(2026, 4, 15, tzinfo=UTC)


def _ds(project: str = "p", dataset: str = "sales") -> DatasetMeta:
    return DatasetMeta(
        project_id=project,
        dataset_id=dataset,
        creation_time=NOW,
        last_modified_time=NOW,
        etag="e",
    )


def _table(
    project: str = "p",
    dataset: str = "sales",
    table: str = "orders",
) -> TableMeta:
    return TableMeta(
        project_id=project,
        dataset_id=dataset,
        table_id=table,
        creation_time=NOW,
        last_modified_time=NOW,
        etag="e",
    )


def _routine(
    project: str = "p",
    dataset: str = "sales",
    routine: str = "r1",
) -> RoutineMeta:
    return RoutineMeta(
        project_id=project,
        dataset_id=dataset,
        routine_id=routine,
        routine_type="SCALAR_FUNCTION",
        language="SQL",
        definition_body="x",
        creation_time=NOW,
        last_modified_time=NOW,
        etag="e",
    )


def _model(
    project: str = "p",
    dataset: str = "sales",
    model: str = "m1",
) -> ModelMeta:
    return ModelMeta(
        project_id=project,
        dataset_id=dataset,
        model_id=model,
        model_type="LINEAR_REGRESSION",
        labels={"team": "ml"},
        feature_columns=({"name": "x", "type": {"typeKind": "FLOAT64"}},),
        label_columns=({"name": "y", "type": {"typeKind": "FLOAT64"}},),
        encryption_configuration={"kmsKeyName": "projects/p/keys/k"},
        expiration_time=NOW,
        training_query="SELECT x, y FROM p.sales.t",
        creation_time=NOW,
        last_modified_time=NOW,
        etag="e",
    )


@pytest.mark.asyncio
async def test_ensure_ready_runs_migrations(ephemeral_settings: Settings) -> None:
    engine = DuckDBEngine(ephemeral_settings)
    await engine.start()
    try:
        repo = DuckDBCatalogRepository(engine)
        repo.ensure_ready()
        # Idempotent
        repo.ensure_ready()
    finally:
        await engine.stop()


@pytest.mark.asyncio
async def test_dataset_crud_roundtrips(ephemeral_settings: Settings) -> None:
    engine = DuckDBEngine(ephemeral_settings)
    await engine.start()
    try:
        repo = DuckDBCatalogRepository(engine)
        assert repo.list_datasets("p") == ()

        repo.create_dataset(_ds())
        assert repo.get_dataset("p", "sales") is not None
        assert len(repo.list_datasets("p")) == 1

        updated = _ds().model_copy(update={"description": "d"})
        repo.update_dataset(updated)
        assert repo.get_dataset("p", "sales").description == "d"  # type: ignore[union-attr]

        repo.delete_dataset("p", "sales")
        assert repo.get_dataset("p", "sales") is None
    finally:
        await engine.stop()


@pytest.mark.asyncio
async def test_table_crud_roundtrips(ephemeral_settings: Settings) -> None:
    engine = DuckDBEngine(ephemeral_settings)
    await engine.start()
    try:
        repo = DuckDBCatalogRepository(engine)
        repo.create_dataset(_ds())
        repo.create_table(_table())
        assert repo.list_tables("p", "sales") == (_table(),)
        assert repo.get_table("p", "sales", "orders") == _table()

        updated = _table().model_copy(update={"description": "d"})
        repo.update_table(updated)
        assert repo.get_table("p", "sales", "orders").description == "d"  # type: ignore[union-attr]

        repo.delete_table("p", "sales", "orders")
        assert repo.get_table("p", "sales", "orders") is None
    finally:
        await engine.stop()


@pytest.mark.asyncio
async def test_routine_crud_roundtrips(ephemeral_settings: Settings) -> None:
    engine = DuckDBEngine(ephemeral_settings)
    await engine.start()
    try:
        repo = DuckDBCatalogRepository(engine)
        repo.create_dataset(_ds())
        r = repo.create_routine(_routine())
        assert repo.get_routine("p", "sales", "r1") == r
        assert repo.list_routines("p", "sales") == (r,)

        updated = _routine().model_copy(update={"description": "d"})
        repo.update_routine(updated)
        assert repo.get_routine("p", "sales", "r1").description == "d"  # type: ignore[union-attr]

        repo.delete_routine("p", "sales", "r1")
        assert repo.get_routine("p", "sales", "r1") is None
    finally:
        await engine.stop()


@pytest.mark.asyncio
async def test_model_crud_and_on_disk_persistence(persistent_settings: Settings) -> None:
    """Model CRUD writes through to DuckDB and survives an engine reopen.

    Covers create/get/list/update write-through, then a fresh engine on
    the same data directory rehydrating the full :class:`ModelMeta`
    (opaque feature/label column shapes + internal training-query
    provenance, and the persisted update), then delete write-through.
    """
    engine = DuckDBEngine(persistent_settings)
    await engine.start()
    try:
        repo = DuckDBCatalogRepository(engine)
        repo.create_dataset(_ds())
        m = repo.create_model(_model())
        assert repo.get_model("p", "sales", "m1") == m
        assert repo.list_models("p", "sales") == (m,)
        repo.update_model(_model().model_copy(update={"description": "d"}))
        assert repo.get_model("p", "sales", "m1").description == "d"  # type: ignore[union-attr]
    finally:
        await engine.stop()

    reopened = DuckDBEngine(persistent_settings)
    await reopened.start()
    try:
        repo = DuckDBCatalogRepository(reopened)
        got = repo.get_model("p", "sales", "m1")
        # Full ModelMeta (including every metadata_json-backed field and the
        # persisted update) round-trips through the on-disk reopen.
        assert got == _model().model_copy(update={"description": "d"})
        repo.delete_model("p", "sales", "m1")
        assert repo.get_model("p", "sales", "m1") is None
    finally:
        await reopened.stop()


@pytest.mark.asyncio
async def test_jobs_upsert_and_list(ephemeral_settings: Settings) -> None:
    from bqemulator.catalog.models import JobMeta

    engine = DuckDBEngine(ephemeral_settings)
    await engine.start()
    try:
        repo = DuckDBCatalogRepository(engine)
        for i in range(3):
            repo.upsert_job(
                JobMeta(
                    project_id="p",
                    job_id=f"j-{i}",
                    job_type="QUERY",
                    state="DONE",
                    configuration={},
                    creation_time=NOW,
                    etag=f"e-{i}",
                ),
            )
        assert len(repo.list_jobs("p")) == 3
        repo.delete_job("p", "j-0")
        assert repo.get_job("p", "j-0") is None

        with pytest.raises(NotFoundError):
            repo.delete_job("p", "missing")
    finally:
        await engine.stop()


@pytest.mark.asyncio
async def test_delete_dataset_not_found_ok(ephemeral_settings: Settings) -> None:
    engine = DuckDBEngine(ephemeral_settings)
    await engine.start()
    try:
        repo = DuckDBCatalogRepository(engine)
        repo.delete_dataset("p", "missing", not_found_ok=True)
        repo.delete_table("p", "missing", "tbl", not_found_ok=True)
        repo.delete_routine("p", "missing", "r", not_found_ok=True)
        repo.delete_model("p", "missing", "m", not_found_ok=True)
    finally:
        await engine.stop()


@pytest.mark.asyncio
async def test_delete_dataset_cascades_through_persistent_tables(
    ephemeral_settings: Settings,
) -> None:
    """``delete_dataset(delete_contents=True)`` removes every dataset-scoped row.

    Phase 7 (snapshots, materialized_views, mv_dependencies) and
    Phase 8 (row_access_policies) catalog tables must be cleaned up
    when a dataset is dropped, otherwise re-creating the same
    ``(project, dataset, table)`` triple finds a leftover row and a
    REST ``POST`` collides with 409 Conflict. Caught by the
    cross-language E2E row_access seeded-fixture re-run in P2.d
    follow-up #2.
    """
    engine = DuckDBEngine(ephemeral_settings)
    await engine.start()
    try:
        repo = DuckDBCatalogRepository(engine)
        repo.create_dataset(_ds())
        repo.create_table(
            TableMeta(
                project_id="p",
                dataset_id="sales",
                table_id="orders",
                creation_time=NOW,
                last_modified_time=NOW,
                etag="etag-orders",
            ),
        )
        repo.create_row_access_policy(
            RowAccessPolicyMeta(
                project_id="p",
                dataset_id="sales",
                table_id="orders",
                policy_id="eu_only",
                filter_predicate="region = 'EU'",
                grantees=("user:eu@example.com",),
                creation_time=NOW,
                last_modified_time=NOW,
                etag="rap-1",
            ),
        )
        repo.create_snapshot(
            SnapshotMeta(
                snapshot_id="snap-1",
                project_id="p",
                dataset_id="sales",
                table_id="orders",
                snapshot_time=NOW,
                kind="USER",
                duckdb_schema="p__sales",
                duckdb_table="orders_snap",
            ),
        )
        repo.upsert_materialized_view(
            MaterializedViewMeta(
                project_id="p",
                dataset_id="sales",
                table_id="orders_mv",
                view_query="SELECT id FROM `p.sales.orders`",
                base_tables=(("p", "sales", "orders"),),
                last_refresh_time=NOW,
            ),
        )
        repo.create_model(_model())

        # Drop the dataset — every dependent row should go with it.
        repo.delete_dataset("p", "sales", delete_contents=True)

        # Catalog cache should be cleared.
        assert repo.list_row_access_policies("p", "sales", "orders") == ()
        assert repo.list_snapshots_for_table("p", "sales", "orders") == ()
        assert repo.get_materialized_view("p", "sales", "orders_mv") is None
        assert repo.list_models("p", "sales") == ()

        # The persistent DuckDB-side rows must also be cleared so a
        # fresh repo on the same engine doesn't rehydrate stale state.
        rebuilt = DuckDBCatalogRepository(engine)
        assert rebuilt.list_row_access_policies("p", "sales", "orders") == ()
        assert rebuilt.list_snapshots_for_table("p", "sales", "orders") == ()
        assert rebuilt.get_materialized_view("p", "sales", "orders_mv") is None
        assert rebuilt.list_models("p", "sales") == ()
    finally:
        await engine.stop()


def _job(job_id: str = "j1", *, state: str = "DONE", etag: str = "e") -> JobMeta:
    return JobMeta(
        project_id="p",
        job_id=job_id,
        job_type="QUERY",
        state=state,  # type: ignore[arg-type]
        configuration={},
        creation_time=NOW,
        etag=etag,
    )


def _stored_job_rows(engine: DuckDBEngine) -> list[tuple[str, str, str]]:
    return engine.execute(
        'SELECT job_id, state, etag FROM "_bqemulator_catalog"."jobs" ORDER BY job_id',
    ).fetchall()


@pytest.mark.asyncio
async def test_upsert_job_async_is_readable_at_once_and_persisted(
    persistent_settings: Settings,
) -> None:
    engine = DuckDBEngine(persistent_settings)
    await engine.start()
    try:
        repo = DuckDBCatalogRepository(engine)
        task = asyncio.ensure_future(repo.upsert_job_async(_job("j1")))
        # The cache is updated before the lane write is awaited.
        await asyncio.sleep(0)
        assert repo.get_job("p", "j1") == _job("j1")
        assert await task == _job("j1")
        assert _stored_job_rows(engine) == [("j1", "DONE", "e")]
    finally:
        await engine.stop()

    reopened = DuckDBEngine(persistent_settings)
    await reopened.start()
    try:
        assert DuckDBCatalogRepository(reopened).get_job("p", "j1") == _job("j1")
    finally:
        await reopened.stop()


@pytest.mark.asyncio
async def test_upsert_job_async_runs_on_the_job_lane(ephemeral_settings: Settings) -> None:
    engine = DuckDBEngine(ephemeral_settings)
    await engine.start()
    try:
        repo = DuckDBCatalogRepository(engine)
        repo.ensure_ready()
        threads: list[str] = []
        original = engine.run_on_job_lane

        async def spy(fn: Any) -> Any:
            def wrapped(cursor: Any) -> Any:
                threads.append(threading.current_thread().name)
                return fn(cursor)

            return await original(wrapped)

        engine.run_on_job_lane = spy  # type: ignore[method-assign]
        await repo.upsert_job_async(_job("j1"))
        assert threads and all(name.startswith("bqemu-jobs") for name in threads)
    finally:
        await engine.stop()


@pytest.mark.asyncio
async def test_last_async_upsert_of_a_job_wins(persistent_settings: Settings) -> None:
    engine = DuckDBEngine(persistent_settings)
    await engine.start()
    try:
        repo = DuckDBCatalogRepository(engine)
        await asyncio.gather(
            repo.upsert_job_async(_job("j1", state="RUNNING", etag="e1")),
            repo.upsert_job_async(_job("j1", state="DONE", etag="e2")),
        )
        assert _stored_job_rows(engine) == [("j1", "DONE", "e2")]
    finally:
        await engine.stop()


@pytest.mark.asyncio
async def test_upsert_then_delete_leaves_no_row(persistent_settings: Settings) -> None:
    engine = DuckDBEngine(persistent_settings)
    await engine.start()
    try:
        repo = DuckDBCatalogRepository(engine)
        await asyncio.gather(
            repo.upsert_job_async(_job("j1")),
            repo.delete_job_async("p", "j1"),
        )
        assert repo.get_job("p", "j1") is None
        assert _stored_job_rows(engine) == []
    finally:
        await engine.stop()


@pytest.mark.asyncio
async def test_delete_then_reupsert_keeps_the_row(persistent_settings: Settings) -> None:
    engine = DuckDBEngine(persistent_settings)
    await engine.start()
    try:
        repo = DuckDBCatalogRepository(engine)
        await repo.upsert_job_async(_job("j1", etag="old"))
        await asyncio.gather(
            repo.delete_job_async("p", "j1"),
            repo.upsert_job_async(_job("j1", etag="new")),
        )
        assert repo.get_job("p", "j1") == _job("j1", etag="new")
        assert _stored_job_rows(engine) == [("j1", "DONE", "new")]
    finally:
        await engine.stop()


@pytest.mark.asyncio
async def test_delete_job_async_missing_job(ephemeral_settings: Settings) -> None:
    engine = DuckDBEngine(ephemeral_settings)
    await engine.start()
    try:
        repo = DuckDBCatalogRepository(engine)
        with pytest.raises(NotFoundError):
            await repo.delete_job_async("p", "missing")
        await repo.delete_job_async("p", "missing", not_found_ok=True)
    finally:
        await engine.stop()
