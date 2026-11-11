"""Tests for the DuckDBEngine lifecycle and concurrency primitives."""

from __future__ import annotations

import asyncio
from pathlib import Path
import threading
import time

import pytest

from bqemulator.config import PersistenceMode, Settings
from bqemulator.domain.errors import InternalError
from bqemulator.storage.engine import CATALOG_SCHEMA, DuckDBEngine

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
class TestLifecycle:
    async def test_start_opens_connection(self, ephemeral_settings: Settings) -> None:
        engine = DuckDBEngine(ephemeral_settings)
        await engine.start()
        try:
            assert engine.connection is not None
        finally:
            await engine.stop()

    async def test_stop_is_idempotent(self, ephemeral_settings: Settings) -> None:
        engine = DuckDBEngine(ephemeral_settings)
        await engine.start()
        await engine.stop()
        await engine.stop()  # no raise

    async def test_start_is_idempotent(self, ephemeral_settings: Settings) -> None:
        engine = DuckDBEngine(ephemeral_settings)
        await engine.start()
        await engine.start()  # no raise
        await engine.stop()

    async def test_connection_raises_before_start(self, ephemeral_settings: Settings) -> None:
        engine = DuckDBEngine(ephemeral_settings)
        with pytest.raises(InternalError):
            _ = engine.connection


@pytest.mark.asyncio
class TestExecute:
    async def test_select_one(self, ephemeral_settings: Settings) -> None:
        engine = DuckDBEngine(ephemeral_settings)
        await engine.start()
        try:
            result = engine.execute("SELECT 1 AS one").fetchone()
            assert result == (1,)
        finally:
            await engine.stop()

    async def test_fetch_arrow_returns_arrow_table(
        self,
        ephemeral_settings: Settings,
    ) -> None:
        import pyarrow as pa

        engine = DuckDBEngine(ephemeral_settings)
        await engine.start()
        try:
            table = engine.fetch_arrow("SELECT 1 AS a, 'hi' AS b")
            assert isinstance(table, pa.Table)
            assert table.column_names == ["a", "b"]
            assert table.num_rows == 1
        finally:
            await engine.stop()

    async def test_has_view(self, ephemeral_settings: Settings) -> None:
        engine = DuckDBEngine(ephemeral_settings)
        await engine.start()
        try:
            engine.execute('CREATE SCHEMA "p__ds"')
            engine.execute('CREATE TABLE "p__ds"."t" (x INTEGER)')
            engine.execute('CREATE VIEW "p__ds"."v" AS SELECT * FROM "p__ds"."t"')
            assert engine.has_view("p__ds", "v")
            assert not engine.has_view("p__ds", "t")
            assert not engine.has_view("p__ds", "missing")
        finally:
            await engine.stop()


@pytest.mark.asyncio
class TestFetchArrowOffLoop:
    async def test_matches_fetch_arrow(self, ephemeral_settings: Settings) -> None:
        engine = DuckDBEngine(ephemeral_settings)
        await engine.start()
        try:
            sql = "SELECT ? AS a, '{\"k\": 1}'::JSON AS j"
            off_loop = await engine.fetch_arrow_off_loop(sql, [7])
            assert off_loop.equals(engine.fetch_arrow(sql, [7]))
            assert off_loop.schema.field("j").metadata == {b"bqemu.duckdb_type": b"JSON"}
        finally:
            await engine.stop()

    async def test_sees_state_of_the_main_connection(
        self,
        ephemeral_settings: Settings,
    ) -> None:
        engine = DuckDBEngine(ephemeral_settings)
        await engine.start()
        try:
            engine.execute('CREATE TABLE "_bqemulator_catalog"."off_loop" AS SELECT 1 AS x')
            engine.execute('CREATE MACRO "_bqemulator_catalog"."plus_one"(v) AS (v + 1)')
            table = await engine.fetch_arrow_off_loop(
                "SELECT current_setting('TimeZone') AS tz,"
                ' "_bqemulator_catalog"."plus_one"(x) AS y,'
                " bqemu_normalize('a', 'NFC') AS n"
                ' FROM "_bqemulator_catalog"."off_loop"',
            )
            assert table.to_pylist() == [{"tz": "UTC", "y": 2, "n": "a"}]
        finally:
            await engine.stop()

    async def test_event_loop_runs_while_statement_executes(
        self,
        ephemeral_settings: Settings,
    ) -> None:
        engine = DuckDBEngine(ephemeral_settings)
        await engine.start()
        try:
            events: list[str] = []

            async def query() -> None:
                await engine.fetch_arrow_off_loop("SELECT count(*) FROM range(10000000)")
                events.append("query")

            async def other_request() -> None:
                await asyncio.sleep(0)
                engine.execute("SELECT 1").fetchone()
                events.append("other")

            await asyncio.gather(query(), other_request())
            assert events == ["other", "query"]
        finally:
            await engine.stop()

    async def test_raises_before_start(self, ephemeral_settings: Settings) -> None:
        engine = DuckDBEngine(ephemeral_settings)
        with pytest.raises(InternalError):
            await engine.fetch_arrow_off_loop("SELECT 1")


class _Probe:
    """Python UDF body that records how many calls overlap in time."""

    def __init__(self, hold_seconds: float) -> None:
        self.hold_seconds = hold_seconds
        self.running = 0
        self.max_running = 0
        self._lock = threading.Lock()

    def __call__(self, value: int) -> int:
        with self._lock:
            self.running += 1
            self.max_running = max(self.max_running, self.running)
        time.sleep(self.hold_seconds)
        with self._lock:
            self.running -= 1
        return value


def _install_probe(engine: DuckDBEngine, hold_seconds: float) -> _Probe:
    probe = _Probe(hold_seconds)
    engine.connection.create_function(
        "bqemu_probe",
        probe,
        [engine.connection.type("BIGINT")],
        engine.connection.type("BIGINT"),
    )
    return probe


# DuckDB 2.x warns on the Arrow conversion, and the repo filter makes warnings errors.
@pytest.mark.filterwarnings("ignore:Arrow output on a DB-API result")
@pytest.mark.asyncio
class TestReadOnlyParallelism:
    async def test_read_only_selects_overlap(self, ephemeral_settings: Settings) -> None:
        engine = DuckDBEngine(ephemeral_settings.model_copy(update={"query_workers": 4}))
        await engine.start()
        try:
            probe = _install_probe(engine, hold_seconds=0.4)
            started = time.perf_counter()
            tables = await asyncio.gather(
                *(
                    engine.fetch_arrow_off_loop(f"SELECT bqemu_probe({i}) AS v", read_only=True)
                    for i in range(4)
                ),
            )
            elapsed = time.perf_counter() - started
            assert sorted(t.column("v")[0].as_py() for t in tables) == [0, 1, 2, 3]
            assert probe.max_running == 4
            assert elapsed < 4 * 0.4 * 0.75
        finally:
            await engine.stop()

    async def test_read_only_false_stays_serial(self, ephemeral_settings: Settings) -> None:
        engine = DuckDBEngine(ephemeral_settings.model_copy(update={"query_workers": 4}))
        await engine.start()
        try:
            probe = _install_probe(engine, hold_seconds=0.1)
            await asyncio.gather(
                *(engine.fetch_arrow_off_loop(f"SELECT bqemu_probe({i}) AS v") for i in range(4)),
            )
            assert probe.max_running == 1
        finally:
            await engine.stop()

    async def test_default_is_four_workers(self) -> None:
        assert Settings.model_fields["query_workers"].default == 4

    async def test_one_worker_has_no_read_pool(self, ephemeral_settings: Settings) -> None:
        engine = DuckDBEngine(ephemeral_settings.model_copy(update={"query_workers": 1}))
        await engine.start()
        try:
            assert engine._read_executor is None
            assert engine._read_cursors is None
            probe = _install_probe(engine, hold_seconds=0.1)
            await asyncio.gather(
                *(
                    engine.fetch_arrow_off_loop(f"SELECT bqemu_probe({i}) AS v", read_only=True)
                    for i in range(4)
                ),
            )
            assert probe.max_running == 1
        finally:
            await engine.stop()

    async def test_read_cursors_see_main_connection_state(
        self,
        ephemeral_settings: Settings,
    ) -> None:
        engine = DuckDBEngine(ephemeral_settings.model_copy(update={"query_workers": 2}))
        await engine.start()
        try:
            engine.execute('CREATE TABLE "_bqemulator_catalog"."ro" AS SELECT 1 AS x')
            table = await engine.fetch_arrow_off_loop(
                "SELECT current_setting('TimeZone') AS tz, bqemu_normalize('a', 'NFC') AS n,"
                ' x FROM "_bqemulator_catalog"."ro"',
                read_only=True,
            )
            assert table.to_pylist() == [{"tz": "UTC", "n": "a", "x": 1}]
            engine.execute('INSERT INTO "_bqemulator_catalog"."ro" VALUES (2)')
            count = await engine.fetch_arrow_off_loop(
                'SELECT count(*) AS c FROM "_bqemulator_catalog"."ro"',
                read_only=True,
            )
            assert count.to_pylist() == [{"c": 2}]
        finally:
            await engine.stop()

    async def test_cursor_returned_to_pool_after_error(
        self,
        ephemeral_settings: Settings,
    ) -> None:
        engine = DuckDBEngine(ephemeral_settings.model_copy(update={"query_workers": 2}))
        await engine.start()
        try:
            for _ in range(5):
                with pytest.raises(Exception, match="Catalog Error"):
                    await engine.fetch_arrow_off_loop("SELECT * FROM nope", read_only=True)
            table = await engine.fetch_arrow_off_loop("SELECT 1 AS a", read_only=True)
            assert table.to_pylist() == [{"a": 1}]
        finally:
            await engine.stop()

    async def test_stop_closes_the_read_pool(self, ephemeral_settings: Settings) -> None:
        engine = DuckDBEngine(ephemeral_settings.model_copy(update={"query_workers": 3}))
        await engine.start()
        cursors = list(engine._read_cursor_list)
        assert len(cursors) == 3
        await engine.stop()
        assert engine._read_executor is None
        assert engine._read_cursors is None
        assert engine._read_cursor_list == []
        for cursor in cursors:
            with pytest.raises(Exception, match="closed"):
                cursor.execute("SELECT 1")
        await engine.stop()


@pytest.mark.asyncio
class TestSessionPragmas:
    async def test_timezone_is_utc(self, ephemeral_settings: Settings) -> None:
        engine = DuckDBEngine(ephemeral_settings)
        await engine.start()
        try:
            tz = engine.execute("SELECT current_setting('TimeZone')").fetchone()
            assert tz is not None
            assert tz[0] == "UTC"
        finally:
            await engine.stop()


@pytest.mark.asyncio
class TestCatalogSchemaSetup:
    async def test_catalog_schema_is_created(self, ephemeral_settings: Settings) -> None:
        engine = DuckDBEngine(ephemeral_settings)
        await engine.start()
        try:
            rows = engine.execute(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = ?",
                [CATALOG_SCHEMA],
            ).fetchall()
            assert len(rows) == 1
        finally:
            await engine.stop()


@pytest.mark.asyncio
class TestPersistence:
    async def test_persistent_writes_file(self, tmp_path: Path) -> None:
        settings = Settings(
            persistence_mode=PersistenceMode.PERSISTENT,
            data_dir=tmp_path,
        )
        engine = DuckDBEngine(settings)
        await engine.start()
        try:
            engine.execute(
                'CREATE TABLE "_bqemulator_catalog"."probe" (x INTEGER)',
            )
            engine.execute(
                'INSERT INTO "_bqemulator_catalog"."probe" VALUES (1)',
            )
        finally:
            await engine.stop()

        # Re-open and verify the data survived.
        engine2 = DuckDBEngine(settings)
        await engine2.start()
        try:
            rows = engine2.execute(
                'SELECT x FROM "_bqemulator_catalog"."probe"',
            ).fetchall()
            assert rows == [(1,)]
        finally:
            await engine2.stop()


@pytest.mark.asyncio
class TestWriteLock:
    async def test_write_lock_is_async_context_manager(
        self,
        ephemeral_settings: Settings,
    ) -> None:
        engine = DuckDBEngine(ephemeral_settings)
        await engine.start()
        try:
            async with engine.write_lock():
                engine.execute(
                    'CREATE TABLE "_bqemulator_catalog"."write_probe" (x INTEGER)',
                )
        finally:
            await engine.stop()


@pytest.mark.asyncio
class TestSpatialExtension:
    """Phase 9 requires the spatial extension at startup."""

    async def test_spatial_loaded_at_startup(
        self,
        ephemeral_settings: Settings,
    ) -> None:
        engine = DuckDBEngine(ephemeral_settings)
        await engine.start()
        try:
            # ``ST_Point`` is provided by the spatial extension; a
            # successful query proves the extension is loaded.
            row = engine.execute("SELECT ST_AsText(ST_Point(1, 2))").fetchone()
            assert row is not None
            assert "POINT" in row[0]
        finally:
            await engine.stop()

    async def test_fail_fast_on_spatial_unavailable(
        self,
        ephemeral_settings: Settings,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from bqemulator.domain.errors import InternalError
        from bqemulator.storage import engine as engine_mod

        # Force the spatial install to fail and verify we raise an
        # InternalError with a clear message — not a confusing query-
        # time catalog error later on.
        engine = engine_mod.DuckDBEngine(ephemeral_settings)

        def _broken_load() -> None:
            raise InternalError("synthetic spatial-load failure")

        monkeypatch.setattr(engine, "_load_spatial", _broken_load)
        with pytest.raises(InternalError, match="synthetic spatial-load failure"):
            await engine.start()
