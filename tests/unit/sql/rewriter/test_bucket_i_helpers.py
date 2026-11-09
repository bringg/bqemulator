"""Tests for the Bucket I pre-translator rewriters (JSON, SAFE, STRUCT).

These rewriters run before SQLGlot's BQ → DuckDB transpile so they can
preserve information the transpile would otherwise drop:

* :mod:`bqemulator.sql.rewriter.json_helpers` — wraps ``TO_JSON`` in
  ``CAST(... AS JSON)`` so the wire column lands on the JSON type
  (SQLGlot collapses both ``TO_JSON`` and ``TO_JSON_STRING`` to the
  same ``CAST(TO_JSON(...) AS TEXT)`` form).
* :mod:`bqemulator.sql.rewriter.safe_helpers` — unwraps ``SAFE.X``
  prefix calls into ``TRY(X)`` so they survive the table-rewriter's
  project-qualification pass.
* :mod:`bqemulator.sql.rewriter.struct_helpers` — replaces positional
  ``STRUCT(value, …)`` calls with ``ROW(…)`` so the struct aligns
  positionally with its target (matching BigQuery's name-from-context
  inference for INSERT VALUES and UNION ALL chains).
"""

from __future__ import annotations

import duckdb
import pytest

from bqemulator.domain.result import Ok
from bqemulator.sql.rewriter.json_helpers import rewrite_json_helpers
from bqemulator.sql.rewriter.safe_helpers import rewrite_safe_helpers
from bqemulator.sql.rewriter.struct_helpers import rewrite_struct_helpers
from bqemulator.sql.translator import SQLTranslator

pytestmark = pytest.mark.unit


@pytest.fixture
def t() -> SQLTranslator:
    return SQLTranslator()


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect()


def _execute(t: SQLTranslator, con: duckdb.DuckDBPyConnection, sql: str) -> object:
    """Translate *sql* and return the first row."""
    result = t.translate(sql)
    assert isinstance(result, Ok), result
    return con.execute(result.value).fetchone()


def _column_type(t: SQLTranslator, con: duckdb.DuckDBPyConnection, sql: str) -> str:
    """Return the DuckDB column type for the first column of *sql*."""
    result = t.translate(sql)
    assert isinstance(result, Ok), result
    desc = con.execute(result.value).description
    return str(desc[0][1]).upper()


class TestJsonHelpersNoOp:
    """``rewrite_json_helpers`` is a no-op when no TO_JSON appears."""

    def test_returns_input_unchanged(self) -> None:
        sql = "SELECT 1, 'x'"
        assert rewrite_json_helpers(sql) is sql


class TestToJson:
    """``TO_JSON(x)`` produces a JSON-typed column."""

    def test_array_returns_json(self, t: SQLTranslator, con: duckdb.DuckDBPyConnection) -> None:
        col_type = _column_type(t, con, "SELECT TO_JSON([1, 2, 3]) AS j")
        assert col_type == "JSON"

    def test_struct_returns_json(self, t: SQLTranslator, con: duckdb.DuckDBPyConnection) -> None:
        col_type = _column_type(t, con, "SELECT TO_JSON(STRUCT(1 AS a, 'b' AS b)) AS j")
        assert col_type == "JSON"

    def test_to_json_string_unchanged(
        self, t: SQLTranslator, con: duckdb.DuckDBPyConnection
    ) -> None:
        # TO_JSON_STRING returns STRING; the JSON pre-translate must
        # leave the ``to_json=False`` variant alone.
        col_type = _column_type(t, con, "SELECT TO_JSON_STRING([1, 2, 3]) AS j")
        assert "VARCHAR" in col_type or "TEXT" in col_type


class TestSafeHelpersNoOp:
    """``rewrite_safe_helpers`` is a no-op when no ``SAFE.`` prefix appears."""

    def test_returns_input_unchanged(self) -> None:
        sql = "SELECT SAFE_DIVIDE(1, 2)"
        # ``SAFE_DIVIDE`` doesn't trigger the prefix-form path.
        assert rewrite_safe_helpers(sql) is sql


class TestSafeFunctionPrefix:
    """``SAFE.X(args)`` → ``TRY(X(args))`` propagates NULL on error."""

    def test_ln_negative_returns_null(
        self, t: SQLTranslator, con: duckdb.DuckDBPyConnection
    ) -> None:
        row = _execute(t, con, "SELECT SAFE.LN(-1) AS x")
        assert row == (None,)

    def test_sqrt_negative_returns_null(
        self, t: SQLTranslator, con: duckdb.DuckDBPyConnection
    ) -> None:
        row = _execute(t, con, "SELECT SAFE.SQRT(-1) AS x")
        assert row == (None,)

    def test_safe_ln_positive_works(self, t: SQLTranslator, con: duckdb.DuckDBPyConnection) -> None:
        row = _execute(t, con, "SELECT SAFE.LN(1) AS x")
        assert row == (0.0,)


class TestStructHelpersNoOp:
    """``rewrite_struct_helpers`` is a no-op when no ``STRUCT`` appears."""

    def test_no_struct_unchanged(self) -> None:
        sql = "SELECT 1, 'x'"
        assert rewrite_struct_helpers(sql) is sql

    def test_named_struct_unchanged(self) -> None:
        # All-aliased struct stays as STRUCT(...).
        sql = "SELECT STRUCT(1 AS a, 2 AS b) AS s"
        # The rewriter returns the same string (no modification).
        assert rewrite_struct_helpers(sql) is sql


class TestPositionalStruct:
    """Positional ``STRUCT(...)`` rewrites to ``ROW(...)`` for INSERT/UNION."""

    def test_union_with_named_first(self, t: SQLTranslator, con: duckdb.DuckDBPyConnection) -> None:
        sql = (
            "WITH t AS (SELECT STRUCT(1 AS id, 'a' AS label) AS s "
            "UNION ALL SELECT STRUCT(2, 'b')) "
            "SELECT s.id, s.label FROM t WHERE s.id > 1"
        )
        rel = con.sql(t.translate(sql).value)  # type: ignore[union-attr]
        assert rel.fetchall() == [(2, "b")]

    def test_insert_positional(self, t: SQLTranslator, con: duckdb.DuckDBPyConnection) -> None:
        con.execute("CREATE TABLE struct_t (id INT64, person STRUCT(name VARCHAR, age INT64))")
        result = t.translate("INSERT INTO struct_t VALUES (1, STRUCT('Alice', 30))")
        assert isinstance(result, Ok)
        con.execute(result.value)
        assert con.sql("SELECT * FROM struct_t").fetchall() == [
            (1, {"name": "Alice", "age": 30}),
        ]


class TestInferredStructFieldNames:
    """Unaliased column fields keep their BigQuery-inferred names."""

    def test_column_fields_named_after_columns(self) -> None:
        out = rewrite_struct_helpers("SELECT STRUCT(rating_id, t.rating) AS s FROM t")
        assert "rating_id AS rating_id" in out
        assert "t.rating AS rating" in out
        assert "ROW" not in out

    def test_mixed_alias_and_column(self) -> None:
        out = rewrite_struct_helpers("SELECT STRUCT(a, b + 1 AS c) AS s FROM t")
        assert "a AS a" in out
        assert "b + 1 AS c" in out

    def test_expression_field_stays_positional(self) -> None:
        # ``a + 1`` has no inferable name, so the struct keeps the ROW path.
        out = rewrite_struct_helpers("SELECT STRUCT(a, a + 1) AS s FROM t")
        assert "ROW(" in out

    def test_insert_select_stays_positional(self) -> None:
        out = rewrite_struct_helpers("INSERT INTO dst SELECT 1, STRUCT(name, age) FROM src")
        assert "ROW(" in out

    def test_set_operation_stays_positional(self) -> None:
        out = rewrite_struct_helpers(
            "SELECT STRUCT(1 AS id) AS s UNION ALL SELECT STRUCT(x) FROM t",
        )
        assert "ROW(" in out

    def test_unnest_alias_field_access(
        self,
        t: SQLTranslator,
        con: duckdb.DuckDBPyConnection,
    ) -> None:
        con.execute(
            "CREATE TABLE ratings_base AS "
            "SELECT 1 AS task_id, 5 AS rating, ['fast'] AS reasons "
            "UNION ALL SELECT 1, 3, ['slow', 'fast']",
        )
        agg = t.translate(
            "CREATE VIEW order_ratings AS SELECT task_id, "
            "ARRAY_AGG(STRUCT(rating, reasons)) AS ratings "
            "FROM ratings_base GROUP BY task_id",
        )
        assert isinstance(agg, Ok)
        con.execute(agg.value)
        row = _execute(
            t,
            con,
            "SELECT (SELECT AVG(r.rating) FROM UNNEST(ratings) AS r), "
            "ARRAY(SELECT DISTINCT reason FROM UNNEST(ratings) AS r, "
            "UNNEST(r.reasons) AS reason ORDER BY reason) FROM order_ratings",
        )
        assert row == (4.0, ["fast", "slow"])
