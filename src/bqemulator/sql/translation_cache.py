"""Bounded LRU cache for the pure SQL translation steps (ADR 0051).

Two steps of the single-statement query path are pure functions of their
inputs and dominate its CPU time: BigQuery to DuckDB translation
(:meth:`bqemulator.sql.translator.SQLTranslator.translate`) and table
reference qualification
(:func:`bqemulator.sql.table_rewriter.rewrite_table_refs`). Every input of
each step is part of its cache key, so no invalidation is needed:

* translation: ``(BigQuery SQL, catalog schema snapshot, caller)``;
* qualification: ``(translated SQL, project id)``.

Only successful results are stored. The cache is used from the event loop
thread only and is not thread-safe.
"""

from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Hashable

    from bqemulator.row_access.identity import CallerIdentity

# Texts longer than this are not stored: they would pin a lot of memory for a
# statement that rarely repeats.
_MAX_CACHED_CHARS = 65_536


class TranslationCache:
    """LRU cache holding translated and qualified SQL; ``max_entries=0`` disables it."""

    def __init__(self, max_entries: int = 2048) -> None:
        self._max_entries = max_entries
        self._entries: OrderedDict[Hashable, str] = OrderedDict()

    def __len__(self) -> int:
        return len(self._entries)

    def get_translation(
        self,
        bq_sql: str,
        schema: dict[str, Any] | None,
        caller: CallerIdentity | None,
    ) -> str | None:
        """Return the cached DuckDB SQL for these translation inputs, or ``None``."""
        return self._get(("translate", bq_sql, _freeze_schema(schema), caller))

    def put_translation(
        self,
        bq_sql: str,
        schema: dict[str, Any] | None,
        caller: CallerIdentity | None,
        duckdb_sql: str,
    ) -> None:
        """Store a successful translation."""
        if len(bq_sql) <= _MAX_CACHED_CHARS:
            self._put(("translate", bq_sql, _freeze_schema(schema), caller), duckdb_sql)

    def get_qualified(self, translated_sql: str, project_id: str) -> str | None:
        """Return the cached table-qualified SQL, or ``None``."""
        return self._get(("qualify", translated_sql, project_id))

    def put_qualified(self, translated_sql: str, project_id: str, qualified_sql: str) -> None:
        """Store a successful table qualification."""
        if len(translated_sql) <= _MAX_CACHED_CHARS:
            self._put(("qualify", translated_sql, project_id), qualified_sql)

    def _get(self, key: Hashable) -> str | None:
        value = self._entries.get(key)
        if value is not None:
            self._entries.move_to_end(key)
        return value

    def _put(self, key: Hashable, value: str) -> None:
        if self._max_entries <= 0:
            return
        self._entries[key] = value
        self._entries.move_to_end(key)
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)


def _freeze_schema(schema: dict[str, Any] | None) -> Hashable:
    """Return a hashable copy of a ``{table: {column: type}}`` snapshot."""
    if not schema:
        return ()
    return tuple((table, tuple(columns.items())) for table, columns in schema.items())


__all__ = ["TranslationCache"]
