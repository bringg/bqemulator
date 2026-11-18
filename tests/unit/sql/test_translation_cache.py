"""``TranslationCache`` is a bounded LRU keyed by every input of the pure steps."""

from __future__ import annotations

import pytest

from bqemulator.row_access.identity import CallerIdentity
from bqemulator.sql.translation_cache import TranslationCache

pytestmark = pytest.mark.unit

_ALICE = CallerIdentity(principal="user:alice@example.com")
_BOB = CallerIdentity(principal="user:bob@example.com")
_SCHEMA = {"t": {"id": "BIGINT"}}


class TestTranslationNamespace:
    def test_miss_then_hit(self) -> None:
        cache = TranslationCache(max_entries=4)
        assert cache.get_translation("SELECT 1", _SCHEMA, _ALICE) is None
        cache.put_translation("SELECT 1", _SCHEMA, _ALICE, "SELECT 1 /* duck */")
        assert cache.get_translation("SELECT 1", _SCHEMA, _ALICE) == "SELECT 1 /* duck */"

    def test_equal_schema_copy_hits(self) -> None:
        cache = TranslationCache(max_entries=4)
        cache.put_translation("SELECT 1", _SCHEMA, _ALICE, "x")
        assert cache.get_translation("SELECT 1", {"t": {"id": "BIGINT"}}, _ALICE) == "x"

    def test_sql_schema_and_caller_are_part_of_the_key(self) -> None:
        cache = TranslationCache(max_entries=8)
        cache.put_translation("SELECT 1", _SCHEMA, _ALICE, "x")
        assert cache.get_translation("SELECT 2", _SCHEMA, _ALICE) is None
        assert cache.get_translation("SELECT 1", {"t": {"id": "VARCHAR"}}, _ALICE) is None
        assert (
            cache.get_translation("SELECT 1", {"t": {"id": "BIGINT", "v": "INT"}}, _ALICE) is None
        )
        assert cache.get_translation("SELECT 1", None, _ALICE) is None
        assert cache.get_translation("SELECT 1", _SCHEMA, _BOB) is None

    def test_empty_schema_and_none_are_the_same_key(self) -> None:
        cache = TranslationCache(max_entries=4)
        cache.put_translation("SELECT 1", None, _ALICE, "x")
        assert cache.get_translation("SELECT 1", {}, _ALICE) == "x"

    def test_namespaces_are_separate(self) -> None:
        cache = TranslationCache(max_entries=4)
        cache.put_translation("SELECT 1", None, _ALICE, "translated")
        assert cache.get_qualified("SELECT 1", "p") is None
        cache.put_qualified("SELECT 1", "p", "qualified")
        assert cache.get_qualified("SELECT 1", "p") == "qualified"
        assert cache.get_qualified("SELECT 1", "other") is None
        assert cache.get_translation("SELECT 1", None, _ALICE) == "translated"


class TestEviction:
    def test_least_recently_used_entry_is_evicted(self) -> None:
        cache = TranslationCache(max_entries=2)
        cache.put_qualified("a", "p", "A")
        cache.put_qualified("b", "p", "B")
        assert cache.get_qualified("a", "p") == "A"  # "a" becomes most recent
        cache.put_qualified("c", "p", "C")
        assert cache.get_qualified("b", "p") is None
        assert cache.get_qualified("a", "p") == "A"
        assert cache.get_qualified("c", "p") == "C"
        assert len(cache) == 2

    def test_put_existing_key_refreshes_value_and_order(self) -> None:
        cache = TranslationCache(max_entries=2)
        cache.put_qualified("a", "p", "A")
        cache.put_qualified("b", "p", "B")
        cache.put_qualified("a", "p", "A2")
        cache.put_qualified("c", "p", "C")
        assert cache.get_qualified("a", "p") == "A2"
        assert cache.get_qualified("b", "p") is None

    def test_size_stays_at_the_bound(self) -> None:
        cache = TranslationCache(max_entries=3)
        for i in range(20):
            cache.put_qualified(f"q{i}", "p", "v")
        assert len(cache) == 3


class TestDisabled:
    def test_zero_entries_stores_nothing(self) -> None:
        cache = TranslationCache(max_entries=0)
        cache.put_translation("SELECT 1", None, _ALICE, "x")
        cache.put_qualified("SELECT 1", "p", "x")
        assert len(cache) == 0
        assert cache.get_translation("SELECT 1", None, _ALICE) is None
        assert cache.get_qualified("SELECT 1", "p") is None


class TestOversizedText:
    def test_very_long_sql_is_not_stored(self) -> None:
        cache = TranslationCache(max_entries=4)
        long_sql = "SELECT '" + "x" * 70_000 + "'"
        cache.put_translation(long_sql, None, _ALICE, "x")
        cache.put_qualified(long_sql, "p", "x")
        assert len(cache) == 0
