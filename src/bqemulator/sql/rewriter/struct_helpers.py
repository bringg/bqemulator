"""Pre-translator rewrites for BigQuery STRUCT literals (ADR 0023 §1.I).

BigQuery's ``STRUCT(value_a, value_b)`` builds a struct by *position*;
the field names are inferred from context (the first row of a UNION
ALL, the target column's STRUCT type, the surrounding alias, …). The
named variant ``STRUCT(value_a AS name_a, value_b AS name_b)`` carries
explicit field names.

SQLGlot transpiles the BigQuery positional form to DuckDB's
``{'_0': value_a, '_1': value_b}`` — a struct *with* fixed field
names that do not match BigQuery's name-inference rules:

* ``UNION ALL`` between named (``{'id': 1, 'label': 'a'}``) and
  positional (``{'_0': 2, '_1': 'b'}``) structs leaves the second row
  with ``id = NULL`` / ``label = NULL`` (and the original ``_0`` /
  ``_1`` fields invisible to the named-projection path), so any
  predicate ``WHERE s.id > 1`` silently filters out the row that
  should match.
* ``INSERT INTO t VALUES (1, {'_0': 'Alice', '_1': 30})`` fails the
  ``STRUCT to STRUCT cast must have at least one matching member``
  binder check when ``t.person`` is typed
  ``STRUCT(name VARCHAR, age INT)``.

DuckDB's ``ROW(value_a, value_b)`` is the equivalent positional
constructor: it produces a struct whose fields are *positionally*
matched to the target's struct type — exactly what BigQuery's
positional STRUCT does. The rewrite walks the BigQuery AST, detects
every ``Struct`` node whose children are all unaliased (no
``PropertyEQ``), and replaces it with ``Anonymous(this='ROW',
expressions=[...])``. SQLGlot passes ``ROW`` through the BQ → DuckDB
transpile unchanged.

Named structs (``STRUCT(value AS field)``) are left alone — their
explicit field names should survive the transpile as DuckDB struct
literals (``{'field': value}``).

An unaliased field that is a column reference or field path is *not*
anonymous in BigQuery: ``STRUCT(rating_id, t.rating)`` names its
fields ``rating_id`` and ``rating`` — the last path component, the
same rule a ``SELECT`` list uses for an implicit alias. Turning such a
struct into ``ROW(...)`` drops those names, so a later
``s.rating`` / ``UNNEST(arr) AS r … r.rating`` fails with
``struct_extract with a string key cannot be used on an unnamed
struct``. When every unaliased child carries an inferable name, the
rewrite therefore makes the names explicit (``STRUCT(rating_id AS
rating_id, t.rating AS rating)``) instead of emitting ``ROW``. Inside
``INSERT`` / ``MERGE`` and set operations BigQuery binds struct fields
by position, so those keep the ``ROW`` form.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp


def rewrite_struct_helpers(bq_sql: str) -> str:
    """Pre-translate BigQuery SQL for positional ``STRUCT`` literals.

    Returns the input unchanged when no rewrite is needed.

    Parse failures are tolerated: we return the original SQL so the
    downstream SQLGlot transpile surfaces its own parse error message.
    """
    if "STRUCT" not in bq_sql.upper():
        return bq_sql

    try:
        parsed = sqlglot.parse_one(bq_sql, read="bigquery")
    except sqlglot.errors.ParseError:
        return bq_sql

    modified = _rewrite_positional_structs(parsed)
    if not modified:
        return bq_sql
    return parsed.sql(dialect="bigquery")


def _rewrite_positional_structs(tree: exp.Expression) -> bool:
    """Replace positional ``STRUCT(value, value, …)`` calls with ``ROW(…)``.

    A struct is positional iff every child is *not* a ``PropertyEQ``
    (the ``AS name`` form). An empty struct (zero children) is treated
    as named — there is nothing positional about it. A struct whose
    unaliased children all have BigQuery-inferable names gets those
    names made explicit instead (see the module docstring).
    """
    modified = False
    for node in list(tree.find_all(exp.Struct)):
        children = list(node.expressions)
        if not children:
            continue
        if _name_inferable_fields(node, children):
            modified = True
            continue
        if any(isinstance(child, exp.PropertyEQ) for child in children):
            continue
        replacement = exp.Anonymous(
            this="ROW",
            expressions=[child.copy() for child in children],
        )
        node.replace(replacement)
        modified = True
    return modified


def _name_inferable_fields(node: exp.Struct, children: list[exp.Expression]) -> bool:
    """Alias every unaliased child of ``node`` with its BigQuery-inferred name.

    Applies only when at least one child is unaliased, every unaliased
    child is a column reference or field path, and ``node`` is outside
    an ``INSERT`` / ``MERGE`` / set operation (which bind structs by
    position). Returns ``True`` when ``node`` was rewritten.
    """
    if node.find_ancestor(exp.Insert, exp.Merge, exp.SetOperation) is not None:
        return False
    names: list[str | None] = []
    for child in children:
        if isinstance(child, exp.PropertyEQ):
            names.append(None)
            continue
        name = _inferred_field_name(child)
        if name is None:
            return False
        names.append(name)
    if all(name is None for name in names):
        return False
    node.set(
        "expressions",
        [
            child
            if name is None
            else exp.PropertyEQ(this=exp.to_identifier(name), expression=child.copy())
            for child, name in zip(children, names, strict=True)
        ],
    )
    return True


def _inferred_field_name(child: exp.Expression) -> str | None:
    """Return the field name BigQuery infers for an unaliased struct field.

    ``col`` → ``col``; ``t.col`` / ``s.a.b`` → the last path component.
    ``None`` for any other expression, which BigQuery leaves anonymous.
    """
    if isinstance(child, exp.Column) and not isinstance(child.this, exp.Star):
        return child.name or None
    if isinstance(child, exp.Dot) and isinstance(child.expression, exp.Identifier):
        return child.expression.name or None
    return None


__all__ = ["rewrite_struct_helpers"]
