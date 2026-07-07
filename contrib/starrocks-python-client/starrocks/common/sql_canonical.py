# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""AST-based SQL canonicalization for view / materialized-view definition comparison.

Parses a definition with sqlglot, normalizes the (semantically meaningless) syntactic
variation StarRocks introduces when storing a view/MV, and re-renders a canonical string
so equivalent definitions compare equal. Operating on the AST rather than raw text avoids
the regex normalizer's failure modes: it never rewrites bytes inside a string literal and
never drops precedence-significant parentheses.

sqlglot is optional; ``canonicalize_sql`` returns ``None`` when it is absent or a
definition fails to parse, and the caller falls back to plain string normalization.
"""

import logging
from typing import Optional


logger = logging.getLogger(__name__)

try:
    import sqlglot
    from sqlglot import exp
    from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
    from sqlglot.optimizer.simplify import simplify

    _SQLGLOT_AVAILABLE = True
except ImportError:
    _SQLGLOT_AVAILABLE = False

_DIALECT = "starrocks"

# Sides for which a trailing "OUTER" is redundant: "<side> OUTER JOIN" == "<side> JOIN".
_REDUNDANT_JOIN_SIDES = ("LEFT", "RIGHT", "FULL")


def is_available() -> bool:
    """Return True if sqlglot is installed and AST canonicalization can be attempted."""
    return _SQLGLOT_AVAILABLE


def _canonicalize_node(node: "exp.Expression", remove_qualifiers: bool) -> "exp.Expression":
    """Per-node normalization applied bottom-up by ``Expression.transform``. Every rewrite
    is semantics-preserving, so it can only erase syntactic noise, never a real change."""
    # Unquote + case-fold identifiers (string literals are exp.Literal, so left untouched).
    if isinstance(node, exp.Identifier):
        name = node.this
        if isinstance(name, str):
            node.set("this", name.lower())
        node.set("quoted", False)
        return node

    # Strip catalog/db/table qualifiers ("db.t.col" -> "col").
    if remove_qualifiers and isinstance(node, exp.Column):
        for key in ("table", "db", "catalog"):
            if node.args.get(key) is not None:
                node.set(key, None)
        return node
    if remove_qualifiers and isinstance(node, exp.Table):
        for key in ("db", "catalog"):
            if node.args.get(key) is not None:
                node.set(key, None)
        return node

    # "INNER JOIN" -> "JOIN"; "<dir> OUTER JOIN" -> "<dir> JOIN".
    if isinstance(node, exp.Join):
        kind = (node.args.get("kind") or "").upper()
        side = (node.args.get("side") or "").upper()
        if kind == "INNER" and not side:
            node.set("kind", None)
        elif kind == "OUTER" and side in _REDUNDANT_JOIN_SIDES:
            node.set("kind", None)
        return node

    # Unwrap "LATERAL unnest(...)" (== comma join to unnest); skip LATERAL VIEW / OUTER.
    if isinstance(node, exp.Lateral) and not node.args.get("view") and not node.args.get("outer"):
        inner = node.this
        if isinstance(inner, exp.Unnest):
            alias = node.args.get("alias")
            if alias is not None and inner.args.get("alias") is None:
                inner.set("alias", alias)
            return inner

    return node


def canonicalize_sql(sql: Optional[str], remove_qualifiers: bool = False) -> Optional[str]:
    """Return a canonical rendering of ``sql`` for definition comparison, or None when
    sqlglot is unavailable, the input is empty, or the statement cannot be parsed.

    When True, ``remove_qualifiers`` drops catalog/db/table qualifiers from references.
    """
    if not _SQLGLOT_AVAILABLE or not sql or not sql.strip():
        return None
    try:
        tree = sqlglot.parse_one(sql, dialect=_DIALECT)
        if tree is None:
            return None
        tree = normalize_identifiers(tree, dialect=_DIALECT)
        tree = tree.transform(lambda node: _canonicalize_node(node, remove_qualifiers))
        tree = simplify(tree)  # precedence-aware, so load-bearing parens survive
        return tree.sql(dialect=_DIALECT, comments=False, normalize=True)
    except Exception as exc:
        # Best-effort: any parse/transform failure falls back to plain normalization.
        logger.debug("sqlglot canonicalization failed (%s); falling back to plain normalization.", exc)
        return None
