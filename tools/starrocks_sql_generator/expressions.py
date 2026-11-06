# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Expression generation aligned with StarRocks.g4 expression rules."""

from __future__ import annotations

from typing import Callable

from .context import GenContext
from .schema import Column, Table


def join_sql(parts: list[str]) -> str:
    sql = " ".join(p for p in parts if p)
    sql = sql.replace(" ,", ",")
    sql = sql.replace("( ", "(")
    sql = sql.replace(" )", ")")
    return sql.strip()


def literal_for_column(col: Column, ctx: GenContext) -> str:
    if col.is_boolean:
        return ctx.rng.choice(["TRUE", "FALSE"])
    if col.is_numeric:
        return str(ctx.rng.randint(1, 100))
    if col.is_datetime:
        return ctx.rng.choice(["'2024-01-01'", "'2024-01-01 10:00:00'"])
    if col.is_json:
        return "'{\"k\":1}'"
    if col.is_array:
        return "[]"
    if col.is_map:
        return "map{}"
    return "'sample'"


def column_ref(table: Table, col: Column, alias: str | None = None) -> str:
    if alias:
        return f"{alias}.{col.name}"
    return col.name


def gen_column_ref(ctx: GenContext, table: Table | None = None, alias: str | None = None) -> str:
    tbl, col = ctx.schema.pick_column(ctx.rng, table)
    return column_ref(tbl, col, alias)


def gen_literal(ctx: GenContext, col: Column | None = None) -> str:
    if col is None:
        _, col = ctx.schema.pick_column(ctx.rng)
    return literal_for_column(col, ctx)


def gen_primary(ctx: GenContext, table: Table | None = None, alias: str | None = None) -> str:
    tbl, col = ctx.schema.pick_column(ctx.rng, table)
    choices: list[Callable[[], str]] = [
        lambda: column_ref(tbl, col, alias),
        lambda: gen_literal(ctx, col),
        lambda: f"({gen_value_expr(ctx, tbl, alias)})",
        lambda: f"CAST({gen_column_ref(ctx, tbl, alias)} AS {col.type})",
        lambda: (
            f"CASE WHEN {gen_column_ref(ctx, tbl, alias)} IS NULL "
            f"THEN {gen_literal(ctx, col)} ELSE {gen_column_ref(ctx, tbl, alias)} END"
        ),
    ]
    if ctx.can_recurse("subquery"):
        choices.append(lambda: gen_scalar_subquery(ctx))
    if col.is_string:
        choices.append(lambda: f"TRIM(BOTH ' ' FROM {gen_column_ref(ctx, tbl, alias)})")
    return ctx.rng.choice(choices)()


def gen_scalar_subquery(ctx: GenContext) -> str:
    from .query import gen_query_no_with

    with ctx.recurse("subquery") as ok:
        if not ok:
            return "1"
        inner = gen_query_no_with(ctx, minimal=True)
        return f"({inner})"


def gen_value_expr(ctx: GenContext, table: Table | None = None, alias: str | None = None) -> str:
    left = gen_primary(ctx, table, alias)
    if not ctx.can_recurse("expression"):
        return left
    with ctx.recurse("expression") as ok:
        if not ok:
            return left
        _, col = ctx.schema.pick_column(ctx.rng, table)
        if col.is_numeric:
            op = ctx.rng.choice(["+", "-", "*", "/", "%"])
            right = gen_literal(ctx, col)
            return f"{left} {op} {right}"
        return left


def gen_predicate(ctx: GenContext, table: Table | None = None, alias: str | None = None) -> str:
    tbl, col = ctx.schema.pick_column(ctx.rng, table)
    ref = column_ref(tbl, col, alias)
    lit = gen_literal(ctx, col)
    variants = [
        f"{ref} = {lit}",
        f"{ref} <> {lit}",
        f"{ref} > {lit}",
        f"{ref} >= {lit}",
        f"{ref} < {lit}",
        f"{ref} <= {lit}",
        f"{ref} IS NULL",
        f"{ref} IS NOT NULL",
        f"{ref} BETWEEN {lit} AND {gen_literal(ctx, col)}",
        f"{ref} IN ({lit}, {gen_literal(ctx, col)})",
        f"{ref} NOT IN ({lit}, {gen_literal(ctx, col)})",
    ]
    if col.is_string:
        variants.extend([
            f"{ref} LIKE '%sample%'",
            f"{ref} RLIKE 'sample.*'",
            f"{ref} REGEXP 'sample.*'",
        ])
    if ctx.can_recurse("subquery"):
        variants.append(f"{ref} IN ({gen_scalar_subquery(ctx)})")
        variants.append(f"EXISTS ({gen_scalar_subquery(ctx)})")
    return ctx.rng.choice(variants)


def gen_boolean_expr(ctx: GenContext, table: Table | None = None, alias: str | None = None) -> str:
    pred = gen_predicate(ctx, table, alias)
    if not ctx.can_recurse("expression"):
        return pred
    with ctx.recurse("expression") as ok:
        if not ok:
            return pred
        if ctx.rng.random() < 0.5:
            return f"NOT ({pred})"
        op = ctx.rng.choice(["AND", "OR"])
        right = gen_predicate(ctx, table, alias)
        return f"({pred}) {op} ({right})"


def gen_expression(ctx: GenContext, table: Table | None = None, alias: str | None = None) -> str:
    return gen_boolean_expr(ctx, table, alias)


def gen_sort_item(ctx: GenContext, table: Table | None = None, alias: str | None = None) -> str:
    expr = gen_expression(ctx, table, alias)
    order = ctx.rng.choice(["ASC", "DESC"])
    nulls = ctx.rng.choice(["", " NULLS FIRST", " NULLS LAST"])
    return f"{expr} {order}{nulls}"


def gen_agg_expr(ctx: GenContext, table: Table | None = None, alias: str | None = None) -> str:
    _, col = ctx.schema.pick_column(ctx.rng, table)
    ref = gen_column_ref(ctx, table, alias)
    fn = ctx.rng.choice(["COUNT", "SUM", "AVG", "MIN", "MAX"])
    if fn == "COUNT" and ctx.rng.random() < 0.3:
        return "COUNT(*)"
    return f"{fn}({ref})"


def gen_window_expr(ctx: GenContext, table: Table | None = None, alias: str | None = None) -> str:
    fn = ctx.rng.choice(["ROW_NUMBER", "RANK", "DENSE_RANK", "SUM", "AVG"])
    ref = gen_column_ref(ctx, table, alias)
    partition = gen_column_ref(ctx, table, alias)
    order = gen_sort_item(ctx, table, alias)
    if fn in ("ROW_NUMBER", "RANK", "DENSE_RANK"):
        body = f"{fn}() OVER (PARTITION BY {partition} ORDER BY {order})"
    else:
        body = f"{fn}({ref}) OVER (PARTITION BY {partition} ORDER BY {order})"
    return body
