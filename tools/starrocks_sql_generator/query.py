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

"""Query statement generation aligned with StarRocks.g4 query rules."""

from __future__ import annotations

from dataclasses import dataclass

from .context import GenContext
from .expressions import (
    gen_agg_expr,
    gen_boolean_expr,
    gen_column_ref,
    gen_expression,
    gen_sort_item,
    gen_window_expr,
    join_sql,
    literal_for_column,
)
from .schema import Table


@dataclass(frozen=True)
class QueryPath:
    tag: str
    with_cte: bool = False
    recursive_cte: bool = False
    set_quantifier: str | None = None
    select_star: bool = False
    select_exclude: bool = False
    from_dual: bool = False
    comma_join: bool = False
    join_type: str | None = None
    lateral: bool = False
    asof_join: bool = False
    subquery_from: bool = False
    values_from: bool = False
    where: bool = False
    group_by: str | None = None
    having: bool = False
    qualify: bool = False
    order_by: bool = False
    limit_style: str | None = None
    compound_op: str | None = None
    explain: str | None = None
    window_fn: bool = False
    pivot: bool = False


JOIN_TYPES = [
    "JOIN",
    "INNER JOIN",
    "LEFT JOIN",
    "RIGHT JOIN",
    "FULL JOIN",
    "LEFT OUTER JOIN",
    "CROSS JOIN",
    "LEFT SEMI JOIN",
    "LEFT ANTI JOIN",
    "NULL AWARE LEFT ANTI JOIN",
]

GROUP_BY_VARIANTS = [
    "single",
    "rollup",
    "cube",
    "grouping_sets",
    "all",
]


def _table_alias(table: Table, idx: int) -> str:
    return f"t{idx}"


def gen_select_items(ctx: GenContext, path: QueryPath, table: Table, alias: str) -> str:
    if path.from_dual:
        return ", ".join([f"{ctx.rng.randint(1, 100)} AS n{i}" for i in range(1, 3)])
    if path.select_star:
        if path.select_exclude and len(table.columns) >= 2:
            excluded = ", ".join(c.name for c in table.columns[:2])
            return f"{alias}.* EXCEPT ({excluded})"
        return "*"
    items = []
    count = min(ctx.max_list_items, max(1, len(table.columns)))
    cols = ctx.rng.sample(table.columns, min(count, len(table.columns)))
    for col in cols:
        ref = f"{alias}.{col.name}" if alias else col.name
        if path.window_fn and ctx.rng.random() < 0.5:
            items.append(f"{gen_window_expr(ctx, table, alias)} AS w_{col.name}")
        else:
            items.append(f"{ref} AS {col.name}")
    if path.qualify and items:
        items[0] = f"{gen_window_expr(ctx, table, alias)} AS rn"
    return ", ".join(items)


def gen_from_clause(ctx: GenContext, path: QueryPath) -> tuple[str, Table, str]:
    if path.from_dual:
        return "FROM DUAL", ctx.schema.pick_table(ctx.rng), ""

    if path.values_from:
        tbl = ctx.schema.pick_table(ctx.rng)
        cols = tbl.columns[: min(2, len(tbl.columns))]
        values = []
        for _ in range(min(2, ctx.max_list_items)):
            row = ", ".join(literal_for_column(c, ctx) for c in cols)
            values.append(f"({row})")
        col_names = ", ".join(c.name for c in cols)
        return f"FROM (VALUES {', '.join(values)}) AS v({col_names})", tbl, "v"

    if path.subquery_from:
        inner_table = ctx.schema.pick_table(ctx.rng)
        inner_alias = "s"
        inner_select = gen_query_no_with(ctx, minimal=True, base_table=inner_table, alias=inner_alias)
        return f"FROM ({inner_select}) AS {inner_alias}", inner_table, inner_alias

    tables = ctx.schema.pick_tables(ctx.rng, 2 if path.comma_join or path.join_type else 1)
    primary = tables[0]
    alias = _table_alias(primary, 0)
    from_sql = f"FROM {primary.qualified_name()} AS {alias}"

    if path.comma_join and len(tables) > 1:
        alias2 = _table_alias(tables[1], 1)
        from_sql = f"{from_sql}, {tables[1].qualified_name()} AS {alias2}"
        return from_sql, primary, alias

    if path.join_type and len(tables) > 1:
        join_type = path.asof_join and f"ASOF {path.join_type}" or path.join_type
        lateral = "LATERAL " if path.lateral else ""
        alias2 = _table_alias(tables[1], 1)
        join_sql_part = f"{join_type} {lateral}{tables[1].qualified_name()} AS {alias2}"
        if "CROSS" not in join_type.upper():
            _, join_col = ctx.schema.pick_column(ctx.rng, tables[1])
            _, pk_col = ctx.schema.pick_column(ctx.rng, primary)
            join_sql_part = (
                f"{join_sql_part} ON {alias}.{pk_col.name} = {alias2}.{join_col.name}"
            )
        from_sql = f"{from_sql} {join_sql_part}"
        return from_sql, primary, alias

    return from_sql, primary, alias


def gen_group_by(ctx: GenContext, path: QueryPath, table: Table, alias: str) -> str:
    if not path.group_by:
        return ""
    cols = [c.name for c in table.columns[: min(ctx.max_list_items, len(table.columns))]]
    refs = ", ".join(f"{alias}.{c}" if alias else c for c in cols)
    if path.group_by == "rollup":
        return f"GROUP BY ROLLUP ({refs})"
    if path.group_by == "cube":
        return f"GROUP BY CUBE ({refs})"
    if path.group_by == "grouping_sets":
        return f"GROUP BY GROUPING SETS (({refs}), ({cols[0]}))"
    if path.group_by == "all":
        return "GROUP BY ALL"
    return f"GROUP BY {refs}"


def gen_limit(path: QueryPath) -> str:
    if path.limit_style == "offset":
        return "LIMIT 10 OFFSET 5"
    if path.limit_style == "comma":
        return "LIMIT 5, 10"
    if path.limit_style == "simple":
        return "LIMIT 100"
    return ""


def gen_with_clause(ctx: GenContext, path: QueryPath) -> str:
    if not path.with_cte:
        return ""
    tbl = ctx.schema.pick_table(ctx.rng)
    alias = "c"
    body = gen_query_no_with(ctx, minimal=True, base_table=tbl, alias=alias)
    recursive = "RECURSIVE " if path.recursive_cte else ""
    return f"WITH {recursive}cte AS ({body})"


def gen_query_spec(ctx: GenContext, path: QueryPath) -> str:
    from_sql, table, alias = gen_from_clause(ctx, path)
    quant = f"{path.set_quantifier} " if path.set_quantifier else ""
    select_items = gen_select_items(ctx, path, table, alias)
    if path.group_by and not path.select_star:
        select_items = ", ".join(
            [
                gen_agg_expr(ctx, table, alias),
                gen_column_ref(ctx, table, alias),
            ]
        )

    parts = ["SELECT", quant + select_items, from_sql]
    if path.where:
        parts.append(f"WHERE {gen_boolean_expr(ctx, table, alias)}")
    group_by = gen_group_by(ctx, path, table, alias)
    if group_by:
        parts.append(group_by)
    if path.having:
        parts.append(f"HAVING {gen_agg_expr(ctx, table, alias)} > 0")
    if path.qualify:
        parts.append("QUALIFY rn <= 10")
    return join_sql(parts)


def gen_query_no_with(
    ctx: GenContext,
    *,
    minimal: bool = False,
    base_table: Table | None = None,
    alias: str | None = None,
    path: QueryPath | None = None,
) -> str:
    if minimal:
        tbl = base_table or ctx.schema.pick_table(ctx.rng)
        al = alias or "t0"
        col = tbl.columns[0].name if tbl.columns else "1"
        return f"SELECT {al}.{col} FROM {tbl.qualified_name()} AS {al}"

    if path is None:
        path = QueryPath(tag="default", where=True, order_by=True, limit_style="simple")

    body = gen_query_spec(ctx, path)
    if path.order_by:
        _, table, alias = gen_from_clause(ctx, path)
        body = join_sql([body, f"ORDER BY {gen_sort_item(ctx, table, alias)}"])
    limit = gen_limit(path)
    if limit:
        body = join_sql([body, limit])
    return body


def gen_query_relation(ctx: GenContext, path: QueryPath) -> str:
    with_clause = gen_with_clause(ctx, path)
    body = gen_query_no_with(ctx, path=path)
    if with_clause:
        body = join_sql([with_clause, body])
    if path.compound_op:
        tbl = ctx.schema.pick_table(ctx.rng)
        rhs = (
            f"SELECT {tbl.columns[0].name} FROM {tbl.qualified_name()} "
            f"WHERE {gen_boolean_expr(ctx, tbl, '')}"
        )
        body = join_sql([body, path.compound_op, rhs])
    return body


def gen_query_statement(ctx: GenContext, path: QueryPath) -> str:
    body = gen_query_relation(ctx, path)
    if path.explain:
        return join_sql([path.explain, body])
    return body


def enumerate_query_paths() -> list[QueryPath]:
    paths: list[QueryPath] = []

    def add(tag: str, **kwargs):
        paths.append(QueryPath(tag=tag, **kwargs))

    add("basic_select", where=True, order_by=True, limit_style="simple")
    add("select_distinct", set_quantifier="DISTINCT", where=True)
    add("select_all_quantifier", set_quantifier="ALL", where=True)
    add("select_star", select_star=True, where=True)
    add("select_star_except", select_star=True, select_exclude=True, where=True)
    add("from_dual", from_dual=True)
    add("values_from", values_from=True, where=False)
    add("subquery_from", subquery_from=True, where=True)

    for join_type in JOIN_TYPES:
        add(f"join_{join_type.replace(' ', '_').lower()}", join_type=join_type, where=True)

    add("comma_join", comma_join=True, where=True)
    add("lateral_join", join_type="INNER JOIN", lateral=True, where=True)
    add("asof_join", asof_join=True, join_type="LEFT JOIN", where=True)

    for group_by in GROUP_BY_VARIANTS:
        add(f"group_by_{group_by}", group_by=group_by, having=True, order_by=True)

    add("qualify", window_fn=True, qualify=True, order_by=True)
    add("with_cte", with_cte=True, where=True, order_by=True)
    add("with_recursive_cte", with_cte=True, recursive_cte=True, where=True)

    for op in ["UNION", "UNION ALL", "INTERSECT", "EXCEPT", "MINUS"]:
        add(f"compound_{op.replace(' ', '_').lower()}", compound_op=op, where=True)

    add("limit_offset", where=True, limit_style="offset")
    add("limit_comma", where=True, limit_style="comma")

    for explain in ["EXPLAIN", "EXPLAIN VERBOSE", "EXPLAIN COSTS", "EXPLAIN LOGICAL"]:
        add(f"{explain.replace(' ', '_').lower()}", explain=explain, where=True)

    return paths
