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

"""DML generation aligned with StarRocks.g4 insert/update/delete/merge rules."""

from __future__ import annotations

from dataclasses import dataclass

from .context import GenContext
from .expressions import gen_boolean_expr, gen_expression, join_sql, literal_for_column
from .query import gen_query_no_with


@dataclass(frozen=True)
class DmlPath:
    tag: str
    kind: str
    with_cte: bool = False
    overwrite: bool = False
    by_name: bool = False
    using_join: bool = False
    use_values: bool = False
    explain: str | None = None


def gen_insert(ctx: GenContext, path: DmlPath) -> str:
    tbl = ctx.schema.pick_table(ctx.rng)
    target = tbl.qualified_name()
    overwrite = "OVERWRITE" if path.overwrite else "INTO"
    with_clause = ""
    if path.with_cte:
        with_clause = f"WITH cte AS (SELECT 1) "
    by_name = " BY NAME" if path.by_name else ""

    if path.use_values:
        cols = tbl.columns[: min(3, len(tbl.columns))]
        row = ", ".join(literal_for_column(c, ctx) for c in cols)
        values = f"VALUES ({row})"
        sql = f"INSERT {overwrite} {target}{by_name} {values}"
    else:
        select_sql = gen_query_no_with(ctx, minimal=False)
        sql = f"INSERT {overwrite} {target}{by_name} {select_sql}"

    if path.explain:
        sql = join_sql([path.explain, sql])
    return join_sql([with_clause, sql])


def gen_update(ctx: GenContext, path: DmlPath) -> str:
    tbl = ctx.schema.pick_table(ctx.rng)
    assignments = []
    for col in tbl.columns[: min(2, len(tbl.columns))]:
        assignments.append(f"{col.name} = {literal_for_column(col, ctx)}")
    set_clause = ", ".join(assignments)

    from_clause = ""
    if path.using_join and len(ctx.schema.tables) > 1:
        other = ctx.rng.choice([t for t in ctx.schema.tables if t.name != tbl.name])
        from_clause = f"FROM {other.qualified_name()} AS s"
        where = f"WHERE {tbl.name}.{tbl.columns[0].name} = s.{other.columns[0].name}"
    else:
        where = f"WHERE {gen_boolean_expr(ctx, tbl, tbl.name)}"

    with_clause = ""
    if path.with_cte:
        with_clause = "WITH cte AS (SELECT 1) "

    sql = f"UPDATE {tbl.qualified_name()} SET {set_clause} {from_clause} {where}".strip()
    if path.explain:
        sql = join_sql([path.explain, sql])
    return join_sql([with_clause, sql])


def gen_delete(ctx: GenContext, path: DmlPath) -> str:
    tbl = ctx.schema.pick_table(ctx.rng)
    using = ""
    if path.using_join and len(ctx.schema.tables) > 1:
        other = ctx.rng.choice([t for t in ctx.schema.tables if t.name != tbl.name])
        using = f"USING {other.qualified_name()} AS s"
        where = f"WHERE {tbl.name}.{tbl.columns[0].name} = s.{other.columns[0].name}"
    else:
        where = f"WHERE {gen_boolean_expr(ctx, tbl, tbl.name)}"

    with_clause = ""
    if path.with_cte:
        with_clause = "WITH cte AS (SELECT 1) "

    sql = f"DELETE FROM {tbl.qualified_name()} {using} {where}".strip()
    if path.explain:
        sql = join_sql([path.explain, sql])
    return join_sql([with_clause, sql])


def gen_merge(ctx: GenContext, path: DmlPath) -> str:
    target = ctx.schema.pick_table(ctx.rng)
    source = ctx.rng.choice([t for t in ctx.schema.tables if t.name != target.name])
    on = f"{target.columns[0].name} = s.{source.columns[0].name}"
    set_parts = []
    for col in target.columns[: min(2, len(target.columns))]:
        set_parts.append(f"{col.name} = {literal_for_column(col, ctx)}")
    update_set = ", ".join(set_parts)
    insert_vals = ", ".join(literal_for_column(c, ctx) for c in target.columns[: min(3, len(target.columns))])
    insert_cols = ", ".join(c.name for c in target.columns[: min(3, len(target.columns))])

    sql = (
        f"MERGE INTO {target.qualified_name()} AS t "
        f"USING {source.qualified_name()} AS s "
        f"ON {on} "
        f"WHEN MATCHED THEN UPDATE SET {update_set} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )
    if path.explain:
        sql = join_sql([path.explain, sql])
    return sql


def gen_dml(ctx: GenContext, path: DmlPath) -> str:
    if path.kind == "insert":
        return gen_insert(ctx, path)
    if path.kind == "update":
        return gen_update(ctx, path)
    if path.kind == "delete":
        return gen_delete(ctx, path)
    if path.kind == "merge":
        return gen_merge(ctx, path)
    raise ValueError(f"unknown dml kind: {path.kind}")


def enumerate_dml_paths() -> list[DmlPath]:
    paths = [
        DmlPath(tag="insert_select", kind="insert"),
        DmlPath(tag="insert_values", kind="insert", use_values=True),
        DmlPath(tag="insert_overwrite_select", kind="insert", overwrite=True),
        DmlPath(tag="insert_by_name_values", kind="insert", by_name=True, use_values=True),
        DmlPath(tag="insert_explain", kind="insert", explain="EXPLAIN"),
        DmlPath(tag="update_basic", kind="update"),
        DmlPath(tag="update_with_from", kind="update", using_join=True),
        DmlPath(tag="update_with_cte", kind="update", with_cte=True),
        DmlPath(tag="delete_basic", kind="delete"),
        DmlPath(tag="delete_using", kind="delete", using_join=True),
        DmlPath(tag="delete_with_cte", kind="delete", with_cte=True),
        DmlPath(tag="merge_basic", kind="merge"),
        DmlPath(tag="merge_explain", kind="merge", explain="EXPLAIN"),
    ]
    return paths
