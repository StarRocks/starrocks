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

"""Top-level SQL generator orchestrating query and DML paths."""

from __future__ import annotations

import random
from dataclasses import dataclass

from .context import GenContext
from .dml import DmlPath, enumerate_dml_paths, gen_dml
from .query import QueryPath, enumerate_query_paths, gen_query_statement
from .schema import Schema, load_schema, parse_schema_from_sql


@dataclass
class GeneratedSql:
    sql: str
    category: str
    path_tag: str


def _dedupe(items: list[GeneratedSql]) -> list[GeneratedSql]:
    seen = set()
    out: list[GeneratedSql] = []
    for item in items:
        key = item.sql.strip()
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def generate_paths(
    schema: Schema,
    *,
    categories: set[str],
    max_depth: int,
    max_list_items: int,
    max_outputs: int,
    seed: int,
) -> list[GeneratedSql]:
    base_ctx = GenContext(
        schema=schema,
        max_depth=max_depth,
        max_list_items=max_list_items,
        seed=seed,
    )
    results: list[GeneratedSql] = []

    if "query" in categories or "all" in categories:
        for idx, path in enumerate(enumerate_query_paths()):
            ctx = base_ctx.clone(path_tag=path.tag, seed=seed + idx)
            sql = gen_query_statement(ctx, path)
            results.append(GeneratedSql(sql=sql, category="query", path_tag=path.tag))

    if "dml" in categories or "all" in categories:
        for idx, path in enumerate(enumerate_dml_paths()):
            ctx = base_ctx.clone(path_tag=path.tag, seed=seed + 1000 + idx)
            sql = gen_dml(ctx, path)
            results.append(GeneratedSql(sql=sql, category="dml", path_tag=path.tag))

    results = _dedupe(results)
    return results[:max_outputs]


def generate_random(
    schema: Schema,
    *,
    categories: set[str],
    max_depth: int,
    max_list_items: int,
    max_outputs: int,
    seed: int,
) -> list[GeneratedSql]:
    rng = random.Random(seed)
    query_paths = enumerate_query_paths()
    dml_paths = enumerate_dml_paths()
    results: list[GeneratedSql] = []

    for i in range(max_outputs):
        ctx = GenContext(
            schema=schema,
            max_depth=max_depth,
            max_list_items=max_list_items,
            seed=seed + i,
        )
        cat = rng.choice(list(categories)) if categories != {"all"} else rng.choice(["query", "dml"])
        if cat == "query" or (cat == "all" and rng.random() < 0.8):
            path = rng.choice(query_paths)
            sql = gen_query_statement(ctx, path)
            results.append(GeneratedSql(sql=sql, category="query", path_tag=path.tag))
        else:
            path = rng.choice(dml_paths)
            sql = gen_dml(ctx, path)
            results.append(GeneratedSql(sql=sql, category="dml", path_tag=path.tag))

    return _dedupe(results)[:max_outputs]


def load_schema_from_args(schema_json: str | None, schema_sql: str | None) -> Schema:
    if schema_json:
        return load_schema(schema_json)
    if schema_sql:
        return parse_schema_from_sql(schema_sql)
    raise ValueError("one of --schema or --schema-sql is required")
