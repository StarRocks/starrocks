#!/usr/bin/env python3
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

"""CLI for StarRocks grammar-aware SQL generation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .generator import generate_paths, generate_random, load_schema_from_args


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate StarRocks SQL from grammar paths in "
            "fe/fe-grammar/src/main/antlr/com/starrocks/grammar/StarRocks.g4"
        )
    )
    parser.add_argument(
        "--schema",
        help="JSON schema file with database/tables/columns.",
    )
    parser.add_argument(
        "--schema-sql",
        help="SQL file containing CREATE TABLE statements.",
    )
    parser.add_argument(
        "--mode",
        choices=["paths", "random", "all"],
        default="paths",
        help="paths=enumerate grammar branches; random=random walk; all=paths then random fill.",
    )
    parser.add_argument(
        "--categories",
        default="query,dml",
        help="Comma-separated categories: query,dml,all.",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=3,
        help="Maximum recursion depth for expression/subquery/join/cte rules.",
    )
    parser.add_argument(
        "--max-list-items",
        type=int,
        default=2,
        help="Maximum repeated list size (columns, tables, GROUP BY keys, etc.).",
    )
    parser.add_argument(
        "--max-outputs",
        type=int,
        default=500,
        help="Maximum number of SQL statements to emit.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=0,
        help="Random seed for reproducible generation.",
    )
    parser.add_argument(
        "--with-semicolon",
        action="store_true",
        help="Append semicolon to each statement.",
    )
    parser.add_argument(
        "--output",
        help="Write SQL to this file (default: stdout).",
    )
    parser.add_argument(
        "--manifest",
        help="Optional JSON manifest with path tags and categories.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.schema and not args.schema_sql:
        default_schema = Path(__file__).with_name("sample_schema.json")
        if default_schema.exists():
            args.schema = str(default_schema)
        else:
            print("error: --schema or --schema-sql is required", file=sys.stderr)
            return 2

    schema = load_schema_from_args(args.schema, args.schema_sql)
    categories = {c.strip() for c in args.categories.split(",") if c.strip()}
    if "all" in categories:
        categories = {"all"}

    if args.mode in ("paths", "all"):
        items = generate_paths(
            schema,
            categories=categories,
            max_depth=args.max_depth,
            max_list_items=args.max_list_items,
            max_outputs=args.max_outputs,
            seed=args.seed,
        )
    else:
        items = generate_random(
            schema,
            categories=categories,
            max_depth=args.max_depth,
            max_list_items=args.max_list_items,
            max_outputs=args.max_outputs,
            seed=args.seed,
        )

    if args.mode == "all" and len(items) < args.max_outputs:
        extra = generate_random(
            schema,
            categories=categories,
            max_depth=args.max_depth,
            max_list_items=args.max_list_items,
            max_outputs=args.max_outputs - len(items),
            seed=args.seed + 9999,
        )
        seen = {x.sql for x in items}
        for e in extra:
            if e.sql not in seen:
                items.append(e)
                seen.add(e.sql)
            if len(items) >= args.max_outputs:
                break

    out = sys.stdout
    if args.output:
        out = open(args.output, "w", encoding="utf-8")

    for item in items:
        sql = item.sql
        if args.with_semicolon and not sql.rstrip().endswith(";"):
            sql = f"{sql};"
        out.write(sql + "\n")

    if args.manifest:
        manifest = [
            {"sql": item.sql, "category": item.category, "path": item.path_tag}
            for item in items
        ]
        Path(args.manifest).write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    if out is not sys.stdout:
        out.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
