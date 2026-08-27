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

"""Load table/column metadata used to generate schema-aware SQL."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = True

    @property
    def is_numeric(self) -> bool:
        t = self.type.upper()
        return any(k in t for k in ("INT", "BIGINT", "DOUBLE", "FLOAT", "DECIMAL", "NUMERIC"))

    @property
    def is_string(self) -> bool:
        t = self.type.upper()
        return any(k in t for k in ("CHAR", "STRING", "TEXT"))

    @property
    def is_datetime(self) -> bool:
        t = self.type.upper()
        return any(k in t for k in ("DATE", "TIME", "DATETIME"))

    @property
    def is_boolean(self) -> bool:
        return "BOOL" in self.type.upper()

    @property
    def is_json(self) -> bool:
        return "JSON" in self.type.upper()

    @property
    def is_array(self) -> bool:
        return self.type.upper().startswith("ARRAY")

    @property
    def is_map(self) -> bool:
        return self.type.upper().startswith("MAP")


@dataclass
class Table:
    name: str
    columns: list[Column]
    database: str | None = None

    def qualified_name(self) -> str:
        if self.database:
            return f"{self.database}.{self.name}"
        return self.name

    def column(self, name: str) -> Column | None:
        for col in self.columns:
            if col.name.lower() == name.lower():
                return col
        return None


@dataclass
class Schema:
    database: str | None = None
    tables: list[Table] = field(default_factory=list)

    def table(self, name: str) -> Table | None:
        for tbl in self.tables:
            if tbl.name.lower() == name.lower():
                return tbl
        return None

    def pick_table(self, rng) -> Table:
        if not self.tables:
            raise ValueError("schema has no tables")
        return rng.choice(self.tables)

    def pick_tables(self, rng, count: int) -> list[Table]:
        if not self.tables:
            raise ValueError("schema has no tables")
        count = min(count, len(self.tables))
        return rng.sample(self.tables, count)

    def pick_column(self, rng, table: Table | None = None) -> tuple[Table, Column]:
        tbl = table or self.pick_table(rng)
        if not tbl.columns:
            raise ValueError(f"table {tbl.name} has no columns")
        return tbl, rng.choice(tbl.columns)

    def numeric_columns(self) -> list[tuple[Table, Column]]:
        out = []
        for tbl in self.tables:
            for col in tbl.columns:
                if col.is_numeric:
                    out.append((tbl, col))
        return out

    def string_columns(self) -> list[tuple[Table, Column]]:
        out = []
        for tbl in self.tables:
            for col in tbl.columns:
                if col.is_string:
                    out.append((tbl, col))
        return out

    def datetime_columns(self) -> list[tuple[Table, Column]]:
        out = []
        for tbl in self.tables:
            for col in tbl.columns:
                if col.is_datetime:
                    out.append((tbl, col))
        return out


def _parse_column(raw: dict[str, Any]) -> Column:
    return Column(
        name=raw["name"],
        type=raw.get("type", "VARCHAR"),
        nullable=raw.get("nullable", True),
    )


def _parse_table(raw: dict[str, Any], default_db: str | None) -> Table:
    db = raw.get("database", default_db)
    return Table(
        name=raw["name"],
        database=db,
        columns=[_parse_column(c) for c in raw.get("columns", [])],
    )


def load_schema(path: str | Path) -> Schema:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    default_db = data.get("database")
    tables = [_parse_table(t, default_db) for t in data.get("tables", [])]
    return Schema(database=default_db, tables=tables)


def parse_schema_from_sql(path: str | Path) -> Schema:
    """Best-effort parser for CREATE TABLE blocks in a SQL file."""
    text = Path(path).read_text(encoding="utf-8")
    database = None
    db_match = re.search(
        r"CREATE\s+DATABASE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)",
        text,
        re.IGNORECASE,
    )
    if db_match:
        database = db_match.group(1)

    tables: list[Table] = []
    create_pattern = re.compile(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
        r"(?:(\w+)\.)?(\w+)\s*\((.*?)\)\s*ENGINE\s*=",
        re.IGNORECASE | re.DOTALL,
    )
    col_pattern = re.compile(
        r"^\s*(\w+)\s+([A-Z<>,\(\)\d\s]+?)(?:\s+NOT\s+NULL|\s+NULL|\s+DEFAULT|\s*,|\s*$)",
        re.IGNORECASE | re.MULTILINE,
    )
    for match in create_pattern.finditer(text):
        db_name = match.group(1) or database
        table_name = match.group(2)
        body = match.group(3)
        columns: list[Column] = []
        for line in body.splitlines():
            line = line.strip().rstrip(",")
            if not line or line.upper().startswith(
                ("PRIMARY", "UNIQUE", "AGGREGATE", "DUPLICATE", "KEY", "PARTITION", "DISTRIBUTED")
            ):
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            col_name = parts[0]
            col_type = parts[1]
            nullable = "NOT NULL" not in line.upper()
            columns.append(Column(name=col_name, type=col_type, nullable=nullable))
        if columns:
            tables.append(Table(name=table_name, database=db_name, columns=columns))

    return Schema(database=database, tables=tables)
