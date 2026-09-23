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

"""
Integration tests verifying that ARRAY, MAP, and STRUCT columns accept
Python-native bind parameters (list, dict) via ``sqlalchemy.insert()``,
rather than requiring hand-written literal SQL.

These are the write-side counterpart to test_result_processor.py: without
``bind_processor``/``bind_expression`` support, the DBAPI driver falls back
to escaping lists/dicts as SQL tuples (e.g. ``('a', 'b')``), which StarRocks
rejects as invalid syntax for a structured-type literal.
"""
import logging
import uuid

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, insert, select
from sqlalchemy.engine import Engine

from starrocks.datatype import ARRAY, INTEGER, MAP, STRUCT, VARCHAR


logger = logging.getLogger(__name__)

# One suffix per worker process — computed once at import time.
_SUFFIX = uuid.uuid4().hex[:8]


def _rows(engine: Engine, table: Table):
    with engine.connect() as conn:
        return {row.id: row for row in conn.execute(select(table).order_by(table.c.id))}


class TestArrayBindExpression:
    """ARRAY columns must accept Python lists as bind parameters."""

    @pytest.fixture(scope="class")
    def table(self, sr_root_engine: Engine):
        name = f"test_bind_array_{_SUFFIX}"
        metadata = MetaData()
        t = Table(
            name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("int_arr", ARRAY(INTEGER)),
            Column("str_arr", ARRAY(VARCHAR(50))),
            Column("nested", ARRAY(ARRAY(INTEGER))),
            starrocks_distributed_by="HASH(id) BUCKETS 1",
            starrocks_properties={"replication_num": "1"},
        )
        with sr_root_engine.begin() as conn:
            metadata.drop_all(conn)
            metadata.create_all(conn)

        yield t

        with sr_root_engine.begin() as conn:
            metadata.drop_all(conn)

    @pytest.fixture(scope="class", autouse=True)
    def seed(self, sr_root_engine: Engine, table: Table):
        with sr_root_engine.begin() as conn:
            conn.execute(
                insert(table),
                [
                    {"id": 1, "int_arr": [1, 2, 3], "str_arr": ["a", "b"], "nested": [[1, 2], [3]]},
                    {"id": 2, "int_arr": [9], "str_arr": ["x"], "nested": [[5]]},
                    {"id": 3, "int_arr": None, "str_arr": None, "nested": None},
                ],
            )

    def test_array_of_ints_round_trips(self, sr_root_engine: Engine, table: Table):
        rows = _rows(sr_root_engine, table)
        assert rows[1].int_arr == [1, 2, 3]

    def test_array_of_strings_round_trips(self, sr_root_engine: Engine, table: Table):
        rows = _rows(sr_root_engine, table)
        assert rows[1].str_arr == ["a", "b"]

    def test_nested_array_round_trips(self, sr_root_engine: Engine, table: Table):
        rows = _rows(sr_root_engine, table)
        assert rows[1].nested == [[1, 2], [3]]

    def test_null_array_round_trips(self, sr_root_engine: Engine, table: Table):
        rows = _rows(sr_root_engine, table)
        assert rows[3].int_arr is None
        assert rows[3].str_arr is None
        assert rows[3].nested is None

    def test_single_element_array_round_trips(self, sr_root_engine: Engine, table: Table):
        rows = _rows(sr_root_engine, table)
        assert rows[2].int_arr == [9]
        assert rows[2].str_arr == ["x"]


class TestMapBindExpression:
    """MAP columns must accept Python dicts as bind parameters."""

    @pytest.fixture(scope="class")
    def table(self, sr_root_engine: Engine):
        name = f"test_bind_map_{_SUFFIX}"
        metadata = MetaData()
        t = Table(
            name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("kv", MAP(VARCHAR(20), INTEGER)),
            Column("nested", MAP(VARCHAR(20), ARRAY(INTEGER))),
            starrocks_distributed_by="HASH(id) BUCKETS 1",
            starrocks_properties={"replication_num": "1"},
        )
        with sr_root_engine.begin() as conn:
            metadata.drop_all(conn)
            metadata.create_all(conn)

        yield t

        with sr_root_engine.begin() as conn:
            metadata.drop_all(conn)

    @pytest.fixture(scope="class", autouse=True)
    def seed(self, sr_root_engine: Engine, table: Table):
        with sr_root_engine.begin() as conn:
            conn.execute(
                insert(table),
                [
                    {"id": 1, "kv": {"a": 1, "b": 2}, "nested": {"x": [1, 2], "y": [3]}},
                    {"id": 2, "kv": {"z": 99}, "nested": {"m": []}},
                    {"id": 3, "kv": None, "nested": None},
                ],
            )

    def test_map_round_trips(self, sr_root_engine: Engine, table: Table):
        rows = _rows(sr_root_engine, table)
        assert rows[1].kv == {"a": 1, "b": 2}

    def test_map_with_array_values_round_trips(self, sr_root_engine: Engine, table: Table):
        rows = _rows(sr_root_engine, table)
        assert rows[1].nested == {"x": [1, 2], "y": [3]}

    def test_null_map_round_trips(self, sr_root_engine: Engine, table: Table):
        rows = _rows(sr_root_engine, table)
        assert rows[3].kv is None
        assert rows[3].nested is None


class TestStructBindExpression:
    """STRUCT columns must accept Python dicts as bind parameters."""

    @pytest.fixture(scope="class")
    def table(self, sr_root_engine: Engine):
        name = f"test_bind_struct_{_SUFFIX}"
        metadata = MetaData()
        t = Table(
            name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("info", STRUCT(name=VARCHAR(50), age=INTEGER)),
            Column("nested", STRUCT(tags=ARRAY(VARCHAR(20)), score=INTEGER)),
            starrocks_distributed_by="HASH(id) BUCKETS 1",
            starrocks_properties={"replication_num": "1"},
        )
        with sr_root_engine.begin() as conn:
            metadata.drop_all(conn)
            metadata.create_all(conn)

        yield t

        with sr_root_engine.begin() as conn:
            metadata.drop_all(conn)

    @pytest.fixture(scope="class", autouse=True)
    def seed(self, sr_root_engine: Engine, table: Table):
        with sr_root_engine.begin() as conn:
            conn.execute(
                insert(table),
                [
                    {
                        "id": 1,
                        "info": {"name": "Alice", "age": 30},
                        "nested": {"tags": ["go", "python"], "score": 9},
                    },
                    {"id": 2, "info": None, "nested": None},
                ],
            )

    def test_struct_round_trips(self, sr_root_engine: Engine, table: Table):
        rows = _rows(sr_root_engine, table)
        assert rows[1].info == {"name": "Alice", "age": 30}

    def test_struct_with_array_field_round_trips(self, sr_root_engine: Engine, table: Table):
        rows = _rows(sr_root_engine, table)
        assert rows[1].nested == {"tags": ["go", "python"], "score": 9}

    def test_null_struct_round_trips(self, sr_root_engine: Engine, table: Table):
        rows = _rows(sr_root_engine, table)
        assert rows[2].info is None
        assert rows[2].nested is None
