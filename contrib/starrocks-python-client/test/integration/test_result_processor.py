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
Integration tests verifying that ARRAY, MAP, and STRUCT columns are
deserialized to proper Python types (list, dict) rather than raw JSON
strings when queried over the MySQL protocol.
"""
import logging
import uuid
from datetime import date, datetime

import pytest
from sqlalchemy import MetaData, Table
from sqlalchemy.engine import Engine


logger = logging.getLogger(__name__)

# One suffix per worker process — computed once at import time.
_SUFFIX = uuid.uuid4().hex[:8]


class TestArrayResultProcessor:
    """ARRAY columns must be deserialized to Python lists, not strings."""

    @pytest.fixture(scope="class")
    def table_name(self, sr_root_engine: Engine):
        name = f"test_rp_array_{_SUFFIX}"
        with sr_root_engine.connect() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")
            conn.exec_driver_sql(f"""
                CREATE TABLE {name} (
                    id      INT NOT NULL,
                    int_arr ARRAY<INT>,
                    str_arr ARRAY<VARCHAR(50)>,
                    nested  ARRAY<ARRAY<INT>>
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """)
            conn.exec_driver_sql(
                f"INSERT INTO {name} VALUES"
                "  (1, [1,2,3], ['a','b','c'], [[1,2],[3,4]]),"
                "  (2, [9],     ['x'],         [[5]]),"
                "  (3, NULL,    NULL,           NULL)"
            )
            conn.commit()

        yield name

        with sr_root_engine.connect() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")
            conn.commit()

    def _rows(self, sr_root_engine: Engine, table_name: str):
        meta = MetaData()
        with sr_root_engine.connect() as conn:
            t = Table(table_name, meta, autoload_with=conn)
            return {row.id: row for row in conn.execute(t.select().order_by(t.c.id))}

    def test_array_of_ints_is_list(self, sr_root_engine: Engine, table_name: str):
        rows = self._rows(sr_root_engine, table_name)
        assert rows[1].int_arr == [1, 2, 3]
        assert isinstance(rows[1].int_arr, list)

    def test_array_of_strings_is_list(self, sr_root_engine: Engine, table_name: str):
        rows = self._rows(sr_root_engine, table_name)
        assert rows[1].str_arr == ["a", "b", "c"]
        assert isinstance(rows[1].str_arr, list)

    def test_nested_array_is_list_of_lists(self, sr_root_engine: Engine, table_name: str):
        rows = self._rows(sr_root_engine, table_name)
        assert rows[1].nested == [[1, 2], [3, 4]]
        assert isinstance(rows[1].nested[0], list)

    def test_null_array_is_none(self, sr_root_engine: Engine, table_name: str):
        rows = self._rows(sr_root_engine, table_name)
        assert rows[3].int_arr is None
        assert rows[3].str_arr is None

    def test_single_element_array(self, sr_root_engine: Engine, table_name: str):
        rows = self._rows(sr_root_engine, table_name)
        assert rows[2].int_arr == [9]
        assert rows[2].str_arr == ["x"]


class TestMapResultProcessor:
    """MAP columns must be deserialized to Python dicts, not strings."""

    @pytest.fixture(scope="class")
    def table_name(self, sr_root_engine: Engine):
        name = f"test_rp_map_{_SUFFIX}"
        with sr_root_engine.connect() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")
            conn.exec_driver_sql(f"""
                CREATE TABLE {name} (
                    id     INT NOT NULL,
                    kv     MAP<VARCHAR(20), INT>,
                    nested MAP<VARCHAR(20), ARRAY<INT>>
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """)
            # Use exec_driver_sql to bypass SQLAlchemy colon-param parsing in MAP literals
            conn.exec_driver_sql(
                f"INSERT INTO {name} VALUES"
                "  (1, map('a',1,'b',2), map('x',[1,2],'y',[3])),"
                "  (2, map('z',99),      map('m',[])),"
                "  (3, NULL,             NULL)"
            )
            conn.commit()

        yield name

        with sr_root_engine.connect() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")
            conn.commit()

    def _rows(self, sr_root_engine: Engine, table_name: str):
        meta = MetaData()
        with sr_root_engine.connect() as conn:
            t = Table(table_name, meta, autoload_with=conn)
            return {row.id: row for row in conn.execute(t.select().order_by(t.c.id))}

    def test_map_is_dict(self, sr_root_engine: Engine, table_name: str):
        rows = self._rows(sr_root_engine, table_name)
        assert isinstance(rows[1].kv, dict)
        assert rows[1].kv == {"a": 1, "b": 2}

    def test_map_with_array_values_nested(self, sr_root_engine: Engine, table_name: str):
        rows = self._rows(sr_root_engine, table_name)
        assert isinstance(rows[1].nested, dict)
        assert rows[1].nested == {"x": [1, 2], "y": [3]}
        assert isinstance(rows[1].nested["x"], list)

    def test_null_map_is_none(self, sr_root_engine: Engine, table_name: str):
        rows = self._rows(sr_root_engine, table_name)
        assert rows[3].kv is None
        assert rows[3].nested is None


class TestStructResultProcessor:
    """STRUCT columns must be deserialized to Python dicts, not strings."""

    @pytest.fixture(scope="class")
    def table_name(self, sr_root_engine: Engine):
        name = f"test_rp_struct_{_SUFFIX}"
        with sr_root_engine.connect() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")
            conn.exec_driver_sql(f"""
                CREATE TABLE {name} (
                    id     INT NOT NULL,
                    info   STRUCT<name VARCHAR(50), age INT>,
                    nested STRUCT<tags ARRAY<VARCHAR(20)>, score DECIMAL(5,2)>
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """)
            conn.exec_driver_sql(
                f"INSERT INTO {name} VALUES"
                "  (1, named_struct('name','Alice','age',30), named_struct('tags',['go','python'],'score',9.5)),"
                "  (2, named_struct('name','Bob','age',25),   named_struct('tags',['java'],'score',7.0)),"
                "  (3, NULL, NULL)"
            )
            conn.commit()

        yield name

        with sr_root_engine.connect() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")
            conn.commit()

    def _rows(self, sr_root_engine: Engine, table_name: str):
        meta = MetaData()
        with sr_root_engine.connect() as conn:
            t = Table(table_name, meta, autoload_with=conn)
            return {row.id: row for row in conn.execute(t.select().order_by(t.c.id))}

    def test_struct_is_dict(self, sr_root_engine: Engine, table_name: str):
        rows = self._rows(sr_root_engine, table_name)
        assert isinstance(rows[1].info, dict)
        assert rows[1].info["name"] == "Alice"
        assert rows[1].info["age"] == 30

    def test_nested_struct_with_array_field(self, sr_root_engine: Engine, table_name: str):
        rows = self._rows(sr_root_engine, table_name)
        assert isinstance(rows[1].nested, dict)
        assert rows[1].nested["tags"] == ["go", "python"]

    def test_null_struct_is_none(self, sr_root_engine: Engine, table_name: str):
        rows = self._rows(sr_root_engine, table_name)
        assert rows[3].info is None
        assert rows[3].nested is None


class TestResultProcessorWithDateTypes:
    """Element-level result_processors (e.g. DATE inside ARRAY) must be applied."""

    @pytest.fixture(scope="class")
    def table_name(self, sr_root_engine: Engine):
        name = f"test_rp_dates_{_SUFFIX}"
        with sr_root_engine.connect() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")
            conn.exec_driver_sql(f"""
                CREATE TABLE {name} (
                    id       INT NOT NULL,
                    date_arr ARRAY<DATE>,
                    dt_arr   ARRAY<DATETIME>
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """)
            conn.exec_driver_sql(
                f"INSERT INTO {name} VALUES"
                "  (1, ['2024-01-15','2024-06-30'], ['2024-01-15 10:30:00','2024-06-30 23:59:59'])"
            )
            conn.commit()

        yield name

        with sr_root_engine.connect() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")
            conn.commit()

    def _rows(self, sr_root_engine: Engine, table_name: str):
        meta = MetaData()
        with sr_root_engine.connect() as conn:
            t = Table(table_name, meta, autoload_with=conn)
            return {row.id: row for row in conn.execute(t.select())}

    def test_array_of_dates_elements_are_date_objects(self, sr_root_engine: Engine, table_name: str):
        rows = self._rows(sr_root_engine, table_name)
        arr = rows[1].date_arr
        assert isinstance(arr, list)
        assert arr[0] == date(2024, 1, 15)
        assert arr[1] == date(2024, 6, 30)

    def test_array_of_datetimes_elements_are_datetime_objects(self, sr_root_engine: Engine, table_name: str):
        rows = self._rows(sr_root_engine, table_name)
        arr = rows[1].dt_arr
        assert isinstance(arr, list)
        assert arr[0] == datetime(2024, 1, 15, 10, 30, 0)
        assert arr[1] == datetime(2024, 6, 30, 23, 59, 59)


class TestArrayJsonResultProcessor:
    """ARRAY<JSON> is returned as a raw string.

    StarRocks serializes ARRAY<JSON> wire values using single-quoted element
    strings (e.g. ['{"a":1}','{"b":2}']), which is not valid JSON.  Rather
    than add a special-case parser, we leave the raw string value intact.
    """

    @pytest.fixture(scope="class")
    def table_name(self, sr_root_engine: Engine):
        name = f"test_rp_array_json_{_SUFFIX}"
        with sr_root_engine.connect() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")
            conn.exec_driver_sql(f"""
                CREATE TABLE {name} (
                    id       INT NOT NULL,
                    obj_arr  ARRAY<JSON>,
                    null_arr ARRAY<JSON>
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """)
            conn.exec_driver_sql(
                f"INSERT INTO {name} VALUES"
                r"  (1, ['{\"a\":1}', '{\"b\":2}'], NULL),"
                r"  (2, ['{\"x\":[1,2]}'], NULL)"
            )
            conn.commit()

        yield name

        with sr_root_engine.connect() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")
            conn.commit()

    def _rows(self, sr_root_engine: Engine, table_name: str):
        meta = MetaData()
        with sr_root_engine.connect() as conn:
            t = Table(table_name, meta, autoload_with=conn)
            return {row.id: row for row in conn.execute(t.select().order_by(t.c.id))}

    def test_array_of_json_is_raw_string(self, sr_root_engine: Engine, table_name: str):
        # StarRocks sends ARRAY<JSON> in a non-standard format; we return the raw string.
        rows = self._rows(sr_root_engine, table_name)
        assert isinstance(rows[1].obj_arr, str)

    def test_null_array_of_json_is_none(self, sr_root_engine: Engine, table_name: str):
        rows = self._rows(sr_root_engine, table_name)
        assert rows[1].null_arr is None
