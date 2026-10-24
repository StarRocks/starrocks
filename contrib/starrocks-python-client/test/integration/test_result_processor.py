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
from datetime import date, datetime
import logging

from sqlalchemy import MetaData, Table, testing
from sqlalchemy.testing import config, fixtures


logger = logging.getLogger(__name__)


class ArrayResultProcessorTest(fixtures.TestBase):
    """ARRAY columns must be deserialized to Python lists, not strings."""

    __backend__ = True
    __only_on__ = "starrocks"

    @testing.fixture(scope="class")
    def table_name(self):
        name = "test_rp_array"
        with config.db.begin() as conn:
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

        yield name

        with config.db.begin() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")

    def _rows(self, table_name: str):
        meta = MetaData()
        with config.db.begin() as conn:
            t = Table(table_name, meta, autoload_with=conn)
            return {row.id: row for row in conn.execute(t.select().order_by(t.c.id))}

    def test_array_of_ints_is_list(self, table_name):
        rows = self._rows(table_name)
        assert rows[1].int_arr == [1, 2, 3]
        assert isinstance(rows[1].int_arr, list)

    def test_array_of_strings_is_list(self, table_name):
        rows = self._rows(table_name)
        assert rows[1].str_arr == ["a", "b", "c"]
        assert isinstance(rows[1].str_arr, list)

    def test_nested_array_is_list_of_lists(self, table_name):
        rows = self._rows(table_name)
        assert rows[1].nested == [[1, 2], [3, 4]]
        assert isinstance(rows[1].nested[0], list)

    def test_null_array_is_none(self, table_name):
        rows = self._rows(table_name)
        assert rows[3].int_arr is None
        assert rows[3].str_arr is None

    def test_single_element_array(self, table_name):
        rows = self._rows(table_name)
        assert rows[2].int_arr == [9]
        assert rows[2].str_arr == ["x"]


class MapResultProcessorTest(fixtures.TestBase):
    """MAP columns must be deserialized to Python dicts, not strings."""

    __backend__ = True
    __only_on__ = "starrocks"

    @testing.fixture(scope="class")
    def table_name(self):
        name = "test_rp_map"
        with config.db.begin() as conn:
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

        yield name

        with config.db.begin() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")

    def _rows(self, table_name: str):
        meta = MetaData()
        with config.db.begin() as conn:
            t = Table(table_name, meta, autoload_with=conn)
            return {row.id: row for row in conn.execute(t.select().order_by(t.c.id))}

    def test_map_is_dict(self, table_name):
        rows = self._rows(table_name)
        assert isinstance(rows[1].kv, dict)
        assert rows[1].kv == {"a": 1, "b": 2}

    def test_map_with_array_values_nested(self, table_name):
        rows = self._rows(table_name)
        assert isinstance(rows[1].nested, dict)
        assert rows[1].nested == {"x": [1, 2], "y": [3]}
        assert isinstance(rows[1].nested["x"], list)

    def test_null_map_is_none(self, table_name):
        rows = self._rows(table_name)
        assert rows[3].kv is None
        assert rows[3].nested is None


class StructResultProcessorTest(fixtures.TestBase):
    """STRUCT columns must be deserialized to Python dicts, not strings."""

    __backend__ = True
    __only_on__ = "starrocks"

    @testing.fixture(scope="class")
    def table_name(self):
        name = "test_rp_struct"
        with config.db.begin() as conn:
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

        yield name

        with config.db.begin() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")

    def _rows(self, table_name: str):
        meta = MetaData()
        with config.db.begin() as conn:
            t = Table(table_name, meta, autoload_with=conn)
            return {row.id: row for row in conn.execute(t.select().order_by(t.c.id))}

    def test_struct_is_dict(self, table_name):
        rows = self._rows(table_name)
        assert isinstance(rows[1].info, dict)
        assert rows[1].info["name"] == "Alice"
        assert rows[1].info["age"] == 30

    def test_nested_struct_with_array_field(self, table_name):
        rows = self._rows(table_name)
        assert isinstance(rows[1].nested, dict)
        assert rows[1].nested["tags"] == ["go", "python"]

    def test_null_struct_is_none(self, table_name):
        rows = self._rows(table_name)
        assert rows[3].info is None
        assert rows[3].nested is None


class ResultProcessorWithDateTypesTest(fixtures.TestBase):
    """Element-level result_processors (e.g. DATE inside ARRAY) must be applied."""

    __backend__ = True
    __only_on__ = "starrocks"

    @testing.fixture(scope="class")
    def table_name(self):
        name = "test_rp_dates"
        with config.db.begin() as conn:
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

        yield name

        with config.db.begin() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")

    def _rows(self, table_name: str):
        meta = MetaData()
        with config.db.begin() as conn:
            t = Table(table_name, meta, autoload_with=conn)
            return {row.id: row for row in conn.execute(t.select())}

    def test_array_of_dates_elements_are_date_objects(self, table_name):
        rows = self._rows(table_name)
        arr = rows[1].date_arr
        assert isinstance(arr, list)
        assert arr[0] == date(2024, 1, 15)
        assert arr[1] == date(2024, 6, 30)

    def test_array_of_datetimes_elements_are_datetime_objects(self, table_name):
        rows = self._rows(table_name)
        arr = rows[1].dt_arr
        assert isinstance(arr, list)
        assert arr[0] == datetime(2024, 1, 15, 10, 30, 0)
        assert arr[1] == datetime(2024, 6, 30, 23, 59, 59)


class ArrayJsonResultProcessorTest(fixtures.TestBase):
    """ARRAY<JSON> is read as a list of decoded JSON values.

    StarRocks writes the elements as single-quoted strings, as in ['{"a":1}','{"b":2}'], which is not JSON; the
    dialect's parser reads them, and decodes each element as JSON.
    """

    __backend__ = True
    __only_on__ = "starrocks"

    @testing.fixture(scope="class")
    def table_name(self):
        name = "test_rp_array_json"
        with config.db.begin() as conn:
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

        yield name

        with config.db.begin() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")

    def _rows(self, table_name: str):
        meta = MetaData()
        with config.db.begin() as conn:
            t = Table(table_name, meta, autoload_with=conn)
            return {row.id: row for row in conn.execute(t.select().order_by(t.c.id))}

    def test_array_of_json_decodes_single_quoted_items(self, table_name):
        rows = self._rows(table_name)
        assert rows[1].obj_arr == [{"a": 1}, {"b": 2}]
        assert rows[2].obj_arr == [{"x": [1, 2]}]

    def test_null_array_of_json_is_none(self, table_name):
        rows = self._rows(table_name)
        assert rows[1].null_arr is None
