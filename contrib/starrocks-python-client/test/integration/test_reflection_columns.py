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

from sqlalchemy import Column, Integer, MetaData, String, Table, inspect, testing
from sqlalchemy.testing import fixtures

from starrocks.common.params import ColumnAggInfoKey, DialectName
from starrocks.common.types import ColumnAggType
from starrocks.datatype import BITMAP, HLL


class ReflectionColumnsAggTest(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "starrocks"

    def test_reflect_aggregate_key_table(self, connection, metadata):
        Table(
            "test_reflect_columns",
            metadata,
            Column("id", Integer),
            Column("key1", String(32), starrocks_IS_AGG_KEY=True),
            Column("val_sum", Integer, starrocks_agg_type="sum"),
            Column("val_min", Integer, starrocks_agg_type="Min"),
            Column("val_max", Integer, starrocks_agg_type=ColumnAggType.MAX),
            Column("val_replace", String(32), starrocks_agg_type=ColumnAggType.REPLACE),
            Column("val_replace_if_not_null", Integer, starrocks_agg_type=ColumnAggType.REPLACE_IF_NOT_NULL),
            Column("val_hll", HLL, starrocks_agg_type=ColumnAggType.HLL_UNION),
            Column("val_bitmap", BITMAP, starrocks_agg_type=ColumnAggType.BITMAP_UNION),
            starrocks_engine="OLAP",
            starrocks_aggregate_key="id, key1",
            starrocks_distributed_by="HASH(id)",
            starrocks_properties={"replication_num": "1"},
        )
        metadata.create_all(connection)

        reflected = Table("test_reflect_columns", MetaData(), autoload_with=connection)
        agg_types = {
            column.name: column.dialect_options[DialectName].get(ColumnAggInfoKey.AGG_TYPE)
            for column in reflected.columns
        }

        assert agg_types == {
            "id": None,
            "key1": None,
            "val_sum": ColumnAggType.SUM,
            "val_min": ColumnAggType.MIN,
            "val_max": ColumnAggType.MAX,
            "val_replace": ColumnAggType.REPLACE,
            "val_replace_if_not_null": ColumnAggType.REPLACE_IF_NOT_NULL,
            "val_hll": ColumnAggType.HLL_UNION,
            "val_bitmap": ColumnAggType.BITMAP_UNION,
        }


class ReflectionColumnAutoIncrementTest(fixtures.TestBase):
    """The tables are created with plain DDL, so reflection is checked against what the server reports."""

    __backend__ = True
    __only_on__ = "starrocks"

    @testing.fixture
    def table_from_ddl(self, connection):
        created = []

        def create(name: str, id_column: str) -> str:
            connection.exec_driver_sql(
                f"CREATE TABLE {name} ({id_column}, v INT) DISTRIBUTED BY RANDOM PROPERTIES(\"replication_num\"=\"1\")"
            )
            created.append(name)
            return name

        yield create
        for name in created:
            connection.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")

    @testing.combinations(
        ("autoincrement", "id BIGINT NOT NULL AUTO_INCREMENT", True),
        ("no_autoincrement", "id BIGINT NOT NULL", False),
        argnames="id_column, expected",
        id_="iaa",
    )
    def test_reflects_autoincrement(self, connection, table_from_ddl, id_column, expected):
        table_name = table_from_ddl("reflect_columns_ai", id_column)
        id_col = next(c for c in inspect(connection).get_columns(table_name) if c["name"].lower() == "id")
        assert id_col["autoincrement"] is expected
