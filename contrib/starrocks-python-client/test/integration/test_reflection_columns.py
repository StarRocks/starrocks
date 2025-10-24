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

import logging
from typing import Union

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, inspect, text
from sqlalchemy.engine import Engine, Inspector
from sqlalchemy.engine.interfaces import ReflectedColumn

from starrocks.common.params import ColumnAggInfoKey, DialectName
from starrocks.common.types import ColumnAggType
from starrocks.datatype import BITMAP, HLL
from test.conftest_sr import create_test_engine, test_default_schema


logger = logging.getLogger(__name__)


class TestReflectionColumnsAggIntegration:
    def test_reflect_aggregate_key_table(self, sr_root_engine: Engine):
        table_name = "test_reflect_columns"
        metadata = MetaData()
        sr_engine = sr_root_engine

        table = Table(
            table_name,
            metadata,
            Column("id", Integer),
            Column("key1", String(32), starrocks_IS_AGG_KEY=True),
            Column("val_sum", Integer, starrocks_agg_type="sum"),
            # Column("val_count", Integer, starrocks_agg_type=ColumnAggType.COUNT),
            Column("val_min", Integer, starrocks_agg_type="Min"),
            Column("val_max", Integer, starrocks_agg_type=ColumnAggType.MAX),

            Column("val_replace", String(32), starrocks_agg_type=ColumnAggType.REPLACE),
            Column(
                "val_replace_if_not_null",
                Integer,
                starrocks_agg_type=ColumnAggType.REPLACE_IF_NOT_NULL,
            ),
            Column("val_hll", HLL, starrocks_agg_type=ColumnAggType.HLL_UNION),
            Column("val_bitmap", BITMAP, starrocks_agg_type=ColumnAggType.BITMAP_UNION),
            starrocks_engine='OLAP',
            starrocks_aggregate_key='id, key1',
            starrocks_distributed_by='HASH(id)',
            starrocks_properties={"replication_num": "1"},
        )

        with sr_engine.connect() as connection:
            connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            table.create(connection)

            try:
                # Create a map of column name to its reflected info dict
                conn_db = MetaData()
                conn_table = Table(table_name, conn_db, autoload_with=sr_engine, schema=test_default_schema)
                reflected_map: dict[str, ReflectedColumn] = {col.name: col for col in conn_table.columns}

                # Create a map of column name to its reflected info dict
                # By using this way, the dialect options are raw as set, haven't been processed by `check_kwargs`
                # to form dialect_options object/property.
                # inspector = inspect(sr_engine)
                # reflected_columns: list[ReflectedColumn] = inspector.get_columns(table_name)
                #reflected_map: dict[str, ReflectedColumn] = {col["name"]: col for col in reflected_columns}


                # Assertions
                logger.info(f"reflected_column: id {reflected_map['id']!r}")
                assert ColumnAggInfoKey.AGG_TYPE not in reflected_map["id"].dialect_options[DialectName] or \
                    reflected_map["id"].dialect_options[DialectName][ColumnAggInfoKey.AGG_TYPE] is None
                logger.info(f"reflected_column: key1 {reflected_map['key1']!r}")
                assert ColumnAggInfoKey.AGG_TYPE not in reflected_map["key1"].dialect_options[DialectName] or \
                    reflected_map["key1"].dialect_options[DialectName][ColumnAggInfoKey.AGG_TYPE] is None
                logger.info(f"reflected_column: val_sum {reflected_map['val_sum']!r}")
                logger.debug(f"dialect options for column: val_sum {reflected_map['val_sum'].dialect_options[DialectName]}")
                assert reflected_map["val_sum"].dialect_options[DialectName][ColumnAggInfoKey.AGG_TYPE] == ColumnAggType.SUM
                assert reflected_map["val_replace"].dialect_options[DialectName][ColumnAggInfoKey.AGG_TYPE] == ColumnAggType.REPLACE
                assert (
                    reflected_map["val_replace_if_not_null"].dialect_options[DialectName][ColumnAggInfoKey.AGG_TYPE]
                    == ColumnAggType.REPLACE_IF_NOT_NULL
                )
                assert reflected_map["val_min"].dialect_options[DialectName][ColumnAggInfoKey.AGG_TYPE] == ColumnAggType.MIN
                assert reflected_map["val_max"].dialect_options[DialectName][ColumnAggInfoKey.AGG_TYPE] == ColumnAggType.MAX
                assert reflected_map["val_hll"].dialect_options[DialectName][ColumnAggInfoKey.AGG_TYPE] == ColumnAggType.HLL_UNION
                assert (
                    reflected_map["val_bitmap"].dialect_options[DialectName][ColumnAggInfoKey.AGG_TYPE]
                    == ColumnAggType.BITMAP_UNION
                )

            finally:
                # Clean up
                connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))


@pytest.mark.integration
class TestReflectionColumnAutoIncrementIntegration:
    sr_engine: Engine
    test_schema: Union[str, None]

    @classmethod
    def setup_class(cls) -> None:
        cls.sr_engine = create_test_engine()
        cls.test_schema = test_default_schema
        with cls.sr_engine.begin() as conn:
            if cls.test_schema:
                conn.exec_driver_sql(f"CREATE DATABASE IF NOT EXISTS {cls.test_schema}")

    @classmethod
    def teardown_class(cls) -> None:
        with cls.sr_engine.begin() as conn:
            res = conn.exec_driver_sql("SHOW TABLES")
            for row in res:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {row[0]}")
        cls.sr_engine.dispose()

    def _full_table_name(self, name: str) -> str:
        return f"{self.test_schema}.{name}" if self.test_schema else name

    @pytest.mark.xfail(reason="AUTO_INCREMENT reflection not implemented yet", strict=False)
    def test_reflects_autoincrement_true(self) -> None:
        tname = "reflect_columns_ai_true"
        with self.sr_engine.begin() as conn:
            conn.exec_driver_sql(
                f"""
                CREATE TABLE {self._full_table_name(tname)} (
                    id BIGINT NOT NULL AUTO_INCREMENT,
                    v INT
                )
                DISTRIBUTED BY RANDOM
                PROPERTIES("replication_num"="1")
                """
            )
        try:
            insp: Inspector = inspect(self.sr_engine)
            cols: list[ReflectedColumn] = insp.get_columns(tname, schema=self.test_schema)
            id_col: ReflectedColumn = next(c for c in cols if c["name"].lower() == "id")
            assert id_col.autoincrement is True  # Not implemented yet
        finally:
            with self.sr_engine.begin() as conn:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {self._full_table_name(tname)}")

    @pytest.mark.xfail(reason="AUTO_INCREMENT reflection not implemented yet", strict=False)
    def test_reflects_autoincrement_false(self) -> None:
        tname = "reflect_columns_ai_false"
        with self.sr_engine.begin() as conn:
            conn.exec_driver_sql(
                f"""
                CREATE TABLE {self._full_table_name(tname)} (
                    id BIGINT NOT NULL,
                    v INT
                )
                DISTRIBUTED BY RANDOM
                PROPERTIES("replication_num"="1")
                """
            )
        try:
            insp: Inspector = inspect(self.sr_engine)
            cols: list[ReflectedColumn] = insp.get_columns(tname, schema=self.test_schema)
            id_col: ReflectedColumn = next(c for c in cols if c["name"].lower() == "id")
            assert id_col.autoincrement is False  # Not implemented yet
        finally:
            with self.sr_engine.begin() as conn:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {self._full_table_name(tname)}")
