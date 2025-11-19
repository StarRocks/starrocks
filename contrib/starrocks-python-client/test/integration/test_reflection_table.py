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

from sqlalchemy import Column, Integer, MetaData, String, Table, inspect, text
from sqlalchemy.engine import Engine

from starrocks.common.params import TableInfoKeyWithPrefix
from starrocks.engine.interfaces import ReflectedPartitionInfo


logger = logging.getLogger(__name__)


class TestReflectionTablesIntegration:
    """Integration tests for StarRocks table reflection from information_schema."""

    def test_reflect_table_options(self, sr_root_engine: Engine):
        """Test that `get_table_options` correctly reflects all StarRocks table options."""
        table_name = "test_reflect_table_options"
        metadata = MetaData()
        sr_engine = sr_root_engine

        # Define a table with various StarRocks-specific options
        table = Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(32)),
            comment="Test table with all StarRocks options",
            starrocks_engine='OLAP',
            starrocks_primary_key='id',
            starrocks_partition_by="""RANGE(id) (
                PARTITION p1 VALUES LESS THAN ("10"),
                PARTITION p2 VALUES LESS THAN ("20"),
                PARTITION p3 VALUES LESS THAN (MAXVALUE)
            )""",
            starrocks_distributed_by='HASH(id) BUCKETS 8',
            starrocks_order_by='id, name',
            starrocks_properties={"replication_num": "1", "storage_medium": "SSD"},
        )

        with sr_engine.connect() as connection:
            # Ensure the table is dropped before creation for a clean test environment
            connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            # Create the table in the test database
            table.create(connection)

            try:
                # Inspect the database to get table options
                inspector = inspect(sr_engine)
                table_options = inspector.get_table_options(table_name)
                logger.info("table_options: %s", table_options)

                from test.unit.test_utils import normalize_sql
                # Assertions for all expected StarRocks table options
                assert table_options[TableInfoKeyWithPrefix.ENGINE] == 'OLAP'
                assert normalize_sql(table_options[TableInfoKeyWithPrefix.PRIMARY_KEY]) == 'id'
                partition_info = table_options[TableInfoKeyWithPrefix.PARTITION_BY]
                assert partition_info is not None and isinstance(partition_info, ReflectedPartitionInfo)
                assert normalize_sql(partition_info.partition_method) == normalize_sql('RANGE(id)')
                assert "(PARTITION p1 VALUES" in normalize_sql(partition_info.pre_created_partitions)
                assert normalize_sql(table_options[TableInfoKeyWithPrefix.DISTRIBUTED_BY]) == normalize_sql('HASH(id) BUCKETS 8')
                assert normalize_sql(table_options[TableInfoKeyWithPrefix.ORDER_BY]) == normalize_sql("id, name")
                assert table_options[TableInfoKeyWithPrefix.PROPERTIES]['replication_num'] == '1'
                assert table_options[TableInfoKeyWithPrefix.PROPERTIES]['storage_medium'] == 'SSD'
                assert table_options[TableInfoKeyWithPrefix.COMMENT] == normalize_sql('Test table with all StarRocks options')

            finally:
                # Clean up: Drop the table after the test
                connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
