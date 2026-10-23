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

from sqlalchemy import Column, Integer, String, Table, inspect
from sqlalchemy.testing import fixtures

from starrocks.common.params import TableInfoKeyWithPrefix
from starrocks.common.types import SystemRunMode
from starrocks.engine.interfaces import ReflectedPartitionInfo
from test.util import normalize_sql


class ReflectionTableOptionsTest(fixtures.TestBase):
    """``get_table_options`` reflects every StarRocks table option from information_schema."""

    __backend__ = True
    __only_on__ = "starrocks"

    def test_reflect_table_options(self, connection, metadata):
        Table(
            "test_reflect_table_options",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(32)),
            comment="Test table with all StarRocks options",
            starrocks_engine="OLAP",
            starrocks_primary_key="id",
            starrocks_partition_by="""RANGE(id) (
                PARTITION p1 VALUES LESS THAN ("10"),
                PARTITION p2 VALUES LESS THAN ("20"),
                PARTITION p3 VALUES LESS THAN (MAXVALUE)
            )""",
            starrocks_distributed_by="HASH(id) BUCKETS 8",
            starrocks_order_by="id, name",
            starrocks_properties={"replication_num": "1", "storage_medium": "SSD"},
        )
        metadata.create_all(connection)

        table_options = inspect(connection).get_table_options("test_reflect_table_options")

        assert table_options[TableInfoKeyWithPrefix.ENGINE] == "OLAP"
        assert normalize_sql(table_options[TableInfoKeyWithPrefix.PRIMARY_KEY]) == "id"
        partition_info = table_options[TableInfoKeyWithPrefix.PARTITION_BY]
        assert isinstance(partition_info, ReflectedPartitionInfo)
        assert normalize_sql(partition_info.partition_method) == normalize_sql("RANGE(id)")
        assert "(PARTITION p1 VALUES" in normalize_sql(partition_info.pre_created_partitions)
        assert normalize_sql(table_options[TableInfoKeyWithPrefix.DISTRIBUTED_BY]) == normalize_sql(
            "HASH(id) BUCKETS 8"
        )
        assert normalize_sql(table_options[TableInfoKeyWithPrefix.ORDER_BY]) == normalize_sql("id, name")
        properties = table_options[TableInfoKeyWithPrefix.PROPERTIES]
        assert properties["replication_num"] == "1"
        if connection.dialect.run_mode == SystemRunMode.SHARED_DATA:
            assert properties.get("storage_medium", "HDD") == "HDD"
        else:
            assert properties["storage_medium"] == "SSD"
        assert table_options[TableInfoKeyWithPrefix.COMMENT] == "Test table with all StarRocks options"
