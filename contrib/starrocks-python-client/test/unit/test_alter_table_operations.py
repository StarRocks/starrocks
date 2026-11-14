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

"""ALTER TABLE operation edge cases and error handling."""

from unittest.mock import patch

from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from sqlalchemy import testing
from sqlalchemy.testing import expect_raises_message, fixtures
from sqlalchemy.testing.assertions import AssertsCompiledSQL

from starrocks.alembic.ops import (
    AlterMaterializedViewOp,
    AlterTableDistributionOp,
    AlterTableEngineOp,
    AlterTableKeyOp,
    AlterTableOrderOp,
    AlterTablePartitionOp,
    AlterTablePropertiesOp,
    AlterViewOp,
    CreateMaterializedViewOp,
    CreateViewOp,
    DropMaterializedViewOp,
    DropViewOp,
)
from starrocks.dialect import StarRocksDialect
from starrocks.sql.ddl import AlterTableDistribution


class UnsupportedOperationsTest(fixtures.TestBase):
    """ALTER TABLE ENGINE, KEY and PARTITION BY raise ``NotImplementedError`` and log what was asked for."""

    @testing.fixture
    def op(self) -> Operations:
        return Operations(MigrationContext.configure(dialect=StarRocksDialect()))

    def test_alter_table_engine_not_supported(self, op):
        with patch("starrocks.alembic.toimpl.logger") as mock_logger:
            with expect_raises_message(NotImplementedError, "ALTER TABLE ENGINE is not yet supported"):
                op.alter_table_engine("test_table", "OLAP", schema="test_db")

            mock_logger.error.assert_called_once()
            error_call = mock_logger.error.call_args[0]
            assert "ALTER TABLE ENGINE is not currently supported" in error_call[0]
            assert "test_table" in error_call[1]
            assert "OLAP" in error_call[2]

    def test_alter_table_key_not_supported(self, op):
        with patch("starrocks.alembic.toimpl.logger") as mock_logger:
            with expect_raises_message(NotImplementedError, "ALTER TABLE KEY is not yet supported"):
                op.alter_table_key("test_table", "PRIMARY KEY", "id", schema="test_db")

            mock_logger.error.assert_called_once()
            error_call = mock_logger.error.call_args[0]
            assert "ALTER TABLE KEY is not currently supported" in error_call[0]
            assert "test_table" in error_call[1]
            assert "PRIMARY KEY" in error_call[2]
            assert "id" in error_call[3]

    def test_alter_table_partition_not_supported(self, op):
        with patch("starrocks.alembic.toimpl.logger") as mock_logger:
            with expect_raises_message(NotImplementedError, "ALTER TABLE PARTITION is not yet supported"):
                op.alter_table_partition("test_table", "RANGE(date_col)", schema="test_db")

            mock_logger.error.assert_called_once()
            error_call = mock_logger.error.call_args[0]
            assert "ALTER TABLE PARTITION is not currently supported" in error_call[0]
            assert "test_table" in error_call[1]
            assert "RANGE(date_col)" in error_call[2]


class DistributionBucketsTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = StarRocksDialect()

    @testing.combinations(
        ("none_omits_buckets", None, "ALTER TABLE test_table DISTRIBUTED BY HASH(id)"),
        ("zero_is_rendered", 0, "ALTER TABLE test_table DISTRIBUTED BY HASH(id) BUCKETS 0"),
        argnames="buckets, expected",
        id_="iaa",
    )
    def test_buckets_none_differs_from_zero(self, buckets, expected):
        self.assert_compile(AlterTableDistribution("test_table", "HASH(id)", buckets=buckets), expected)


class AlembicToDiffTupleTest(fixtures.TestBase):
    """The operations return Alembic autogenerate diff tuples instead of raising ``NotImplementedError``."""

    def test_alter_table_operations_diff_tuple(self):
        prop_op = AlterTablePropertiesOp("tbl", {"bloom_filter_columns": "c1"}, schema="sch")
        assert prop_op.to_diff_tuple() == ("alter_table_properties", "sch", "tbl", {"bloom_filter_columns": "c1"})

        engine_op = AlterTableEngineOp("tbl", "OLAP", schema="sch")
        assert engine_op.to_diff_tuple() == ("alter_table_engine", "sch", "tbl", "OLAP")

        key_op = AlterTableKeyOp("tbl", "PRIMARY KEY", "id", schema="sch")
        assert key_op.to_diff_tuple() == ("alter_table_key", "sch", "tbl", "PRIMARY KEY", "id")

        part_op = AlterTablePartitionOp("tbl", "RANGE(dt)", schema="sch")
        assert part_op.to_diff_tuple() == ("alter_table_partition", "sch", "tbl", "RANGE(dt)")

        dist_op = AlterTableDistributionOp("tbl", "HASH(id)", buckets=10, schema="sch")
        assert dist_op.to_diff_tuple() == ("alter_table_distribution", "sch", "tbl", "HASH(id)", 10)

        order_op = AlterTableOrderOp("tbl", "dt", schema="sch")
        assert order_op.to_diff_tuple() == ("alter_table_order", "sch", "tbl", "dt")

    def test_view_operations_diff_tuple(self):
        assert AlterViewOp("v1", "SELECT 1", schema="s1").to_diff_tuple() == ("alter_view", "s1", "v1")
        assert CreateViewOp("v1", "SELECT 1", schema="s1").to_diff_tuple() == ("create_view", "s1", "v1")
        assert DropViewOp("v1", schema="s1").to_diff_tuple() == ("drop_view", "s1", "v1")

        assert AlterMaterializedViewOp("mv1", schema="s1").to_diff_tuple() == ("alter_materialized_view", "s1", "mv1")
        assert CreateMaterializedViewOp("mv1", "SELECT 1", schema="s1").to_diff_tuple() == (
            "create_materialized_view",
            "s1",
            "mv1",
        )
        assert DropMaterializedViewOp("mv1", schema="s1").to_diff_tuple() == ("drop_materialized_view", "s1", "mv1")
