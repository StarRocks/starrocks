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

# test/test_alter_table_operations.py
"""
Tests for ALTER TABLE operation edge cases and error handling.
"""
import logging
from unittest.mock import patch

from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
import pytest

from starrocks.dialect import StarRocksDialect


logger = logging.getLogger(__name__)


class TestUnsupportedOperations:
    """Test error handling for unsupported ALTER TABLE operations."""

    @pytest.fixture
    def op(self) -> Operations:
        """Provide an Operations object for testing."""
        dialect = StarRocksDialect()
        context = MigrationContext.configure(dialect=dialect)
        return Operations(context)

    def test_alter_table_engine_not_supported(self, op: Operations):
        """Test that op.alter_table_engine raises NotImplementedError with proper logging."""
        with patch('starrocks.alembic.toimpl.logger') as mock_logger:
            with pytest.raises(NotImplementedError, match="ALTER TABLE ENGINE is not yet supported"):
                op.alter_table_engine("test_table", "OLAP", schema="test_db")

            # Verify error was logged
            mock_logger.error.assert_called_once()
            error_call = mock_logger.error.call_args[0]
            assert "ALTER TABLE ENGINE is not currently supported" in error_call[0]
            assert "test_table" in error_call[1]
            assert "OLAP" in error_call[2]

    def test_alter_table_key_not_supported(self, op: Operations):
        """Test that op.alter_table_key raises NotImplementedError with proper logging."""
        with patch('starrocks.alembic.toimpl.logger') as mock_logger:
            with pytest.raises(NotImplementedError, match="ALTER TABLE KEY is not yet supported"):
                op.alter_table_key("test_table", "PRIMARY KEY", "id", schema="test_db")

            # Verify error was logged
            mock_logger.error.assert_called_once()
            error_call = mock_logger.error.call_args[0]
            assert "ALTER TABLE KEY is not currently supported" in error_call[0]
            assert "test_table" in error_call[1]
            assert "PRIMARY KEY" in error_call[2]
            assert "id" in error_call[3]

    def test_alter_table_partition_not_supported(self, op: Operations):
        """Test that op.alter_table_partition raises NotImplementedError with proper logging."""
        with patch('starrocks.alembic.toimpl.logger') as mock_logger:
            with pytest.raises(NotImplementedError, match="ALTER TABLE PARTITION is not yet supported"):
                op.alter_table_partition("test_table", "RANGE(date_col)", schema="test_db")

            # Verify error was logged
            mock_logger.error.assert_called_once()
            error_call = mock_logger.error.call_args[0]
            assert "ALTER TABLE PARTITION is not currently supported" in error_call[0]
            assert "test_table" in error_call[1]
            assert "RANGE(date_col)" in error_call[2]


class TestBucketsParsingEdgeCases:
    """Test edge cases in BUCKETS parsing logic."""

    def test_buckets_none_vs_zero_difference(self):
        """Test that None buckets and 0 buckets are handled differently."""
        from starrocks.sql.ddl import AlterTableDistribution

        # None buckets - no BUCKETS clause
        ddl_none = AlterTableDistribution("test_table", "HASH(id)", buckets=None)

        # Zero buckets - BUCKETS 0 clause
        ddl_zero = AlterTableDistribution("test_table", "HASH(id)", buckets=0)

        assert ddl_none.buckets is None
        assert ddl_zero.buckets == 0
        assert ddl_none.buckets != ddl_zero.buckets


class TestPropertiesOrderPreservation:
    """Test that properties order is preserved for deterministic output."""

    def test_properties_order_preservation(self):
        """Test that properties order is preserved for deterministic output."""
        from starrocks.sql.ddl import AlterTableProperties

        # Python dicts maintain insertion order (Python 3.7+)
        properties = {
            "replication_num": "3",
            "storage_medium": "SSD",
            "dynamic_partition.enable": "true"
        }

        ddl = AlterTableProperties("test_table", properties)

        # Should maintain the same order
        keys = list(ddl.properties.keys())
        expected_keys = ["replication_num", "storage_medium", "dynamic_partition.enable"]
        assert keys == expected_keys
