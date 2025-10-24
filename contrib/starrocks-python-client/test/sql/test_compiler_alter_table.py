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

# test/sql/test_compiler_alter_table.py
"""
DDL Compiler tests for StarRocks ALTER TABLE statements.

Tests the compilation of ALTER TABLE DDL objects into SQL strings.
Based on StarRocks documentation and grammar.
"""
import logging
from unittest.mock import Mock, patch

from alembic.operations import Operations

from starrocks.alembic.ops import (
    AlterTableDistributionOp,
    AlterTableOrderOp,
    AlterTablePropertiesOp,
)
from starrocks.dialect import StarRocksDDLCompiler, StarRocksDialect, StarRocksIdentifierPreparer
from starrocks.sql.ddl import (
    AlterTableDistribution,
    AlterTableKey,
    AlterTableOrder,
    AlterTablePartition,
    AlterTableProperties,
)


logger = logging.getLogger(__name__)


class TestSQLGeneration:
    """Test end-to-end SQL generation for ALTER TABLE operations."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dialect = StarRocksDialect()
        self.identifier_parser_with_quotes_func = StarRocksIdentifierPreparer._requires_quotes is not None
        if self.identifier_parser_with_quotes_func:
            def need_quotes(self, ident):
                return True
            setattr(StarRocksIdentifierPreparer, '_requires_quotes', need_quotes)
        self.compiler = StarRocksDDLCompiler(self.dialect, None)
        self.operations = Mock(spec=Operations)

    def teardown_method(self):
        if self.identifier_parser_with_quotes_func:
            delattr(StarRocksIdentifierPreparer, '_requires_quotes')

    def test_alter_table_properties_sql(self):
        """Test SQL generation for ALTER TABLE SET (properties)."""
        # Test various property combinations based on StarRocks documentation
        test_cases = [
            # Single property
            (
                {"replication_num": "3"},
                'ALTER TABLE `test_table` SET ("replication_num" = "3")'
            ),
            # Multiple properties
            (
                {"replication_num": "3", "storage_medium": "SSD"},
                # 'ALTER TABLE `test_table` SET ("replication_num" = "3", "storage_medium" = "SSD")'
                'ALTER TABLE `test_table` SET ("replication_num" = "3"); '
                'ALTER TABLE `test_table` SET ("storage_medium" = "SSD")'
            ),
            # Dynamic partition properties
            (
                {
                    "dynamic_partition.enable": "true",
                    "dynamic_partition.time_unit": "DAY",
                    "dynamic_partition.start": "-7",
                    "dynamic_partition.end": "3"
                },
                ('ALTER TABLE `test_table` SET ("dynamic_partition.enable" = "true"); '
                 'ALTER TABLE `test_table` SET ("dynamic_partition.time_unit" = "DAY"); '
                 'ALTER TABLE `test_table` SET ("dynamic_partition.start" = "-7"); '
                 'ALTER TABLE `test_table` SET ("dynamic_partition.end" = "3")')
            ),
            # Bloom filter properties
            (
                {"bloom_filter_columns": "k1,k2,k3"},
                'ALTER TABLE `test_table` SET ("bloom_filter_columns" = "k1,k2,k3")'
            ),
            # Colocate properties
            (
                {"colocate_with": "group1"},
                'ALTER TABLE `test_table` SET ("colocate_with" = "group1")'
            ),
        ]

        for properties, expected_sql in test_cases:
            with patch('starrocks.dialect.format_table_name') as mock_format:
                mock_format.return_value = "`test_table`"

                ddl = AlterTableProperties("test_table", properties)

                result = self.compiler.visit_alter_table_properties(ddl)
                assert result == expected_sql

    def test_alter_table_distribution_sql(self):
        """Test SQL generation for ALTER TABLE DISTRIBUTED BY."""
        test_cases = [
            # Hash distribution without buckets
            (
                "HASH(id)", None,
                "ALTER TABLE `users` DISTRIBUTED BY HASH(id)"
            ),
            # Hash distribution with buckets
            (
                "HASH(user_id)", 32,
                "ALTER TABLE `users` DISTRIBUTED BY HASH(user_id) BUCKETS 32"
            ),
            # Multiple column hash
            (
                "HASH(tenant_id, user_id)", 16,
                "ALTER TABLE `users` DISTRIBUTED BY HASH(tenant_id, user_id) BUCKETS 16"
            ),
            # Random distribution
            (
                "RANDOM", 10,
                "ALTER TABLE `users` DISTRIBUTED BY RANDOM BUCKETS 10"
            ),
        ]

        for distributed_by, buckets, expected_sql in test_cases:
            with patch('starrocks.dialect.format_table_name') as mock_format:
                mock_format.return_value = "`users`"

                ddl = AlterTableDistribution("users", distributed_by, buckets)

                result = self.compiler.visit_alter_table_distribution(ddl)
                assert result == expected_sql

    def test_alter_table_order_sql(self):
        """Test SQL generation for ALTER TABLE ORDER BY."""
        test_cases = [
            # Single column
            (
                "id",
                "ALTER TABLE `events` ORDER BY (id)"
            ),
            # Multiple columns
            (
                "timestamp, event_id",
                "ALTER TABLE `events` ORDER BY (timestamp, event_id)"
            ),
            # With ASC/DESC
            (
                "timestamp DESC, event_id ASC",
                "ALTER TABLE `events` ORDER BY (timestamp DESC, event_id ASC)"
            ),
            # Complex ordering
            (
                "created_at DESC, updated_at ASC, id",
                "ALTER TABLE `events` ORDER BY (created_at DESC, updated_at ASC, id)"
            ),
        ]

        for order_by, expected_sql in test_cases:
            with patch('starrocks.dialect.format_table_name') as mock_format:
                mock_format.return_value = "`events`"

                ddl = AlterTableOrder("events", order_by)

                result = self.compiler.visit_alter_table_order(ddl)
                assert result == expected_sql

    def test_schema_qualified_table_names(self):
        """Test SQL generation with schema-qualified table names."""
        test_cases = [
            # Properties with schema
            (
                lambda: self._create_properties_ddl_with_schema(),
                "ALTER TABLE `test_db`.`users` SET (\"replication_num\" = \"3\")"
            ),
            # Distribution with schema
            (
                lambda: self._create_distribution_ddl_with_schema(),
                "ALTER TABLE `analytics`.`events` DISTRIBUTED BY HASH(user_id) BUCKETS 16"
            ),
            # Order with schema
            (
                lambda: self._create_order_ddl_with_schema(),
                "ALTER TABLE `shop`.`orders` ORDER BY (created_at DESC, id ASC)"
            ),
        ]

        for ddl_factory, expected_sql in test_cases:
            with patch('starrocks.dialect.format_table_name') as mock_format:
                # Mock different schema.table combinations
                if "test_db" in expected_sql:
                    mock_format.return_value = "`test_db`.`users`"
                elif "analytics" in expected_sql:
                    mock_format.return_value = "`analytics`.`events`"
                elif "shop" in expected_sql:
                    mock_format.return_value = "`shop`.`orders`"

                ddl = ddl_factory()

                if hasattr(ddl, 'properties'):
                    result = self.compiler.visit_alter_table_properties(ddl)
                elif hasattr(ddl, 'distribution_method'):
                    result = self.compiler.visit_alter_table_distribution(ddl)
                elif hasattr(ddl, 'order_by'):
                    result = self.compiler.visit_alter_table_order(ddl)

                assert result == expected_sql

    def _create_properties_ddl_with_schema(self):
        return AlterTableProperties("users", {"replication_num": "3"}, schema="test_db")

    def _create_distribution_ddl_with_schema(self):
        return AlterTableDistribution("events", "HASH(user_id)", buckets=16, schema="analytics")

    def _create_order_ddl_with_schema(self):
        return AlterTableOrder("orders", "created_at DESC, id ASC", schema="shop")


class TestOperationToSQLFlow:
    """Test the complete flow from Operation to SQL execution."""

    def setup_method(self):
        """Set up test fixtures."""
        self.operations = Mock(spec=Operations)

    def test_properties_operation_to_sql(self):
        """Test complete flow for properties operation."""
        # Create operation
        op = AlterTablePropertiesOp(
            table_name="users",
            properties={"replication_num": "3", "storage_medium": "SSD"},
            schema="test_db"
        )

        # Test that implementation creates correct DDL
        from starrocks.alembic.toimpl import alter_table_properties

        with patch.object(self.operations, 'execute') as mock_execute:
            alter_table_properties(self.operations, op)

            # Verify DDL was created and executed
            mock_execute.assert_called_once()
            ddl_arg = mock_execute.call_args[0][0]

            assert ddl_arg.table_name == "users"
            assert ddl_arg.schema == "test_db"
            assert ddl_arg.properties == {"replication_num": "3", "storage_medium": "SSD"}

    def test_distribution_operation_to_sql(self):
        """Test complete flow for distribution operation."""
        # Create operation
        op = AlterTableDistributionOp(
            table_name="orders",
            distribution_method="HASH(user_id)",
            buckets=32,
            schema="shop_db"
        )

        # Test that implementation creates correct DDL
        from starrocks.alembic.toimpl import alter_table_distribution

        with patch.object(self.operations, 'execute') as mock_execute:
            alter_table_distribution(self.operations, op)

            # Verify DDL was created and executed
            mock_execute.assert_called_once()
            ddl_arg = mock_execute.call_args[0][0]

            assert ddl_arg.table_name == "orders"
            assert ddl_arg.schema == "shop_db"
            assert ddl_arg.distribution_method == "HASH(user_id)"
            assert ddl_arg.buckets == 32

    def test_order_operation_to_sql(self):
        """Test complete flow for order operation."""
        # Create operation
        op = AlterTableOrderOp(
            table_name="events",
            order_by="timestamp DESC, event_id ASC",
            schema="analytics_db"
        )

        # Test that implementation creates correct DDL
        from starrocks.alembic.toimpl import alter_table_order

        with patch.object(self.operations, 'execute') as mock_execute:
            alter_table_order(self.operations, op)

            # Verify DDL was created and executed
            mock_execute.assert_called_once()
            ddl_arg = mock_execute.call_args[0][0]

            assert ddl_arg.table_name == "events"
            assert ddl_arg.schema == "analytics_db"
            assert ddl_arg.order_by == "timestamp DESC, event_id ASC"


class TestStarRocksDocumentationExamples:
    """Test examples from StarRocks documentation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dialect = StarRocksDialect()
        self.compiler = StarRocksDDLCompiler(self.dialect, None)

    def test_dynamic_partition_example(self):
        """Test dynamic partition properties from documentation."""
        # Based on https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE/
        properties = {
            "storage_medium": "SSD",
            "dynamic_partition.enable": "true",
            "dynamic_partition.time_unit": "DAY",
            "dynamic_partition.start": "-3",
            "dynamic_partition.end": "3",
            "dynamic_partition.prefix": "p",
            "dynamic_partition.buckets": "10"
        }

        with patch('starrocks.dialect.format_table_name') as mock_format:
            mock_format.return_value = "`example_db`.`dynamic_partition`"

            ddl = AlterTableProperties("dynamic_partition", properties, schema="example_db")

            result = self.compiler.visit_alter_table_properties(ddl)

            # Should contain all properties
            assert "ALTER TABLE `example_db`.`dynamic_partition` SET (" in result
            assert '"storage_medium" = "SSD"' in result
            assert '"dynamic_partition.enable" = "true"' in result
            assert '"dynamic_partition.time_unit" = "DAY"' in result

    def test_colocate_join_example(self):
        """Test colocate join properties from documentation."""
        properties = {"colocate_with": "t1"}

        with patch('starrocks.dialect.format_table_name') as mock_format:
            mock_format.return_value = "`t2`"

            ddl = AlterTableProperties("t2", properties)

            result = self.compiler.visit_alter_table_properties(ddl)
            expected = 'ALTER TABLE `t2` SET ("colocate_with" = "t1")'

            assert result == expected

    def test_primary_key_table_example(self):
        """Test ORDER BY from Primary Key table example in documentation."""
        # From: Primary Key table with specific sort key example
        order_by = "address, last_active"

        with patch('starrocks.dialect.format_table_name') as mock_format:
            mock_format.return_value = "`users`"

            ddl = AlterTableOrder("users", order_by)

            result = self.compiler.visit_alter_table_order(ddl)
            expected = "ALTER TABLE `users` ORDER BY (address, last_active)"

            assert result == expected


class TestErrorCases:
    """Test error handling and edge cases."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dialect = StarRocksDialect()
        self.compiler = StarRocksDDLCompiler(self.dialect, None)

    def test_empty_properties(self):
        """Test handling of empty properties."""
        with patch('starrocks.dialect.format_table_name') as mock_format:
            mock_format.return_value = "`test_table`"

            ddl = AlterTableProperties("test_table", {})

            result = self.compiler.visit_alter_table_properties(ddl)
            expected = ""

            assert result == expected

    def test_special_characters_in_properties(self):
        """Test handling of special characters in property values."""
        properties = {
            "comment": "Table with 'quotes' and \"double quotes\"",
            "path": "/data/warehouse/table_name"
        }

        with patch('starrocks.dialect.format_table_name') as mock_format:
            mock_format.return_value = "`test_table`"

            ddl = AlterTableProperties("test_table", properties)

            result = self.compiler.visit_alter_table_properties(ddl)

            # Should handle special characters (though proper escaping would be better)
            assert "ALTER TABLE `test_table` SET (" in result
            assert '"comment"' in result
            assert '"path"' in result


class TestSpecialCharacterHandling:
    """Test special character handling in SQL generation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dialect = StarRocksDialect()
        self.compiler = StarRocksDDLCompiler(self.dialect, None)

    def test_properties_with_quotes(self):
        """Test properties with single and double quotes."""
        test_cases = [
            # Single quotes in value
            (
                {"description": "table with 'single quotes'"},
                'ALTER TABLE `test_table` SET ("description" = "table with \'single quotes\'")'
            ),
            # Double quotes in value
            (
                {"comment": 'table with "double quotes"'},
                'ALTER TABLE `test_table` SET ("comment" = "table with \\"double quotes\\"")'
            ),
            # Mixed quotes
            (
                {"info": "table with 'single' and \"double\" quotes"},
                ('ALTER TABLE `test_table` SET ("info" = '
                 '"table with \'single\' and \\"double\\" quotes")')
            ),
        ]

        for properties, expected in test_cases:
            with patch('starrocks.dialect.format_table_name') as mock_format:
                mock_format.return_value = "`test_table`"

                ddl = AlterTableProperties("test_table", properties)

                result = self.compiler.visit_alter_table_properties(ddl)
                logger.info(f"Actual result: {result!r}")

                assert result == expected

    def test_properties_with_backslashes(self):
        """Test properties with backslashes and escape characters."""
        properties = {
            "file_path": "C:\\temp\\data\\file.txt",
            "regex_pattern": "\\d+\\.\\d+",
            "json_config": '{"key": "value\\nwith\\nnewlines"}'
        }

        with patch('starrocks.dialect.format_table_name') as mock_format:
            mock_format.return_value = "`test_table`"

            ddl = AlterTableProperties("test_table", properties)

            result = self.compiler.visit_alter_table_properties(ddl)

            # Should contain all property keys
            assert '"file_path"' in result
            assert '"regex_pattern"' in result
            assert '"json_config"' in result
            assert "ALTER TABLE `test_table` SET (" in result

    def test_table_name_with_special_characters(self):
        """Test table names with special characters and keywords."""
        test_cases = [
            # Table name with backticks
            ("table`with`backticks", "`table``with``backticks`"),
            # SQL keywords as table names
            ("order", "`order`"),
            ("select", "`select`"),
            ("table", "`table`"),
            # Table with schema
            ("my_schema.my_table", "`my_schema`.`my_table`"),
        ]

        for table_name, expected_formatted in test_cases:
            with patch('starrocks.dialect.format_table_name') as mock_format:
                mock_format.return_value = expected_formatted

                ddl = AlterTableProperties(table_name, {"test": "value"})

                result = self.compiler.visit_alter_table_properties(ddl)
                assert result.startswith(f"ALTER TABLE {expected_formatted} SET")

    def test_distribution_with_special_characters(self):
        """Test DISTRIBUTED BY with special column names."""
        test_cases = [
            # Column names with backticks
            ("HASH(`order`, `select`)", None, "ALTER TABLE `test_table` DISTRIBUTED BY HASH(`order`, `select`)"),
            # Column names with spaces (should be quoted)
            ("HASH(`user id`, `created at`)", 10,
             "ALTER TABLE `test_table` DISTRIBUTED BY HASH(`user id`, `created at`) BUCKETS 10"),
            # Expression with functions
            ("HASH(substr(`name`, 1, 3))", 8,
             "ALTER TABLE `test_table` DISTRIBUTED BY HASH(substr(`name`, 1, 3)) BUCKETS 8"),
        ]

        for distributed_by, buckets, expected in test_cases:
            with patch('starrocks.dialect.format_table_name') as mock_format:
                mock_format.return_value = "`test_table`"

                ddl = AlterTableDistribution("test_table", distributed_by, buckets=buckets)

                result = self.compiler.visit_alter_table_distribution(ddl)
                assert result == expected

    def test_order_by_with_special_characters(self):
        """Test ORDER BY with special column names and expressions."""
        test_cases = [
            # Column names with backticks
            ("`order` ASC, `select` DESC", "ALTER TABLE `test_table` ORDER BY (`order` ASC, `select` DESC)"),
            # Column names with spaces
            ("`user id`, `created at` DESC", "ALTER TABLE `test_table` ORDER BY (`user id`, `created at` DESC)"),
            # Expressions with functions
            ("date_format(`created_at`, '%Y-%m') DESC, `id`",
             "ALTER TABLE `test_table` ORDER BY (date_format(`created_at`, '%Y-%m') DESC, `id`)"),
        ]

        for order_by, expected in test_cases:
            with patch('starrocks.dialect.format_table_name') as mock_format:
                mock_format.return_value = "`test_table`"

                ddl = AlterTableOrder("test_table", order_by)

                result = self.compiler.visit_alter_table_order(ddl)
                assert result == expected


class TestSQLSyntaxVariations:
    """Test different SQL syntax variations for StarRocks."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dialect = StarRocksDialect()
        self.compiler = StarRocksDDLCompiler(self.dialect, None)

    def test_distribution_strategies(self):
        """Test different distribution strategies."""
        test_cases = [
            # Hash distribution variations
            ("HASH(id)", None, "ALTER TABLE `test_table` DISTRIBUTED BY HASH(id)"),
            ("HASH(id)", 16, "ALTER TABLE `test_table` DISTRIBUTED BY HASH(id) BUCKETS 16"),
            ("HASH(user_id, tenant_id)", 32,
             "ALTER TABLE `test_table` DISTRIBUTED BY HASH(user_id, tenant_id) BUCKETS 32"),

            # Random distribution
            ("RANDOM", 10, "ALTER TABLE `test_table` DISTRIBUTED BY RANDOM BUCKETS 10"),
            ("RANDOM", None, "ALTER TABLE `test_table` DISTRIBUTED BY RANDOM"),

            # Expression-based distribution
            ("HASH(substr(name, 1, 3))", 8,
             "ALTER TABLE `test_table` DISTRIBUTED BY HASH(substr(name, 1, 3)) BUCKETS 8"),
            ("HASH(date_format(created_at, '%Y-%m'))", 12,
             "ALTER TABLE `test_table` DISTRIBUTED BY HASH(date_format(created_at, '%Y-%m')) BUCKETS 12"),
        ]

        for distributed_by, buckets, expected in test_cases:
            with patch('starrocks.dialect.format_table_name') as mock_format:
                mock_format.return_value = "`test_table`"

                ddl = AlterTableDistribution("test_table", distributed_by, buckets=buckets)

                result = self.compiler.visit_alter_table_distribution(ddl)
                assert result == expected

    def test_order_by_variations(self):
        """Test different ORDER BY syntax variations."""
        test_cases = [
            # Single column
            ("id", "ALTER TABLE `test_table` ORDER BY (id)"),
            ("id ASC", "ALTER TABLE `test_table` ORDER BY (id ASC)"),
            ("id DESC", "ALTER TABLE `test_table` ORDER BY (id DESC)"),

            # Multiple columns
            ("id, name", "ALTER TABLE `test_table` ORDER BY (id, name)"),
            ("id ASC, name DESC", "ALTER TABLE `test_table` ORDER BY (id ASC, name DESC)"),
            ("created_at DESC, id ASC, name",
             "ALTER TABLE `test_table` ORDER BY (created_at DESC, id ASC, name)"),

            # Expression-based ordering
            ("date_format(created_at, '%Y-%m') DESC",
             "ALTER TABLE `test_table` ORDER BY (date_format(created_at, '%Y-%m') DESC)"),
            ("substr(name, 1, 1), id DESC",
             "ALTER TABLE `test_table` ORDER BY (substr(name, 1, 1), id DESC)"),
            ("CASE WHEN status = 'active' THEN 1 ELSE 2 END, created_at DESC",
             "ALTER TABLE `test_table` ORDER BY (CASE WHEN status = 'active' THEN 1 ELSE 2 END, created_at DESC)"),
        ]

        for order_by, expected in test_cases:
            with patch('starrocks.dialect.format_table_name') as mock_format:
                mock_format.return_value = "`test_table`"

                ddl = AlterTableOrder("test_table", order_by)

                result = self.compiler.visit_alter_table_order(ddl)
                assert result == expected

    def test_properties_data_types(self):
        """Test different property value data types."""
        test_cases = [
            # String values
            ({"storage_medium": "SSD"}, '"storage_medium" = "SSD"'),
            ({"description": "test table"}, '"description" = "test table"'),

            # Numeric values (stored as strings)
            ({"replication_num": "3"}, '"replication_num" = "3"'),
            ({"buckets": "16"}, '"buckets" = "16"'),

            # Boolean values (stored as strings)
            ({"dynamic_partition.enable": "true"}, '"dynamic_partition.enable" = "true"'),
            ({"fast_schema_evolution": "false"}, '"fast_schema_evolution" = "false"'),

            # Date/time values
            ({"storage_cooldown_time": "2023-12-01 12:00:00"},
             '"storage_cooldown_time" = "2023-12-01 12:00:00"'),

            # JSON-like values
            ({"bloom_filter_columns": "col1,col2,col3"},
             '"bloom_filter_columns" = "col1,col2,col3"'),

            # Complex properties
            ({
                "dynamic_partition.enable": "true",
                "dynamic_partition.time_unit": "DAY",
                "dynamic_partition.start": "-3",
                "dynamic_partition.end": "3"
            }, None),  # We'll check if all keys are present
        ]

        for properties, expected_fragment in test_cases:
            with patch('starrocks.dialect.format_table_name') as mock_format:
                mock_format.return_value = "`test_table`"

                ddl = AlterTableProperties("test_table", properties)

                result = self.compiler.visit_alter_table_properties(ddl)

                if expected_fragment:
                    assert expected_fragment in result
                else:
                    # For complex properties, check all keys are present
                    for key in properties.keys():
                        assert f'"{key}"' in result

    def test_starrocks_specific_properties(self):
        """Test StarRocks-specific property patterns."""
        test_cases = [
            # Storage properties
            ({
                "storage_medium": "SSD",
                "storage_cooldown_time": "2023-12-01 12:00:00"
            }),

            # Replication properties
            ({
                "replication_num": "3",
                "replication_allocation": "tag.location.default: 3"
            }),

            # Bloom filter properties
            ({
                "bloom_filter_columns": "k1,k2,k3",
                "bloom_filter_fpp": "0.05"
            }),

            # Dynamic partition properties
            ({
                "dynamic_partition.enable": "true",
                "dynamic_partition.time_unit": "DAY",
                "dynamic_partition.time_zone": "Asia/Shanghai",
                "dynamic_partition.start": "-3",
                "dynamic_partition.end": "3",
                "dynamic_partition.prefix": "p",
                "dynamic_partition.buckets": "10"
            }),

            # Colocate properties
            ({
                "colocate_with": "group1"
            }),

            # Compression properties
            ({
                "compression": "LZ4_FRAME"
            }),

            # Write quorum properties
            ({
                "write_quorum": "MAJORITY"
            }),
        ]

        for properties in test_cases:
            with patch('starrocks.dialect.format_table_name') as mock_format:
                mock_format.return_value = "`test_table`"

                ddl = AlterTableProperties("test_table", properties)

                result = self.compiler.visit_alter_table_properties(ddl)

                # Basic structure check
                assert result.startswith("ALTER TABLE `test_table` SET (")
                assert result.endswith(")")

                # All properties should be present
                for key, value in properties.items():
                    assert f'"{key}" = "{value}"' in result


class TestPartitionSyntaxVariations:
    """Test different partition syntax variations (for future support)."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dialect = StarRocksDialect()
        self.compiler = StarRocksDDLCompiler(self.dialect, None)

    def test_range_partition_patterns(self):
        """Test RANGE partition patterns that might be supported."""
        # Note: These tests document expected syntax even though
        # ALTER TABLE PARTITION is not yet implemented
        partition_patterns = [
            "RANGE(date_col)",
            "RANGE(date_format(created_at, '%Y-%m'))",
            "RANGE(COLUMNS(year, month))",
            "RANGE(substr(id, 1, 4))",
        ]

        for pattern in partition_patterns:
            ddl = AlterTablePartition("test_table", pattern)

            # Verify DDL object creation
            assert ddl.table_name == "test_table"
            assert ddl.partition_by == pattern
            assert ddl.__visit_name__ == "alter_table_partition"

    def test_list_partition_patterns(self):
        """Test LIST partition patterns that might be supported."""
        partition_patterns = [
            "LIST(category)",
            "LIST(status)",
            "LIST(COLUMNS(region, status))",
        ]

        for pattern in partition_patterns:
            ddl = AlterTablePartition("test_table", pattern)

            assert ddl.table_name == "test_table"
            assert ddl.partition_by == pattern


class TestKeyTypeSyntaxVariations:
    """Test different key type syntax variations (for future support)."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dialect = StarRocksDialect()
        self.compiler = StarRocksDDLCompiler(self.dialect, None)

    def test_key_type_patterns(self):
        """Test different key type patterns that might be supported."""
        key_patterns = [
            # Primary key variations
            ("PRIMARY", "id"),
            ("PRIMARY", "user_id, tenant_id"),

            # Unique key variations
            ("UNIQUE", "email"),
            ("UNIQUE", "username, email"),

            # Duplicate key variations
            ("DUPLICATE", "user_id, created_at"),
            ("DUPLICATE", "category_id, subcategory_id, created_at"),

            # Aggregate key variations
            ("AGGREGATE", "category_id, date_col"),
            ("AGGREGATE", "region, category, date_trunc('day', created_at)"),
        ]

        for key_type, key_columns in key_patterns:
            ddl = AlterTableKey("test_table", key_type, key_columns)

            assert ddl.table_name == "test_table"
            assert ddl.key_type == key_type
            assert ddl.key_columns == key_columns
            assert ddl.__visit_name__ == "alter_table_key"
