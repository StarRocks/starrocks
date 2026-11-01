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

"""ALTER TABLE compilation for StarRocks table attributes: PROPERTIES, DISTRIBUTED BY, ORDER BY, PARTITION BY, KEY.

The Alembic operations that emit these statements are checked against the SQL they produce with ``op_fixture``.
"""

from alembic import op
from alembic.testing.fixtures import op_fixture
from sqlalchemy import exc, testing
from sqlalchemy.testing import expect_raises_message, fixtures
from sqlalchemy.testing.assertions import AssertsCompiledSQL

import starrocks.alembic  # noqa: F401
from starrocks.dialect import StarRocksDialect
from starrocks.sql.ddl import (
    AlterTableDistribution,
    AlterTableKey,
    AlterTableOrder,
    AlterTablePartition,
    AlterTableProperties,
)


BACKSLASH_VALUES = (
    ("file_path", r"C:\temp\data\file.txt"),
    ("regex_pattern", r"\d+\.\d+"),
    ("json_config", r'{"key": "value\nwith\nnewlines"}'),
)


class AlterTablePropertiesCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    """StarRocks accepts one property per ``ALTER TABLE ... SET``, so each property becomes its own statement."""

    __dialect__ = StarRocksDialect()

    @testing.combinations(
        ("single", {"replication_num": "3"}, 'ALTER TABLE test_table SET ("replication_num" = "3")'),
        (
            "multiple",
            {"replication_num": "3", "storage_medium": "SSD"},
            'ALTER TABLE test_table SET ("replication_num" = "3"); '
            'ALTER TABLE test_table SET ("storage_medium" = "SSD")',
        ),
        (
            "dynamic_partition",
            {
                "dynamic_partition.enable": "true",
                "dynamic_partition.time_unit": "DAY",
                "dynamic_partition.start": "-7",
                "dynamic_partition.end": "3",
            },
            'ALTER TABLE test_table SET ("dynamic_partition.enable" = "true", '
            '"dynamic_partition.time_unit" = "DAY", "dynamic_partition.start" = "-7", '
            '"dynamic_partition.end" = "3")',
        ),
        (
            "binlog",
            {"binlog_enable": "true", "binlog_ttl_second": "3600"},
            'ALTER TABLE test_table SET ("binlog_enable" = "true", "binlog_ttl_second" = "3600")',
        ),
        (
            "grouped_and_single",
            {"replication_num": "1", "dynamic_partition.start": "-7", "in_memory": "false", "dynamic_partition.end": "3"},
            'ALTER TABLE test_table SET ("replication_num" = "1"); '
            'ALTER TABLE test_table SET ("dynamic_partition.start" = "-7", "dynamic_partition.end" = "3"); '
            'ALTER TABLE test_table SET ("in_memory" = "false")',
        ),
        (
            "bloom_filter",
            {"bloom_filter_columns": "k1,k2,k3"},
            'ALTER TABLE test_table SET ("bloom_filter_columns" = "k1,k2,k3")',
        ),
        ("colocate", {"colocate_with": "group1"}, 'ALTER TABLE test_table SET ("colocate_with" = "group1")'),
        (
            "datetime_value",
            {"storage_cooldown_time": "2023-12-01 12:00:00"},
            'ALTER TABLE test_table SET ("storage_cooldown_time" = "2023-12-01 12:00:00")',
        ),
        ("no_properties", {}, ""),
        argnames="properties, expected",
        id_="iaa",
    )
    def test_alter_table_properties(self, properties, expected):
        self.assert_compile(AlterTableProperties("test_table", properties), expected)

    @testing.combinations(
        ("single_quotes", "table with 'single quotes'", "\"table with 'single quotes'\""),
        ("double_quotes", 'table with "double quotes"', '"table with \\"double quotes\\""'),
        (
            "mixed_quotes",
            "table with 'single' and \"double\" quotes",
            '"table with \'single\' and \\"double\\" quotes"',
        ),
        argnames="value, rendered_value",
        id_="iaa",
    )
    def test_property_value_quotes(self, value, rendered_value):
        """Double quotes in a value are backslash-escaped; single quotes are left as they are."""
        self.assert_compile(
            AlterTableProperties("test_table", {"comment": value}),
            f'ALTER TABLE test_table SET ("comment" = {rendered_value})',
        )

    def test_property_without_a_value_is_refused(self):
        with expect_raises_message(exc.CompileError, "Property 'colocate_with' has no value"):
            AlterTableProperties("test_table", {"colocate_with": None}).compile(dialect=StarRocksDialect())

    @testing.combinations(
        ("file_path", r'"C:\\temp\\data\\file.txt"'),
        ("regex_pattern", r'"\\d+\\.\\d+"'),
        ("json_config", r'"{\"key\": \"value\\nwith\\nnewlines\"}"'),
        argnames="name, rendered_value",
        id_="sa",
    )
    def test_property_value_backslashes(self, name, rendered_value):
        """A backslash is doubled before double quotes are escaped, so an escaped quote keeps its one backslash."""
        self.assert_compile(
            AlterTableProperties("test_table", {name: dict(BACKSLASH_VALUES)[name]}),
            f'ALTER TABLE test_table SET ("{name}" = {rendered_value})',
        )

    @testing.combinations(
        ("embedded_backticks", "table`with`backticks", None, "`table``with``backticks`"),
        ("reserved_word_order", "order", None, "`order`"),
        ("reserved_word_select", "select", None, "`select`"),
        ("reserved_word_table", "table", None, "`table`"),
        ("dot_in_name", "my_schema.my_table", None, "`my_schema.my_table`"),
        ("schema", "my_table", "my_schema", "my_schema.my_table"),
        ("schema_needing_quotes", "my_table", "my schema", "`my schema`.my_table"),
        argnames="table_name, schema, rendered_name",
        id_="iaaa",
    )
    def test_table_name_quoting(self, table_name, schema, rendered_name):
        """A dot inside a table name is part of the name, so it is quoted rather than read as a schema."""
        self.assert_compile(
            AlterTableProperties(table_name, {"test": "value"}, schema=schema),
            f'ALTER TABLE {rendered_name} SET ("test" = "value")',
        )


class AlterTableDistributionCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = StarRocksDialect()

    @testing.combinations(
        ("hash", "HASH(id)", None, "ALTER TABLE test_table DISTRIBUTED BY HASH(id)"),
        ("hash_buckets", "HASH(id)", 16, "ALTER TABLE test_table DISTRIBUTED BY HASH(id) BUCKETS 16"),
        (
            "hash_multiple_columns",
            "HASH(tenant_id, user_id)",
            16,
            "ALTER TABLE test_table DISTRIBUTED BY HASH(tenant_id, user_id) BUCKETS 16",
        ),
        ("random", "RANDOM", None, "ALTER TABLE test_table DISTRIBUTED BY RANDOM"),
        ("random_buckets", "RANDOM", 10, "ALTER TABLE test_table DISTRIBUTED BY RANDOM BUCKETS 10"),
        (
            "hash_mixed_case_columns",
            "HASH(TenantId, user_id)",
            8,
            "ALTER TABLE test_table DISTRIBUTED BY HASH(TenantId, user_id) BUCKETS 8",
        ),
        (
            "hash_column_with_percent",
            "HASH(`p%ct`)",
            12,
            "ALTER TABLE test_table DISTRIBUTED BY HASH(`p%%ct`) BUCKETS 12",
        ),
        (
            "quoted_columns",
            "HASH(`order`, `select`)",
            None,
            "ALTER TABLE test_table DISTRIBUTED BY HASH(`order`, `select`)",
        ),
        (
            "quoted_columns_with_spaces",
            "HASH(`user id`, `created at`)",
            10,
            "ALTER TABLE test_table DISTRIBUTED BY HASH(`user id`, `created at`) BUCKETS 10",
        ),
        argnames="distribution_method, buckets, expected",
        id_="iaaa",
    )
    def test_alter_table_distribution(self, distribution_method, buckets, expected):
        """The distribution method is passed through as given; only BUCKETS is added."""
        self.assert_compile(AlterTableDistribution("test_table", distribution_method, buckets=buckets), expected)

    def test_alter_table_distribution_with_schema(self):
        self.assert_compile(
            AlterTableDistribution("events", "HASH(user_id)", buckets=16, schema="analytics"),
            "ALTER TABLE analytics.events DISTRIBUTED BY HASH(user_id) BUCKETS 16",
        )


class AlterTableOrderCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = StarRocksDialect()

    @testing.combinations(
        ("single_column", "id", "ALTER TABLE test_table ORDER BY (id)"),
        ("asc", "id ASC", "ALTER TABLE test_table ORDER BY (id ASC)"),
        ("desc", "id DESC", "ALTER TABLE test_table ORDER BY (id DESC)"),
        ("multiple_columns", "timestamp, event_id", "ALTER TABLE test_table ORDER BY (timestamp, event_id)"),
        ("mixed_directions", "id ASC, name DESC", "ALTER TABLE test_table ORDER BY (id ASC, name DESC)"),
        (
            "three_columns",
            "created_at DESC, updated_at ASC, id",
            "ALTER TABLE test_table ORDER BY (created_at DESC, updated_at ASC, id)",
        ),
        (
            "function",
            "date_format(created_at, '%Y-%m') DESC",
            "ALTER TABLE test_table ORDER BY (date_format(created_at, '%%Y-%%m') DESC)",
        ),
        (
            "function_and_column",
            "substr(name, 1, 1), id DESC",
            "ALTER TABLE test_table ORDER BY (substr(name, 1, 1), id DESC)",
        ),
        (
            "case_expression",
            "CASE WHEN status = 'active' THEN 1 ELSE 2 END, created_at DESC",
            "ALTER TABLE test_table ORDER BY (CASE WHEN status = 'active' THEN 1 ELSE 2 END, created_at DESC)",
        ),
        (
            "quoted_columns",
            "`order` ASC, `select` DESC",
            "ALTER TABLE test_table ORDER BY (`order` ASC, `select` DESC)",
        ),
        (
            "quoted_columns_with_spaces",
            "`user id`, `created at` DESC",
            "ALTER TABLE test_table ORDER BY (`user id`, `created at` DESC)",
        ),
        ("list", ["address", "last_active DESC"], "ALTER TABLE test_table ORDER BY (address, last_active DESC)"),
        (
            "list_of_names_to_quote",
            ["text", "Id", "`select`"],
            "ALTER TABLE test_table ORDER BY (`text`, `Id`, `select`)",
        ),
        argnames="order_by, expected",
        id_="iaa",
    )
    def test_alter_table_order(self, order_by, expected):
        """The sort key is passed through as given; a list is joined with commas, its bare names quoted as needed."""
        self.assert_compile(AlterTableOrder("test_table", order_by), expected)

    def test_alter_table_order_with_schema(self):
        self.assert_compile(
            AlterTableOrder("orders", "created_at DESC, id ASC", schema="shop"),
            "ALTER TABLE shop.orders ORDER BY (created_at DESC, id ASC)",
        )


class AlterTablePartitionAndKeyCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    """The dialect can render these statements, but its Alembic operations for them raise ``NotImplementedError``."""

    __dialect__ = StarRocksDialect()

    @testing.combinations(
        ("range", "RANGE(date_col)"),
        ("range_function", "RANGE(date_format(created_at, '%Y-%m'))"),
        ("range_columns", "RANGE(COLUMNS(year, month))"),
        ("range_substr", "RANGE(substr(id, 1, 4))"),
        ("list", "LIST(category)"),
        ("list_columns", "LIST(COLUMNS(region, status))"),
        argnames="partition_by",
        id_="ia",
    )
    def test_alter_table_partition(self, partition_by):
        self.assert_compile(
            AlterTablePartition("test_table", partition_by),
            f"ALTER TABLE test_table PARTITION BY {partition_by.replace('%', '%%')}",
        )

    @testing.combinations(
        ("primary", "PRIMARY", "id"),
        ("primary_multiple", "PRIMARY", "user_id, tenant_id"),
        ("unique", "UNIQUE", "username, email"),
        ("duplicate", "DUPLICATE", "category_id, subcategory_id, created_at"),
        ("aggregate_with_expression", "AGGREGATE", "region, category, date_trunc('day', created_at)"),
        argnames="key_type, key_columns",
        id_="iaa",
    )
    def test_alter_table_key(self, key_type, key_columns):
        self.assert_compile(
            AlterTableKey("test_table", key_type, key_columns), f"ALTER TABLE test_table {key_type} KEY ({key_columns})"
        )


class AlterTableOperationTest(fixtures.TestBase):
    """Each Alembic ``op.alter_table_*`` call emits the matching StarRocks ALTER TABLE statement."""

    def test_alter_table_properties(self):
        context = op_fixture("starrocks")
        op.alter_table_properties("users", {"replication_num": "3", "storage_medium": "SSD"}, schema="test_db")
        context.assert_(
            'ALTER TABLE test_db.users SET ("replication_num" = "3")',
            'ALTER TABLE test_db.users SET ("storage_medium" = "SSD")',
        )

    def test_alter_table_properties_groups_only_dynamic_partition_and_binlog_keys(self):
        context = op_fixture("starrocks")
        op.alter_table_properties(
            "users",
            {
                "bloom_filter_columns": "k1",
                "dynamic_partition.start": "-7",
                "binlog_enable": "true",
                "dynamic_partition.end": "3",
                "binlog_ttl_second": "60",
            },
        )
        context.assert_(
            'ALTER TABLE users SET ("bloom_filter_columns" = "k1")',
            'ALTER TABLE users SET ("dynamic_partition.start" = "-7", "dynamic_partition.end" = "3")',
            'ALTER TABLE users SET ("binlog_enable" = "true", "binlog_ttl_second" = "60")',
        )

    def test_alter_table_properties_with_a_property_of_no_value_sets_none_of_them(self):
        """Each group is a statement of its own; one that can't compile stops the op before the first runs."""
        context = op_fixture("starrocks")
        with expect_raises_message(exc.CompileError, "Property 'storage_medium' has no value"):
            op.alter_table_properties("users", {"replication_num": "3", "storage_medium": None})
        context.assert_()

    def test_alter_table_distribution(self):
        context = op_fixture("starrocks")
        op.alter_table_distribution("orders", "HASH(user_id)", buckets=32, schema="shop_db")
        context.assert_("ALTER TABLE shop_db.orders DISTRIBUTED BY HASH(user_id) BUCKETS 32")

    def test_alter_table_order(self):
        context = op_fixture("starrocks")
        op.alter_table_order("events", "timestamp DESC, event_id ASC", schema="analytics_db")
        context.assert_("ALTER TABLE analytics_db.events ORDER BY (timestamp DESC, event_id ASC)")

    @testing.combinations(
        ("unqualified", None, "ALTER TABLE users RENAME customers"),
        ("schema", "test_db", "ALTER TABLE test_db.users RENAME customers"),
        argnames="schema, expected",
        id_="iaa",
    )
    def test_rename_table(self, schema, expected):
        """StarRocks has no ``RENAME TO``, and the new name stays unqualified when the table has a schema."""
        context = op_fixture("starrocks")
        op.rename_table("users", "customers", schema=schema)
        context.assert_(expected)


class ConstraintOperationTest(fixtures.TestBase):
    """StarRocks tables hold no constraints besides their key, so constraint operations raise before reaching it."""

    @testing.combinations(
        ("primary_key", lambda: op.create_primary_key("pk_users", "users", ["id"]), "cannot add primary key 'pk_users'"),
        (
            "unique",
            lambda: op.create_unique_constraint("uq_users_email", "users", ["email"]),
            "cannot add unique constraint 'uq_users_email'",
        ),
        (
            "foreign_key",
            lambda: op.create_foreign_key("fk_orders_user", "orders", "users", ["user_id"], ["id"]),
            "cannot add foreign key 'fk_orders_user'",
        ),
        (
            "check",
            lambda: op.create_check_constraint("ck_users_age", "users", "age > 0"),
            "cannot add constraint 'ck_users_age'",
        ),
        (
            "drop_foreign_key",
            lambda: op.drop_constraint("fk_orders_user", "orders", type_="foreignkey"),
            "cannot drop foreign key 'fk_orders_user'",
        ),
        (
            "drop_unique",
            lambda: op.drop_constraint("uq_users_email", "users", type_="unique"),
            "cannot drop unique constraint 'uq_users_email'",
        ),
        argnames="operation, message",
        id_="iaa",
    )
    def test_constraint_operation_raises(self, operation, message):
        op_fixture("starrocks")
        with expect_raises_message(exc.CompileError, f"StarRocks {message}"):
            operation()
