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
import time

import pytest
from sqlalchemy import Inspector, MetaData, inspect, text
from sqlalchemy.dialects import registry
from sqlalchemy.engine import Engine

from starrocks.common.utils import TableAttributeNormalizer
from starrocks.engine.interfaces import ReflectedMVOptions
from starrocks.sql.ddl import CreateMaterializedView
from starrocks.sql.schema import MaterializedView


logger = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def setup_mv_reflection_test_tables(sr_root_engine: Engine):
    """Create 'users', 'orders', and 'products' tables needed for MV reflection tests."""
    table_names = ["users", "orders", "orders_part_expr", "products", "`user-table`"]
    with sr_root_engine.connect() as connection:
        for table_name in table_names:
            connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

        connection.execute(text("""
            CREATE TABLE users (
                id INT,
                name VARCHAR(50),
                active BOOLEAN
            )
            DISTRIBUTED BY HASH(id)
            PROPERTIES ("replication_num" = "1")
        """))
        connection.execute(text("""
            CREATE TABLE orders (
                id INT,
                user_id INT,
                amount DECIMAL(10, 2),
                order_date DATETIME
            )
            PARTITION BY (order_date)
            DISTRIBUTED BY HASH(id)
            PROPERTIES ("replication_num" = "1")
        """))
        connection.execute(text("""
            CREATE TABLE orders_part_expr (
                id INT,
                user_id INT,
                amount DECIMAL(10, 2),
                order_date DATETIME
            )
            PARTITION BY date_trunc('day', order_date)
            DISTRIBUTED BY HASH(id)
            PROPERTIES ("replication_num" = "1")
        """))
        connection.execute(text("""
            CREATE TABLE products (
                id INT,
                category_id INT,
                price DECIMAL(10, 2),
                stock_quantity INT
            )
            DISTRIBUTED BY HASH(id)
            PROPERTIES ("replication_num" = "1")
        """))
        connection.execute(text("""
            CREATE TABLE `user-table` (
                `user-id` INT,
                `user name` VARCHAR(50),
                `email@domain.com` VARCHAR(100),
                `status` VARCHAR(20)
            )
            DISTRIBUTED BY HASH(`user-id`)
            PROPERTIES ("replication_num" = "1")
        """))

    yield

    with sr_root_engine.connect() as connection:
        for table_name in table_names:
            connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))


def _wait_for_mv_creation(inspector: Inspector, mv_name: str, max_retries: int = 20, delay: int = 3):
    """Waits for a materialized view to appear in the database metadata."""
    return True
    for _ in range(max_retries):
        inspector.clear_cache()
        if inspector.has_table(mv_name, schema=inspector.default_schema_name):
            logger.info(f"Materialized view '{mv_name}' found.")
            return True
        logger.debug(f"Waiting for materialized view '{mv_name}' to be created...")
        time.sleep(delay)
    logger.error(f"Timed out waiting for materialized view '{mv_name}' to be created.")


@pytest.mark.usefixtures("setup_mv_reflection_test_tables")
class TestReflectionMaterializedViewsIntegration:
    """Integration tests for StarRocks materialized view reflection from information_schema."""

    def test_reflect_simple_mv(self, sr_root_engine: Engine):
        """Test reflection of a simple materialized view with basic SELECT statement."""
        mv_name = "test_reflect_simple_mv"
        sr_engine = sr_root_engine

        with sr_engine.connect() as connection:
            # Clean up any existing MV
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

            # Create a simple materialized view using the users table
            create_mv_sql = f"""
            CREATE MATERIALIZED VIEW {mv_name}
            REFRESH MANUAL
            AS
            SELECT id, name FROM users WHERE active = true
            """
            logger.debug(f"Creating MV: {create_mv_sql}")
            connection.execute(text(create_mv_sql))

            try:
                # Inspect the database to get MV information
                inspector = inspect(connection)
                _wait_for_mv_creation(inspector, mv_name)
                inspector.clear_cache()
                mv_definition = inspector.get_materialized_view_definition(mv_name, schema=sr_engine.url.database)

                assert mv_definition is not None
                # logger.debug(f"MV definition: {mv_definition}")

                # Check that the definition is correctly reflected
                expected_def = "SELECT id, name FROM users WHERE active = true"
                assert TableAttributeNormalizer.normalize_sql(expected_def, remove_qualifiers=True) \
                    in TableAttributeNormalizer.normalize_sql(mv_definition, remove_qualifiers=True)
            finally:
                connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    def test_reflect_mv_with_properties(self, sr_root_engine: Engine):
        """Test reflection of a materialized view with properties."""
        mv_name = "test_reflect_mv_with_properties"
        sr_engine = sr_root_engine

        with sr_engine.connect() as connection:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

            # Create a materialized view with properties
            create_mv_sql = f"""
            CREATE MATERIALIZED VIEW {mv_name}
            REFRESH MANUAL
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "SSD",
                "storage_cooldown_time" = "2025-12-31 23:59:59"
            )
            AS SELECT id, name FROM users WHERE active = 1
            """
            connection.execute(text(create_mv_sql))

            try:
                inspector = inspect(connection)
                _wait_for_mv_creation(inspector, mv_name)
                inspector.clear_cache()
                mv_definition = inspector.get_materialized_view_definition(mv_name, schema=sr_engine.url.database)

                assert mv_definition is not None
                logger.debug(f"mv_definition: {mv_definition}")

                # Check that the definition includes the WHERE clause
                definition = TableAttributeNormalizer.normalize_sql(mv_definition)
                assert "where" in definition
                assert "active" in definition

                # Check properties
                mv_opts: ReflectedMVOptions = inspector.get_materialized_view_options(mv_name, schema=sr_engine.url.database)
                assert mv_opts is not None
                properties: str = mv_opts.properties
                logger.debug(f"properties: {properties}")
                assert "storage_medium" in properties
                assert "SSD" in properties
                assert "storage_cooldown_time" in properties
                assert "2025-12-31 23:59:59" in properties

                logger.info("Reflected MV with properties: %s", mv_definition)

            finally:
                connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    def test_reflect_mv_with_comment(self, sr_root_engine: Engine):
        """Test reflection of a materialized view with comment."""
        mv_name = "test_reflect_mv_with_comment"
        sr_engine = sr_root_engine

        with sr_root_engine.connect() as connection:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

            # Create a materialized view with comment
            create_mv_sql = f"""
            CREATE MATERIALIZED VIEW {mv_name}
            COMMENT 'This is a test comment for the materialized view.'
            REFRESH MANUAL
            AS SELECT id, name FROM users
            """
            connection.execute(text(create_mv_sql))

            try:
                inspector = inspect(connection)
                _wait_for_mv_creation(inspector, mv_name)
                inspector.clear_cache()

                mv_state = inspector.get_materialized_view(mv_name, schema=sr_engine.url.database)

                assert mv_state is not None
                assert mv_state.comment == 'This is a test comment for the materialized view.'
                logger.info(f"Reflected MV comment: '{mv_state.comment}'")

            finally:
                connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    def test_reflect_mv_with_aggregation(self, sr_root_engine: Engine):
        """Test reflection of a materialized view with aggregation functions."""
        mv_name = "test_reflect_mv_with_aggregation"
        sr_engine = sr_root_engine

        with sr_engine.connect() as connection:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

            # Create a materialized view with aggregation
            create_mv_sql = f"""
            CREATE MATERIALIZED VIEW {mv_name}
            REFRESH MANUAL
            AS
            SELECT
                u.id,
                u.name,
                COUNT(o.id) as order_count,
                SUM(o.amount) as total_amount,
                AVG(o.amount) as avg_amount
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            GROUP BY u.id, u.name
            """
            connection.execute(text(create_mv_sql))

            try:
                inspector = inspect(connection)
                _wait_for_mv_creation(inspector, mv_name)
                inspector.clear_cache()
                mv_definition = inspector.get_materialized_view_definition(mv_name, schema=sr_engine.url.database)

                assert mv_definition is not None

                # Verify aggregation functions are preserved
                definition = TableAttributeNormalizer.normalize_sql(mv_definition)
                assert "left outer join" in definition
                assert "group by" in definition
                assert "count" in definition
                assert "sum" in definition
                assert "avg" in definition

                logger.info("Reflected MV with aggregation: %s", mv_definition)

            finally:
                connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    def test_reflect_mv_with_window_functions(self, sr_root_engine: Engine):
        """Test reflection of a materialized view with window functions."""
        mv_name = "test_reflect_mv_with_window"
        sr_engine = sr_root_engine

        with sr_engine.connect() as connection:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

            # Create a materialized view with window functions
            create_mv_sql = f"""
            CREATE MATERIALIZED VIEW {mv_name}
            REFRESH MANUAL
            AS
            SELECT
                user_id,
                order_date,
                amount,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_date DESC) as rn,
                SUM(amount) OVER (PARTITION BY user_id ORDER BY order_date) as running_total
            FROM orders
            """
            connection.execute(text(create_mv_sql))

            try:
                inspector = inspect(connection)
                _wait_for_mv_creation(inspector, mv_name)
                inspector.clear_cache()
                mv_definition = inspector.get_materialized_view_definition(mv_name, schema=sr_engine.url.database)

                assert mv_definition is not None

                # Verify window functions are preserved
                definition = TableAttributeNormalizer.normalize_sql(mv_definition)
                assert "row_number()" in definition
                assert "over (" in definition
                assert "partition by" in definition
                assert "order by" in definition

                logger.info("Reflected MV with window functions: %s", mv_definition)

            finally:
                connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    def test_reflect_mv_with_cte(self, sr_root_engine: Engine):
        """Test reflection of a materialized view with Common Table Expressions (CTEs)."""
        mv_name = "test_reflect_mv_with_cte"
        sr_engine = sr_root_engine

        with sr_engine.connect() as connection:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

            # Create a materialized view with CTE
            create_mv_sql = f"""
            CREATE MATERIALIZED VIEW {mv_name}
            REFRESH MANUAL
            AS
            WITH monthly_stats AS (
                SELECT user_id, COUNT(*) as monthly_orders
                FROM orders
                WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
                GROUP BY user_id
            )
            SELECT u.name, COALESCE(ms.monthly_orders, 0) as orders
            FROM users u
            LEFT JOIN monthly_stats ms ON u.id = ms.user_id
            """
            connection.execute(text(create_mv_sql))

            try:
                inspector = inspect(connection)
                _wait_for_mv_creation(inspector, mv_name)
                inspector.clear_cache()
                mv_definition = inspector.get_materialized_view_definition(mv_name, schema=sr_engine.url.database)

                assert mv_definition is not None

                # Verify CTE is preserved
                definition = TableAttributeNormalizer.normalize_sql(mv_definition)
                assert "with" in definition
                assert "monthly_stats" in definition
                assert "coalesce" in definition

                logger.info("Reflected MV with CTE: %s", mv_definition)

            finally:
                connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    def test_reflect_mv_with_special_chars(self, sr_root_engine: Engine):
        """Test reflection of a materialized view with special characters in identifiers."""
        mv_name = "test_reflect_mv_special_chars"
        sr_engine = sr_root_engine

        with sr_engine.connect() as connection:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

            # Create a materialized view with special characters
            create_mv_sql = f"""
            CREATE MATERIALIZED VIEW {mv_name}
            REFRESH MANUAL
            AS
            SELECT
                `user-id` as user_id,
                `user name` as user_name,
                `email@domain.com` as email
            FROM `user-table`
            WHERE `status` = 'active'
            """
            connection.execute(text(create_mv_sql))

            try:
                inspector = inspect(connection)
                _wait_for_mv_creation(inspector, mv_name)
                inspector.clear_cache()
                mv_definition = inspector.get_materialized_view_definition(mv_name, schema=sr_engine.url.database)

                assert mv_definition is not None
                logger.info("Reflected MV definition: %s", mv_definition)

                # Verify special characters are handled correctly
                definition = TableAttributeNormalizer.normalize_sql(mv_definition, remove_qualifiers=True)
                logger.info("Normalized reflected MV definition: %s", definition)
                assert "user-id" in definition or "user_id" in definition
                assert "user name" in definition or "user_name" in definition

                logger.info("Reflected MV with special characters: %s", mv_definition)

            finally:
                connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    def test_reflect_multiple_mvs(self, sr_root_engine: Engine):
        """Test reflection of multiple materialized views and get_materialized_view_names functionality."""
        mv_names = ["test_reflect_mv_1", "test_reflect_mv_2", "test_reflect_mv_3"]
        sr_engine = sr_root_engine

        with sr_engine.connect() as connection:
            # Clean up any existing MVs
            for mv_name in mv_names:
                connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

            # Create multiple materialized views
            for i, mv_name in enumerate(mv_names):
                create_mv_sql = f"""
                CREATE MATERIALIZED VIEW {mv_name}
                REFRESH MANUAL
                AS
                SELECT id, name FROM users WHERE id = {i + 1}
                """
                connection.execute(text(create_mv_sql))
                _wait_for_mv_creation(inspect(connection), mv_name)

            try:
                inspector = inspect(connection)
                inspector.clear_cache()

                # Test get_materialized_view_names
                all_mv_names = inspector.get_materialized_view_names(schema=sr_engine.url.database)
                for mv_name in mv_names:
                    assert mv_name in all_mv_names

                # Test individual MV reflection
                for i, mv_name in enumerate(mv_names):
                    inspector.clear_cache()
                    mv_definition = inspector.get_materialized_view_definition(mv_name, schema=sr_engine.url.database)
                    assert mv_definition is not None
                    definition = TableAttributeNormalizer.normalize_sql(mv_definition)
                    assert "users" in definition
                    assert "id" in definition
                    assert "name" in definition

                logger.info("Reflected multiple MVs: %s", all_mv_names)

            finally:
                for mv_name in mv_names:
                    connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    def test_reflect_nonexistent_mv(self, sr_root_engine: Engine):
        """Test reflection of a non-existent materialized view returns None."""
        sr_engine = sr_root_engine

        with sr_engine.connect() as connection:
            inspector = inspect(connection)
            inspector.clear_cache()
            mv_definition = inspector.get_materialized_view_definition("nonexistent_mv_12345", schema=sr_engine.url.database)
            assert mv_definition is None
        logger.info("Correctly returned None for non-existent MV")

    def test_reflect_mv_case_normalization(self, sr_root_engine: Engine):
        """Test reflection of materialized view properties normalizes case."""
        mv_name = "test_reflect_mv_case_normalization"
        sr_engine = sr_root_engine

        with sr_engine.connect() as connection:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

            # Create MV with lowercase keywords in its definition
            create_mv_sql = f"""
            create materialized view {mv_name}
            refresh manual
            as
            select id, name from users where id = 1
            """
            connection.execute(text(create_mv_sql))

            try:
                inspector = inspect(connection)
                _wait_for_mv_creation(inspector, mv_name)
                inspector.clear_cache()

                mv_opts = inspector.get_materialized_view_options(mv_name, schema=sr_engine.url.database)
                assert mv_opts is not None

                # Assert that the reflected refresh_type is normalized to uppercase
                assert mv_opts.refresh_type == "MANUAL"

                logger.info(f"Reflected refresh_type '{mv_opts.refresh_type}' is correctly normalized.")

            finally:
                connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    def test_reflect_mv_with_complex_join(self, sr_root_engine: Engine):
        """Test reflection of a materialized view with complex joins."""
        mv_name = "test_reflect_mv_complex_join"
        sr_engine = sr_root_engine

        with sr_engine.connect() as connection:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

            # Create a materialized view with complex joins
            create_mv_sql = f"""
            CREATE MATERIALIZED VIEW {mv_name}
            REFRESH MANUAL
            AS
            SELECT
                u.id as user_id,
                u.name as user_name,
                p.id as product_id,
                p.price,
                o.amount,
                o.order_date
            FROM users u
            INNER JOIN orders o ON u.id = o.user_id
            LEFT JOIN products p ON o.id = p.id
            WHERE u.active = true
            ORDER BY o.order_date DESC
            """
            connection.execute(text(create_mv_sql))

            try:
                inspector = inspect(connection)
                _wait_for_mv_creation(inspector, mv_name)
                inspector.clear_cache()
                mv_definition = inspector.get_materialized_view_definition(mv_name, schema=sr_engine.url.database)

                assert mv_definition is not None

                # Verify complex join elements are preserved
                definition = TableAttributeNormalizer.normalize_sql(mv_definition)
                assert "inner join" in definition
                assert "left outer join" in definition
                assert "where" in definition
                assert "order by" in definition

                logger.info("Reflected MV with complex joins: %s", mv_definition)

            finally:
                connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    @pytest.mark.parametrize(
        "dist_clause, expected_dist",
        [
            ("DISTRIBUTED BY HASH(user_id) BUCKETS 16", "HASH(`user_id`) BUCKETS 16"),
            ("DISTRIBUTED BY RANDOM", "RANDOM"),
            ("DISTRIBUTED BY HASH(user_id, id)", "HASH(`user_id`, `id`)"),
        ]
    )
    def test_reflect_mv_with_distribution(self, sr_root_engine: Engine, dist_clause: str, expected_dist: str):
        """Test reflection of various MV distribution clauses."""
        mv_name = "test_reflect_mv_dist"
        sr_engine = sr_root_engine
        with sr_engine.connect() as connection:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

            create_mv_sql = f"""
            CREATE MATERIALIZED VIEW {mv_name}
            {dist_clause}
            REFRESH MANUAL
            AS SELECT user_id, id, SUM(amount) as total_amount
               FROM orders
               GROUP BY user_id, id
            """
            connection.execute(text(create_mv_sql))

            try:
                inspector = inspect(connection)
                logger.debug(f"inspector: {inspector!r}")
                _wait_for_mv_creation(inspector, mv_name)

                mv_opts = inspector.get_materialized_view_options(mv_name)
                assert mv_opts is not None
                assert str(mv_opts.distributed_by) == expected_dist

            finally:
                connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    @pytest.mark.parametrize(
        "table_name, partition_clause, expected_partition_str",
        [
            # It's for CREATE TABLE, not for CREATE MATERIALIZED VIEW. keep it for reference.
            # (
            #     "orders",
            #     'PARTITION BY RANGE(order_date) (PARTITION p_2023 VALUES LESS THAN ("2024-01-01"))',
            #     "RANGE(`order_date`)"
            # ),
            (
                "orders",
                "PARTITION BY (`order_date`)",
                "(`order_date`)"
            ),
            (
                "orders_part_expr",
                "PARTITION BY date_trunc('month', order_date)",
                "date_trunc('month', `order_date`)"
            ),
            # It's not supporeted yet in StarRocks v3.5 for multi-column expression partitioning with functions.
            # (
            #     "orders_part_expr",
            #     "PARTITION BY (date_trunc('month', order_date), order_date)",
            #     "(date_trunc('month', `order_date`), `order_date`)"
            # ),
        ]
    )
    def test_reflect_mv_partitioning(self, sr_root_engine: Engine,
            table_name: str, partition_clause: str, expected_partition_str: str):
        """Test reflection of various MV partitioning schemes."""
        mv_name = f"test_reflect_mv_partition_{table_name}"
        with sr_root_engine.connect() as connection:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

            create_mv_sql = f"""
            CREATE MATERIALIZED VIEW {mv_name}
            {partition_clause}
            DISTRIBUTED BY HASH(id)
            REFRESH MANUAL
            AS
            SELECT id, order_date FROM {table_name}
            """
            connection.execute(text(create_mv_sql))

            try:
                inspector = inspect(connection)
                _wait_for_mv_creation(inspector, mv_name)
                inspector.clear_cache()

                mv_opts = inspector.get_materialized_view_options(mv_name)
                assert mv_opts is not None
                assert expected_partition_str in str(mv_opts.partition_by)

            finally:
                connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    @pytest.mark.parametrize(
        "refresh_clause, expected_moment, expected_type",
        [
            ('REFRESH ASYNC START("2025-01-01 00:00:00") EVERY(INTERVAL 1 DAY)', None, 'ASYNC START("2025-01-01 00:00:00") EVERY(INTERVAL 1 DAY)'),
            ("REFRESH ASYNC EVERY(INTERVAL 2 HOUR)", None, "ASYNC EVERY(INTERVAL 2 HOUR)"),
            ("REFRESH ASYNC", None, "ASYNC"),
            ("REFRESH MANUAL", None, "MANUAL"),
            # refresh_moment `IMMEDIATE` is not saved in SR MV definition, but `DEFERRED` is.
            ("REFRESH IMMEDIATE ASYNC", None, "ASYNC"),
            ("REFRESH DEFERRED MANUAL", "DEFERRED", "MANUAL"),
        ]
    )
    def test_reflect_mv_refresh_strategy(self, sr_root_engine: Engine, refresh_clause: str, expected_moment: str,
                                           expected_type: str):
        """Test reflection of different MV refresh strategies."""
        mv_name = "test_reflect_mv_refresh"
        with sr_root_engine.connect() as connection:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

            create_mv_sql = f"""
            CREATE MATERIALIZED VIEW {mv_name}
            {refresh_clause}
            AS SELECT id, name FROM users
            """
            logger.debug(f"Creating MV with refresh clause: {create_mv_sql}")
            connection.execute(text(create_mv_sql))

            try:
                inspector = inspect(connection)
                _wait_for_mv_creation(inspector, mv_name)
                inspector.clear_cache()

                mv_opts = inspector.get_materialized_view_options(mv_name)
                assert mv_opts is not None
                assert mv_opts.refresh_moment == expected_moment
                assert mv_opts.refresh_type.upper() == expected_type

            finally:
                pass
                # connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    def test_reflect_mv_kitchen_sink(self, sr_root_engine: Engine):
        """Test reflection of a materialized view with all properties (kitchen sink)."""
        mv_name = "test_reflect_mv_kitchen_sink"
        sr_engine = sr_root_engine

        with sr_engine.connect() as connection:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

            create_mv_sql = f"""
            CREATE MATERIALIZED VIEW {mv_name}
            PARTITION BY date_trunc('month', order_date)
            DISTRIBUTED BY HASH(user_id) BUCKETS 8
            ORDER BY (order_date)
            REFRESH ASYNC EVERY(INTERVAL 1 HOUR)
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "SSD",
                "query_rewrite_consistency" = "loose"
            )
            AS
            SELECT user_id, order_date, count(*) as cnt
            FROM orders_part_expr
            GROUP BY user_id, order_date
            """
            connection.execute(text(create_mv_sql))

            try:
                inspector = inspect(connection)
                _wait_for_mv_creation(inspector, mv_name)

                mv_opts = inspector.get_materialized_view_options(mv_name)
                assert mv_opts is not None
                logger.debug(f"mv_opts: {mv_opts}")

                # Assert Distribution & Order
                assert str(mv_opts.distributed_by) == 'HASH(`user_id`) BUCKETS 8'
                assert mv_opts.order_by == '`order_date`'

                # Assert Partitioning
                assert "date_trunc('month', `order_date`)" in str(mv_opts.partition_by)

                # Assert Refresh Info
                assert mv_opts.refresh_moment is None
                assert mv_opts.refresh_type == 'ASYNC EVERY(INTERVAL 1 HOUR)'

                # Assert Properties
                properties = mv_opts.properties.lower()
                logger.debug(f"properties: {properties}")
                assert "storage_medium" in properties
                assert "SSD".lower() in properties
                assert "query_rewrite_consistency" in properties
                assert "loose" in properties

            finally:
                connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    def test_reflect_mv_from_schema_object(self, sr_root_engine: Engine):
        """Tests that a MaterializedView schema object and a reflected MV are equivalent."""
        mv_name = "test_reflect_from_object"
        schema = sr_root_engine.url.database

        # 1. Define the MaterializedView object
        metadata = MetaData()
        mv_obj = MaterializedView(
            mv_name,
            "SELECT user_id, order_date, count(*) as cnt FROM orders_part_expr GROUP BY user_id, order_date",
            metadata=metadata,
            schema=schema,
            comment="A test MV created from a schema object.",
            partition_by="date_trunc('month', order_date)",
            distributed_by="HASH(user_id) BUCKETS 8",
            order_by="order_date",
            refresh_moment="DEFERRED",
            refresh_type="MANUAL",
            properties={
                "replication_num": "1",
                "query_rewrite_consistency": "loose"
            }
        )

        with sr_root_engine.connect() as connection:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

            # 2. Compile and create the MV
            dialect = registry.load("starrocks")()
            create_sql = str(CreateMaterializedView(mv_obj).compile(dialect=dialect))
            connection.execute(text(create_sql))

            try:
                # 3. Reflect the MV
                inspector = inspect(connection)
                _wait_for_mv_creation(inspector, mv_name)

                reflected_state = inspector.get_materialized_view(mv_name, schema=schema)
                assert reflected_state is not None

                # 4. Compare original object with reflected state
                assert reflected_state.name == mv_obj.name
                assert reflected_state.comment == mv_obj.comment

                # Compare options
                reflected_opts = reflected_state.mv_options
                assert reflected_opts.refresh_moment == mv_obj.refresh_moment.upper()
                assert reflected_opts.refresh_type == mv_obj.refresh_type.upper()
                assert str(reflected_opts.order_by) == '`order_date`'

                assert "date_trunc('month', `order_date`)" in str(reflected_opts.partition_by)
                assert str(reflected_opts.distributed_by) == 'HASH(`user_id`) BUCKETS 8'

                reflected_props_str = reflected_opts.properties.lower()
                for k, v in mv_obj.properties.items():
                    assert f'"{k.lower()}" = "{v.lower()}"' in reflected_props_str

                normalized_original_def = TableAttributeNormalizer.normalize_sql(mv_obj.definition, remove_qualifiers=True)
                normalized_reflected_def = TableAttributeNormalizer.normalize_sql(reflected_state.definition, remove_qualifiers=True)
                logger.debug(f"mv_obj.definition: {mv_obj.definition}")
                logger.debug(f"normalized_original_def: {normalized_original_def}")
                logger.debug(f"reflected_state.definition: {reflected_state.definition}")
                logger.debug(f"normalized_reflected_def: {normalized_reflected_def}")
                assert normalized_original_def in normalized_reflected_def

            finally:
                connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
