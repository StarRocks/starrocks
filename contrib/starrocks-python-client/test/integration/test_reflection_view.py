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

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from starrocks.common.utils import TableAttributeNormalizer
from starrocks.engine.interfaces import ReflectedViewState


logger = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def setup_reflection_test_tables(sr_root_engine: Engine):
    """Create 'users' and 'orders' tables needed for several reflection tests."""
    with sr_root_engine.connect() as connection:
        connection.execute(text("DROP TABLE IF EXISTS users"))
        connection.execute(text("DROP TABLE IF EXISTS orders"))
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
                order_date DATE
            )
            DISTRIBUTED BY HASH(id)
            PROPERTIES ("replication_num" = "1")
        """))
        # Also create the special table for the special characters test
        connection.execute(text("DROP TABLE IF EXISTS `user-table`"))
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
        connection.execute(text("DROP TABLE IF EXISTS users"))
        connection.execute(text("DROP TABLE IF EXISTS orders"))
        connection.execute(text("DROP TABLE IF EXISTS `user-table`"))


@pytest.mark.usefixtures("setup_reflection_test_tables")
class TestReflectionViewsIntegration:
    """Integration tests for StarRocks view reflection from information_schema."""

    def test_reflect_simple_view(self, sr_root_engine: Engine):
        """Test reflection of a simple view with basic SELECT statement."""
        view_name = "test_reflect_simple_view"
        sr_engine = sr_root_engine

        with sr_engine.connect() as connection:
            # Clean up any existing view
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

            # Create a simple view
            create_view_sql = f"""
            CREATE VIEW {view_name} AS
            SELECT 1 as id, 'test' as name
            """
            connection.execute(text(create_view_sql))

            try:
                # Inspect the database to get view information
                inspector = inspect(connection)
                view_info = inspector.get_view(view_name, schema=sr_engine.url.database)

                assert view_info is not None
                assert isinstance(view_info, ReflectedViewState)
                assert view_info.name == view_name

                # Check that the definition is correctly reflected
                expected_def = "SELECT 1 AS id, 'test' AS name"
                assert TableAttributeNormalizer.normalize_sql(view_info.definition) == TableAttributeNormalizer.normalize_sql(expected_def)

                logger.info("Reflected view info: %s", view_info)

            finally:
                connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_reflect_view_with_comment(self, sr_root_engine: Engine):
        """Test reflection of a view with a comment."""
        view_name = "test_reflect_view_with_comment"
        sr_engine = sr_root_engine

        with sr_engine.connect() as connection:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

            # Create a view with comment
            create_view_sql = f"""
            CREATE VIEW {view_name}
            COMMENT 'This is a test view with comment'
            AS SELECT id, name FROM users WHERE active = 1
            """
            connection.execute(text(create_view_sql))

            try:
                inspector = inspect(connection)
                view_info = inspector.get_view(view_name, schema=sr_engine.url.database)

                assert view_info is not None
                assert view_info.name == view_name
                assert view_info.comment == "This is a test view with comment"

                logger.info("Reflected view with comment: %s", view_info)

            finally:
                connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_reflect_view_with_security(self, sr_root_engine: Engine):
        """Test reflection of a view with security definer/invoker."""
        view_name = "test_reflect_view_with_security"
        sr_engine = sr_root_engine

        with sr_engine.connect() as connection:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

            # Create a view with security definer
            create_view_sql = f"""
            CREATE VIEW {view_name}
            SECURITY INVOKER
            AS SELECT COUNT(*) as total FROM users
            """
            connection.execute(text(create_view_sql))

            try:
                inspector = inspect(connection)
                view_info = inspector.get_view(view_name, schema=sr_engine.url.database)

                assert view_info is not None
                assert view_info.name == view_name
                # TODO: StarRocks may not preserve SECURITY INVOKER in reflection
                # This test verifies the reflection doesn't crash
                logger.info(f"Reflected view with security: {view_info!r}")

            finally:
                connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_reflect_view_with_columns(self, sr_root_engine: Engine):
        """Test reflection of a view with explicit column definitions."""
        view_name = "test_reflect_view_with_columns"
        sr_engine = sr_root_engine

        with sr_engine.connect() as connection:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

            # Create a view with explicit column names and comments
            create_view_sql = f"""
            CREATE VIEW {view_name} (
                user_id COMMENT 'User identifier',
                user_name COMMENT 'User display name',
                order_count COMMENT 'Number of orders'
            ) AS
            SELECT u.id, u.name, COUNT(o.id)
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            GROUP BY u.id, u.name
            """
            connection.execute(text(create_view_sql))

            try:
                inspector = inspect(connection)
                view_info = inspector.get_view(view_name, schema=sr_engine.url.database)

                assert view_info is not None
                assert view_info.name == view_name

                # Check that the definition includes the GROUP BY clause
                definition = TableAttributeNormalizer.normalize_sql(view_info.definition)
                assert "group by" in definition
                assert "count" in definition

                logger.info("Reflected view with columns: %s", view_info)

            finally:
                connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_reflect_complex_view(self, sr_root_engine: Engine):
        """Test reflection of a complex view with joins, subqueries, and aggregations."""
        view_name = "test_reflect_complex_view"
        sr_engine = sr_root_engine

        with sr_engine.connect() as connection:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

            # Create a complex view
            create_view_sql = f"""
            CREATE VIEW {view_name} AS
            SELECT
                u.id,
                u.name,
                COUNT(o.id) as order_count,
                SUM(o.amount) as total_amount,
                AVG(o.amount) as avg_amount
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            WHERE u.active = 1
            GROUP BY u.id, u.name
            HAVING COUNT(o.id) > 0
            ORDER BY total_amount DESC
            """
            connection.execute(text(create_view_sql))

            try:
                inspector = inspect(connection)
                view_info = inspector.get_view(view_name, schema=sr_engine.url.database)

                assert view_info is not None
                assert view_info.name == view_name

                # Verify complex SQL elements are preserved
                definition = TableAttributeNormalizer.normalize_sql(view_info.definition)
                assert "left outer join" in definition
                assert "group by" in definition
                assert "having" in definition
                assert "order by" in definition
                assert "count" in definition
                assert "sum" in definition
                assert "avg" in definition

                logger.info("Reflected complex view: %s", view_info)

            finally:
                connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_reflect_view_with_window_functions(self, sr_root_engine: Engine):
        """Test reflection of a view with window functions."""
        view_name = "test_reflect_view_with_window"
        sr_engine = sr_root_engine

        with sr_engine.connect() as connection:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

            # Create a view with window functions
            create_view_sql = f"""
            CREATE VIEW {view_name} AS
            SELECT
                user_id,
                order_date,
                amount,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_date DESC) as rn,
                SUM(amount) OVER (PARTITION BY user_id ORDER BY order_date) as running_total
            FROM orders
            """
            connection.execute(text(create_view_sql))

            try:
                inspector = inspect(connection)
                view_info = inspector.get_view(view_name, schema=sr_engine.url.database)

                assert view_info is not None
                assert view_info.name == view_name

                # Verify window functions are preserved
                definition = TableAttributeNormalizer.normalize_sql(view_info.definition)
                assert "row_number()" in definition
                assert "over (" in definition
                assert "partition by" in definition
                assert "order by" in definition

                logger.info("Reflected view with window functions: %s", view_info)

            finally:
                connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_reflect_view_with_cte(self, sr_root_engine: Engine):
        """Test reflection of a view with Common Table Expressions (CTEs)."""
        view_name = "test_reflect_view_with_cte"
        sr_engine = sr_root_engine

        with sr_engine.connect() as connection:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

            # Create a view with CTE
            create_view_sql = f"""
            CREATE VIEW {view_name} AS
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
            connection.execute(text(create_view_sql))

            try:
                inspector = inspect(connection)
                view_info = inspector.get_view(view_name, schema=sr_engine.url.database)

                assert view_info is not None
                assert view_info.name == view_name

                # Verify CTE is preserved
                definition = TableAttributeNormalizer.normalize_sql(view_info.definition)
                assert "with" in definition
                assert "monthly_stats" in definition
                assert "coalesce" in definition

                logger.info("Reflected view with CTE: %s", view_info)

            finally:
                connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_reflect_view_with_special_characters(self, sr_root_engine: Engine):
        """Test reflection of a view with special characters in identifiers."""
        view_name = "test_reflect_view_special_chars"
        sr_engine = sr_root_engine

        with sr_engine.connect() as connection:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

            # Create a view with special characters
            create_view_sql = f"""
            CREATE VIEW {view_name} AS
            SELECT
                `user-id` as user_id,
                `user name` as user_name,
                `email@domain.com` as email
            FROM `user-table`
            WHERE `status` = 'active'
            """
            connection.execute(text(create_view_sql))

            try:
                inspector = inspect(connection)
                view_info = inspector.get_view(view_name, schema=sr_engine.url.database)

                assert view_info is not None
                assert view_info.name == view_name

                # Verify special characters are handled correctly
                definition = TableAttributeNormalizer.normalize_sql(view_info.definition)
                assert "user-id" in definition or "user_id" in definition
                assert "user name" in definition or "user_name" in definition

                logger.info("Reflected view with special characters: %s", view_info)

            finally:
                connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_reflect_multiple_views(self, sr_root_engine: Engine):
        """Test reflection of multiple views and get_view_names functionality."""
        view_names = ["test_reflect_view_1", "test_reflect_view_2", "test_reflect_view_3"]
        sr_engine = sr_root_engine

        with sr_engine.connect() as connection:
            # Clean up any existing views
            for view_name in view_names:
                connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

            # Create multiple views
            for i, view_name in enumerate(view_names):
                create_view_sql = f"""
                CREATE VIEW {view_name} AS
                SELECT {i + 1} as id, 'view_{i + 1}' as name
                """
                connection.execute(text(create_view_sql))

            try:
                inspector = inspect(connection)

                # Test get_view_names
                all_view_names = inspector.get_view_names(schema=sr_engine.url.database)
                for view_name in view_names:
                    assert view_name in all_view_names

                # Test individual view reflection
                for i, view_name in enumerate(view_names):
                    view_info = inspector.get_view(view_name, schema=sr_engine.url.database)
                    assert view_info is not None
                    assert view_info.name == view_name
                    assert str(i + 1) in TableAttributeNormalizer.normalize_sql(view_info.definition)

                logger.info("Reflected multiple views: %s", all_view_names)

            finally:
                for view_name in view_names:
                    connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_reflect_nonexistent_view(self, sr_root_engine: Engine):
        """Test reflection of a non-existent view returns None."""
        sr_engine = sr_root_engine

        inspector = inspect(sr_engine)
        view_info = inspector.get_view("nonexistent_view_12345", schema=sr_engine.url.database)

        assert view_info is None
        logger.info("Correctly returned None for non-existent view")

    def test_reflect_view_case_sensitivity(self, sr_root_engine: Engine):
        """Test view reflection with different case sensitivity scenarios."""
        view_name_lower = "test_reflect_view_case"
        view_name_upper = "TEST_REFLECT_VIEW_CASE"
        sr_engine = sr_root_engine

        with sr_engine.connect() as connection:
            # Clean up
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name_lower}"))
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name_upper}"))

            # Create view with lowercase name
            create_view_sql = f"""
            CREATE VIEW {view_name_lower} AS
            SELECT 1 as id, 'lowercase' as name
            """
            connection.execute(text(create_view_sql))

            try:
                inspector = inspect(connection)

                # Test reflection with exact case
                view_info = inspector.get_view(view_name_lower, schema=sr_engine.url.database)
                assert view_info is not None
                assert view_info.name == view_name_lower

                # Test reflection with different case (may or may not work depending on DB settings)
                view_info_upper = inspector.get_view(view_name_upper, schema=sr_engine.url.database)
                # This might be None or the same view depending on case sensitivity settings

                logger.info("Reflected view case sensitivity test: %s", view_info)

            finally:
                connection.execute(text(f"DROP VIEW IF EXISTS {view_name_lower}"))
                connection.execute(text(f"DROP VIEW IF EXISTS {view_name_upper}"))
