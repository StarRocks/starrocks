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
from sqlalchemy import MetaData, Table, inspect, text
from sqlalchemy.engine import Engine

from starrocks.common.params import TableInfoKey
from starrocks.common.utils import TableAttributeNormalizer, get_dialect_option
from starrocks.datatype import DATE, DECIMAL, INTEGER, TINYINT, VARCHAR
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
    """
    Integration tests for StarRocks view reflection.

    This test suite is organized into two sections:
    1. Low-level API tests (inspector.get_view) - verify dialect implementation
    2. User-facing API tests (Table.autoload_with) - verify end-user experience
    """

    # ============================================================================
    # Section 1: Low-level API Tests (inspector.get_view)
    # These tests verify the dialect's get_view() implementation
    # ============================================================================

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
        """Test reflection of a view with a comment using inspector.reflect_table()."""
        view_name = "test_reflect_view_with_comment"
        sr_engine = sr_root_engine
        metadata = MetaData()

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
                # Reflect the view using reflect_table() - must use connection-based inspector
                inspector = inspect(connection)
                view_table = Table(view_name, metadata)
                inspector.reflect_table(view_table, include_columns=None, exclude_columns=())

                # Verify it's recognized as a VIEW
                assert view_table.info.get('table_kind') == 'VIEW'

                # Verify comment
                assert view_table.comment == "This is a test view with comment"

                # Verify definition - compare normalized SQL for simple cases
                assert 'definition' in view_table.info
                definition = view_table.info['definition']
                normalized_def = TableAttributeNormalizer.normalize_sql(definition)

                # For simple views, we can compare the full normalized SQL
                expected_elements = ['select', 'id', 'name', 'from', 'users', 'where', 'active']
                for elem in expected_elements:
                    assert elem in normalized_def, f"Expected '{elem}' in definition"

                logger.info("Reflected view with comment: table_kind=%s, comment=%s",
                           view_table.info.get('table_kind'), view_table.comment)

            finally:
                connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_reflect_view_with_security(self, sr_root_engine: Engine):
        """Test reflection of a view with security definer/invoker using Table.autoload_with."""
        view_name = "test_reflect_view_with_security"
        sr_engine = sr_root_engine
        metadata = MetaData()

        with sr_engine.connect() as connection:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

            # Create a view with security invoker
            create_view_sql = f"""
            CREATE VIEW {view_name}
            SECURITY INVOKER
            AS SELECT COUNT(*) as total FROM users
            """
            connection.execute(text(create_view_sql))

            try:
                # Reflect the view as a Table object
                view_table = Table(view_name, metadata, autoload_with=sr_engine)

                # Verify it's recognized as a VIEW
                assert view_table.info.get('table_kind') == 'VIEW'

                # Verify definition exists
                assert 'definition' in view_table.info
                definition = view_table.info['definition']
                normalized_def = TableAttributeNormalizer.normalize_sql(definition)
                assert 'count' in normalized_def
                assert 'users' in normalized_def

                # Verify security attribute is correctly reflected from SHOW CREATE VIEW
                security = get_dialect_option(view_table, TableInfoKey.SECURITY)
                assert security == 'INVOKER', f"Expected security='INVOKER', got '{security}'"
                logger.info("Reflected view with security: table_kind=%s, security=%s",
                           view_table.info.get('table_kind'), security)

            finally:
                connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_reflect_view_with_columns(self, sr_root_engine: Engine):
        """Test reflection of a view with explicit column definitions using inspector.reflect_table()."""
        view_name = "test_reflect_view_with_columns"
        sr_engine = sr_root_engine
        metadata = MetaData()

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
                # Reflect the view using reflect_table() with explicit parameters - use connection inspector
                inspector = inspect(connection)
                view_table = Table(view_name, metadata)
                inspector.reflect_table(
                    view_table,
                    include_columns=None,  # Include all columns
                    exclude_columns=(),     # Exclude none
                    resolve_fks=True
                )

                # Verify it's recognized as a VIEW
                assert view_table.info.get('table_kind') == 'VIEW'

                # Verify columns are reflected
                assert len(view_table.columns) == 3
                assert 'user_id' in view_table.columns
                assert 'user_name' in view_table.columns
                assert 'order_count' in view_table.columns

                # Verify column comments if StarRocks supports it
                user_id_col = view_table.columns['user_id']
                if hasattr(user_id_col, 'comment') and user_id_col.comment:
                    assert user_id_col.comment == 'User identifier'

                # Check that the definition includes the GROUP BY clause
                definition = view_table.info.get('definition', '')
                normalized_def = TableAttributeNormalizer.normalize_sql(definition)
                assert "group by" in normalized_def
                assert "count" in normalized_def

                logger.info("Reflected view with columns: %d columns", len(view_table.columns))

            finally:
                connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_reflect_complex_view(self, sr_root_engine: Engine):
        """Test reflection of a complex view with joins, subqueries, and aggregations using Table.autoload_with."""
        view_name = "test_reflect_complex_view"
        sr_engine = sr_root_engine
        metadata = MetaData()

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
                # Reflect the view as a Table object
                view_table = Table(view_name, metadata, autoload_with=sr_engine)

                # Verify it's recognized as a VIEW
                assert view_table.info.get('table_kind') == 'VIEW'

                # Verify columns are reflected (5 columns expected)
                assert len(view_table.columns) == 5
                assert 'id' in view_table.columns
                assert 'name' in view_table.columns
                assert 'order_count' in view_table.columns
                assert 'total_amount' in view_table.columns
                assert 'avg_amount' in view_table.columns

                # Verify complex SQL elements are preserved in definition
                definition = view_table.info.get('definition', '')
                normalized_def = TableAttributeNormalizer.normalize_sql(definition)
                assert "left" in normalized_def and "join" in normalized_def
                assert "group by" in normalized_def
                assert "having" in normalized_def
                assert "order by" in normalized_def
                assert "count" in normalized_def
                assert "sum" in normalized_def
                assert "avg" in normalized_def

                logger.info("Reflected complex view: %d columns", len(view_table.columns))

            finally:
                connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_reflect_view_definition_with_window_functions(self, sr_root_engine: Engine):
        """Test reflection of a view with window functions using Table.autoload_with."""
        view_name = "test_reflect_view_with_window"
        sr_engine = sr_root_engine
        metadata = MetaData()

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
                # Reflect the view as a Table object
                view_table = Table(view_name, metadata, autoload_with=sr_engine)

                # Verify it's recognized as a VIEW
                assert view_table.info.get('table_kind') == 'VIEW'

                # Verify columns (5 expected)
                assert len(view_table.columns) == 5
                assert 'rn' in view_table.columns
                assert 'running_total' in view_table.columns

                # Verify window functions are preserved in definition
                definition = view_table.info.get('definition', '')
                normalized_def = TableAttributeNormalizer.normalize_sql(definition)
                assert "row_number()" in normalized_def
                assert "over (" in normalized_def
                assert "partition by" in normalized_def
                assert "order by" in normalized_def

                logger.info("Reflected view with window functions: %d columns", len(view_table.columns))

            finally:
                connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_reflect_view_definition_with_cte(self, sr_root_engine: Engine):
        """Test reflection of a view with Common Table Expressions (CTEs) using Table.autoload_with."""
        view_name = "test_reflect_view_with_cte"
        sr_engine = sr_root_engine
        metadata = MetaData()

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
                # Reflect the view as a Table object
                view_table = Table(view_name, metadata, autoload_with=sr_engine)

                # Verify it's recognized as a VIEW
                assert view_table.info.get('table_kind') == 'VIEW'

                # Verify columns (2 expected)
                assert len(view_table.columns) == 2
                assert 'name' in view_table.columns
                assert 'orders' in view_table.columns

                # Verify CTE is preserved in definition
                definition = view_table.info.get('definition', '')
                normalized_def = TableAttributeNormalizer.normalize_sql(definition)
                assert "with" in normalized_def
                assert "monthly_stats" in normalized_def
                assert "coalesce" in normalized_def

                logger.info("Reflected view with CTE: %d columns", len(view_table.columns))

            finally:
                connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_reflect_view_definition_with_special_characters(self, sr_root_engine: Engine):
        """Test reflection of a view with special characters in identifiers using Table.autoload_with."""
        view_name = "test_reflect_view_special_chars"
        sr_engine = sr_root_engine
        metadata = MetaData()

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
                # Reflect the view as a Table object
                view_table = Table(view_name, metadata, autoload_with=sr_engine)

                # Verify it's recognized as a VIEW
                assert view_table.info.get('table_kind') == 'VIEW'

                # Verify columns (3 expected with aliased names)
                assert len(view_table.columns) == 3
                assert 'user_id' in view_table.columns
                assert 'user_name' in view_table.columns
                assert 'email' in view_table.columns

                # Verify special characters are handled correctly in definition
                definition = view_table.info.get('definition', '')
                normalized_def = TableAttributeNormalizer.normalize_sql(definition)
                assert ("user-id" in normalized_def or "user_id" in normalized_def)
                assert ("user name" in normalized_def or "user_name" in normalized_def)

                logger.info("Reflected view with special characters: %d columns", len(view_table.columns))

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
        """Test reflection of a non-existent view raises NoSuchTableError."""
        from sqlalchemy.exc import NoSuchTableError
        sr_engine = sr_root_engine

        with sr_engine.connect() as connection:
            inspector = inspect(connection)

            # Should raise NoSuchTableError for non-existent view
            with pytest.raises(NoSuchTableError):
                inspector.get_view("nonexistent_view_12345", schema=sr_engine.url.database)

            logger.info("Correctly raised NoSuchTableError for non-existent view")

    def test_reflect_view_name_case_sensitivity(self, sr_root_engine: Engine):
        """Test view reflection with different case sensitivity scenarios using Table.autoload_with."""
        view_name_lower = "test_reflect_view_case"
        view_name_upper = "TEST_REFLECT_VIEW_CASE"
        sr_engine = sr_root_engine
        metadata = MetaData()

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
                # Test reflection with exact case
                view_table = Table(view_name_lower, metadata, autoload_with=sr_engine)
                assert view_table.info.get('table_kind') == 'VIEW'
                assert view_table.name == view_name_lower
                assert len(view_table.columns) == 2

                # Test reflection with different case (may or may not work depending on DB settings)
                metadata2 = MetaData()
                try:
                    view_table_upper = Table(view_name_upper, metadata2, autoload_with=sr_engine)
                    logger.info("Reflected view with different case: %s", view_table_upper.name)
                except Exception:
                    # Case-sensitive databases will raise an error
                    logger.info("Different case not found (case-sensitive database)")

                logger.info("Reflected view case sensitivity test: %s", view_table.name)

            finally:
                connection.execute(text(f"DROP VIEW IF EXISTS {view_name_lower}"))
                connection.execute(text(f"DROP VIEW IF EXISTS {view_name_upper}"))

    def test_reflect_view_via_table_autoload(self, sr_root_engine: Engine):
        """
        End-to-end test for view reflection using the primary Table.autoload_with mechanism.

        This test verifies that a view is correctly and fully reflected into a
        SQLAlchemy Table object, including its kind, definition, comment, dialect options,
        and column details, aligning with the core design goals.
        """
        view_name = "test_view_for_autoload"
        sr_engine = sr_root_engine
        metadata = MetaData()

        with sr_engine.connect() as connection:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

            create_view_sql = f"""
            CREATE VIEW {view_name} (
                user_id COMMENT 'User primary key',
                user_name COMMENT 'User screen name'
            )
            COMMENT 'A comprehensive view for autoload testing'
            SECURITY INVOKER
            AS
            SELECT id, name FROM users WHERE active = TRUE
            """
            connection.execute(text(create_view_sql))

            try:
                # Use Table.autoload_with to reflect the view
                view_table = Table(view_name, metadata, autoload_with=sr_engine)

                # 1. Verify table-level attributes
                assert view_table.info.get("table_kind") == "VIEW"
                assert view_table.comment == "A comprehensive view for autoload testing"

                # 2. Verify definition
                expected_def = "SELECT `id`, `name` FROM `users` WHERE `active` = TRUE"
                assert TableAttributeNormalizer.normalize_sql(
                    view_table.info.get("definition"), remove_qualifiers=True
                ) == TableAttributeNormalizer.normalize_sql(expected_def)

                # 3. Verify dialect-specific options (security is now correctly reflected from SHOW CREATE VIEW)
                security = get_dialect_option(view_table, TableInfoKey.SECURITY)
                assert security == "INVOKER", f"Expected security='INVOKER', got '{security}'"

                # 4. Verify reflected columns (name and comment)
                assert len(view_table.c) == 2
                assert "user_id" in view_table.c
                assert "user_name" in view_table.c

                assert view_table.c.user_id.comment == "User primary key"
                assert view_table.c.user_name.comment == "User screen name"

                logger.info(f"Successfully reflected view via autoload: {view_name}")

            finally:
                connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_autoload_verifies_column_types(self, sr_root_engine: Engine):
        """
        Tests that Table.autoload_with correctly reflects the data types
        of view columns as inferred from the SELECT statement.
        """
        view_name = "test_view_column_types_autoload"
        sr_engine = sr_root_engine
        metadata = MetaData()

        with sr_engine.connect() as connection:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
            connection.execute(text(f"DROP TABLE IF EXISTS type_test_table"))
            connection.execute(
                text(
                    """
                CREATE TABLE type_test_table (
                    c_int INT,
                    c_str VARCHAR(100),
                    c_bool BOOLEAN,
                    c_decimal DECIMAL(10, 2),
                    c_date DATE
                )
                DISTRIBUTED BY HASH(c_int)
                PROPERTIES ("replication_num" = "1")
                """
                )
            )

            create_view_sql = f"""
            CREATE VIEW {view_name} AS
            SELECT c_int, c_str, c_bool, c_decimal, c_date
            FROM type_test_table
            """
            connection.execute(text(create_view_sql))

            try:
                view_table = Table(view_name, metadata, autoload_with=sr_engine)

                # Verify column types
                assert isinstance(view_table.c.c_int.type, INTEGER)
                assert isinstance(view_table.c.c_str.type, VARCHAR)
                assert view_table.c.c_str.type.length == 100
                assert isinstance(view_table.c.c_bool.type, TINYINT)
                assert isinstance(view_table.c.c_decimal.type, DECIMAL)
                assert view_table.c.c_decimal.type.precision == 10
                assert view_table.c.c_decimal.type.scale == 2
                assert isinstance(view_table.c.c_date.type, DATE)

                logger.info("Successfully verified reflected column types via autoload.")

            finally:
                connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
                connection.execute(text(f"DROP TABLE IF EXISTS type_test_table"))

    def test_reflect_comprehensive_view_with_all_attributes(self, sr_root_engine: Engine):
        """
        Comprehensive test: reflection of a view with ALL attributes combined using Table.autoload_with.

        This test verifies that a view with comment, security, explicit columns,
        and a complex definition (including window functions, CTE, joins, aggregations)
        is correctly reflected as a Table object.
        """
        view_name = "test_comprehensive_view"
        sr_engine = sr_root_engine
        metadata = MetaData()

        with sr_engine.connect() as connection:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

            # Create a comprehensive view with all features
            create_view_sql = f"""
            CREATE VIEW {view_name} (
                user_id COMMENT 'User identifier',
                user_name COMMENT 'User display name',
                total_amount COMMENT 'Total order amount',
                order_count COMMENT 'Number of orders',
                row_num COMMENT 'Row number within partition'
            )
            COMMENT 'Comprehensive view with all attributes for testing'
            SECURITY INVOKER
            AS
            WITH user_stats AS (
                SELECT
                    u.id,
                    u.name,
                    COUNT(o.id) as cnt,
                    SUM(o.amount) as total
                FROM users u
                LEFT JOIN orders o ON u.id = o.user_id
                WHERE u.active = TRUE
                GROUP BY u.id, u.name
                HAVING COUNT(o.id) > 0
            )
            SELECT
                us.id,
                us.name,
                us.total,
                us.cnt,
                ROW_NUMBER() OVER (ORDER BY us.total DESC) as rn
            FROM user_stats us
            ORDER BY us.total DESC
            """
            connection.execute(text(create_view_sql))

            try:
                # Reflect the view as a Table object
                view_table = Table(view_name, metadata, autoload_with=sr_engine)

                # 1. Verify it's recognized as a VIEW
                assert view_table.info.get('table_kind') == 'VIEW'
                assert view_table.name == view_name

                # 2. Verify comment
                assert view_table.comment == "Comprehensive view with all attributes for testing"

                # 3. Verify security attribute is correctly reflected from SHOW CREATE VIEW
                security = get_dialect_option(view_table, TableInfoKey.SECURITY)
                assert security == 'INVOKER', f"Expected security='INVOKER', got '{security}'"
                logger.info(f"Security attribute: {security}")

                # 4. Verify definition contains all complex elements
                definition = view_table.info.get('definition', '')
                normalized_def = TableAttributeNormalizer.normalize_sql(definition)
                # CTE
                assert "with" in normalized_def
                assert "user_stats" in normalized_def
                # JOIN
                assert "left" in normalized_def and "join" in normalized_def
                # Aggregations
                assert "count" in normalized_def
                assert "sum" in normalized_def
                # WHERE and HAVING
                assert "where" in normalized_def
                assert "having" in normalized_def
                # GROUP BY
                assert "group by" in normalized_def
                # Window function
                assert "row_number()" in normalized_def
                assert "over (" in normalized_def
                # ORDER BY
                assert "order by" in normalized_def

                # 5. Verify columns are reflected
                assert len(view_table.columns) == 5
                col_names = [col.name for col in view_table.columns]
                assert 'user_id' in col_names
                assert 'user_name' in col_names
                assert 'total_amount' in col_names
                assert 'order_count' in col_names
                assert 'row_num' in col_names

                logger.info(f"Reflected columns: {col_names}")

                logger.info(f"Successfully reflected comprehensive view: {view_name} with {len(view_table.columns)} columns")

            finally:
                connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
