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

from sqlalchemy import Column, Table, select
from sqlalchemy.dialects import registry
from sqlalchemy.schema import MetaData

from starrocks.datatype import INTEGER, VARCHAR
from starrocks.sql.ddl import AlterView, CreateView, DropView
from starrocks.sql.schema import View
from test.test_utils import normalize_sql


class TestViewCompilerBase:
    """Base class for view compiler tests with shared setup."""

    @classmethod
    def setup_class(cls):
        cls.logger = logging.getLogger(__name__)
        cls.dialect = registry.load("starrocks")()
        cls.metadata = MetaData()


class TestCreateViewCompiler(TestViewCompilerBase):
    """Tests for CREATE VIEW statements."""

    def test_create_view(self):
        view = View("my_view", self.metadata, definition="SELECT * FROM my_table")
        sql = str(CreateView(view).compile(dialect=self.dialect))
        expected = "CREATE VIEW my_view AS SELECT * FROM my_table"
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_create_view_from_selectable(self):
        """Test CREATE VIEW with SQLAlchemy Selectable object."""
        # Create a source table
        users = Table('users', self.metadata,
                     Column('id', INTEGER),
                     Column('name', VARCHAR(50)),
                     Column('email', VARCHAR(100)))

        # Create a select statement
        stmt = select(users.c.id, users.c.name).where(users.c.id > 100)

        # Create view from selectable
        view = View("active_users", self.metadata, definition=stmt)
        sql = str(CreateView(view).compile(dialect=self.dialect))

        # Verify the SQL contains the expected elements
        normalized = normalize_sql(sql)
        assert "CREATE VIEW active_users AS" in normalized
        assert "SELECT" in normalized
        assert "users.id" in normalized or "id" in normalized
        assert "users.name" in normalized or "name" in normalized
        assert "users.id > 100" in normalized or "id > 100" in normalized

    def test_create_view_variations(self):
        view = View("simple_view", self.metadata, definition="SELECT c1, c2, c3 FROM test_table")
        sql = str(CreateView(view).compile(dialect=self.dialect))
        assert normalize_sql(sql) == normalize_sql(
            "CREATE VIEW simple_view AS SELECT c1, c2, c3 FROM test_table"
        )

        view = View("simple_view_or_replace", self.metadata, definition="SELECT c1, c2, c3 FROM test_table")
        sql = str(CreateView(view, or_replace=True).compile(dialect=self.dialect))
        assert normalize_sql(sql) == normalize_sql(
            "CREATE OR REPLACE VIEW simple_view_or_replace AS SELECT c1, c2, c3 FROM test_table"
        )

        view = View("simple_view_if_not_exists", self.metadata, definition="SELECT c1 FROM test_table")
        sql = str(CreateView(view, if_not_exists=True).compile(dialect=self.dialect))
        assert normalize_sql(sql) == normalize_sql(
            "CREATE VIEW IF NOT EXISTS simple_view_if_not_exists AS SELECT c1 FROM test_table"
        )

        view = View("simple_view_with_schema", self.metadata, definition="SELECT c1 FROM test_table", schema="test_db")
        sql = str(CreateView(view).compile(dialect=self.dialect))
        assert normalize_sql(sql) == normalize_sql(
            "CREATE VIEW test_db.simple_view_with_schema AS SELECT c1 FROM test_table"
        )

        view = View(
            "commented_view",
            self.metadata,
            definition="SELECT c1, c2 FROM test_table",
            comment="This is a view with a comment",
        )
        sql = str(CreateView(view).compile(dialect=self.dialect))
        assert normalize_sql(sql) == normalize_sql(
            "CREATE VIEW commented_view COMMENT 'This is a view with a comment' AS SELECT c1, c2 FROM test_table"
        )

    def test_create_view_with_security(self):
        view = View("secure_view", self.metadata, definition="SELECT 1", starrocks_security="INVOKER")
        sql = str(CreateView(view).compile(dialect=self.dialect))
        assert normalize_sql(sql) == normalize_sql(
            "CREATE VIEW secure_view SECURITY INVOKER AS SELECT 1"
        )

    def test_create_view_with_column_definitions(self):
        """Test CREATE VIEW with various ways to define columns."""
        # 1. Columns as a list of strings
        view = View(
            "view_with_str_cols",
            self.metadata,
            definition="SELECT c1, c2 FROM test_table",
            columns=["col_a", "col_b"],
        )
        sql = str(CreateView(view).compile(dialect=self.dialect))
        assert normalize_sql(sql) == normalize_sql(
            "CREATE VIEW view_with_str_cols(col_a, col_b) AS SELECT c1, c2 FROM test_table"
        )

        # 2. Columns as a list of dictionaries with comments
        view = View(
            "view_with_dict_cols",
            self.metadata,
            definition="SELECT c1, c2 FROM test_table",
            columns=[
                {"name": "col_a", "comment": "This is the first column"},
                {"name": "col_b", "comment": "This is the second column"},
            ],
        )
        sql = str(CreateView(view).compile(dialect=self.dialect))
        expected = (
            "CREATE VIEW view_with_dict_cols("
            "col_a COMMENT 'This is the first column', "
            "col_b COMMENT 'This is the second column') "
            "AS SELECT c1, c2 FROM test_table"
        )
        assert normalize_sql(sql) == normalize_sql(expected)

        # 3. Columns as SQLAlchemy Column objects (with and without comments)
        cols = [
            Column('id', INTEGER),
            Column('name', VARCHAR(50), comment='User name'),
            Column('email', VARCHAR(100))
        ]
        view = View("user_view_sqla_cols", self.metadata, *cols, definition="SELECT id, name, email FROM users")
        sql = str(CreateView(view).compile(dialect=self.dialect))
        expected = (
            "CREATE VIEW user_view_sqla_cols "
            "(id, name COMMENT 'User name', email) "
            "AS SELECT id, name, email FROM users"
        )
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_create_view_with_combined_attributes(self):
        """Test CREATE VIEW with a combination of columns, comment, and security."""
        cols = [
            Column('id', INTEGER, comment='User ID'),
            Column('name', VARCHAR(50), comment='User name')
        ]
        view = View(
            "user_view_combined",
            self.metadata,
            *cols,
            definition="SELECT id, name FROM users",
            schema='test_db',
            comment='A comprehensive user view',
            starrocks_security='INVOKER'
        )
        sql = str(CreateView(view).compile(dialect=self.dialect))
        expected = (
            "CREATE VIEW test_db.user_view_combined "
            "(id COMMENT 'User ID', name COMMENT 'User name') "
            "COMMENT 'A comprehensive user view' "
            "SECURITY INVOKER "
            "AS SELECT id, name FROM users"
        )
        assert normalize_sql(sql) == normalize_sql(expected)


class TestDropViewCompiler(TestViewCompilerBase):
    """Tests for DROP VIEW statements."""

    def test_drop_view(self):
        view = View("my_view", self.metadata, definition="SELECT * FROM my_table")
        sql = str(DropView(view).compile(dialect=self.dialect))
        assert normalize_sql(sql) == normalize_sql("DROP VIEW my_view")

    def test_drop_view_if_exists(self):
        view = View("my_view", self.metadata, definition="SELECT * FROM my_table")
        sql = str(DropView(view, if_exists=True).compile(dialect=self.dialect))
        assert normalize_sql(sql) == normalize_sql("DROP VIEW IF EXISTS my_view")


class TestAlterViewCompiler(TestViewCompilerBase):
    """Tests for ALTER VIEW statements."""

    def test_alter_view(self):
        view = View("my_view", self.metadata, definition="SELECT 2", comment="New Comment", starrocks_security="DEFINER")
        sql = str(AlterView(view).compile(dialect=self.dialect))
        expected = """
        ALTER VIEW my_view
        AS
        SELECT 2
        """
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_alter_view_with_schema(self):
        """Test ALTER VIEW with schema qualification."""
        view = View("my_view", self.metadata, definition="SELECT id, name FROM users", schema="test_db")
        sql = str(AlterView(view).compile(dialect=self.dialect))
        expected = """
        ALTER VIEW test_db.my_view
        AS
        SELECT id, name FROM users
        """
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_alter_view_complex_query(self):
        """Test ALTER VIEW with complex SELECT query."""
        complex_query = """
        SELECT u.id, u.name, COUNT(o.id) as order_count
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.active = true
        GROUP BY u.id, u.name
        HAVING COUNT(o.id) > 0
        ORDER BY order_count DESC
        """
        view = View("user_order_summary", self.metadata, definition=complex_query)
        sql = str(AlterView(view).compile(dialect=self.dialect))
        expected = f"""
        ALTER VIEW user_order_summary
        AS
        {complex_query.strip()}
        """
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_alter_view_with_subquery(self):
        """Test ALTER VIEW with subqueries and CTEs."""
        query_with_subquery = """
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
        view = View("user_monthly_stats", self.metadata, definition=query_with_subquery)
        sql = str(AlterView(view).compile(dialect=self.dialect))
        expected = f"""
        ALTER VIEW user_monthly_stats
        AS
        {query_with_subquery.strip()}
        """
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_alter_view_with_special_chars(self):
        """Test ALTER VIEW with special characters in column names and values."""
        special_query = """
        SELECT `user-id`, `user name`, `email@domain.com`
        FROM `user-table`
        WHERE `status` = 'active' AND `created-at` > '2023-01-01'
        """
        view = View("special_chars_view", self.metadata, definition=special_query)
        sql = str(AlterView(view).compile(dialect=self.dialect))
        expected = f"""
        ALTER VIEW special_chars_view
        AS
        {special_query.strip()}
        """
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_alter_view_with_window_functions(self):
        """Test ALTER VIEW with window functions."""
        window_query = """
        SELECT
            user_id,
            order_date,
            amount,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_date DESC) as rn,
            SUM(amount) OVER (PARTITION BY user_id ORDER BY order_date) as running_total
        FROM orders
        """
        view = View("user_order_window", self.metadata, definition=window_query)
        sql = str(AlterView(view).compile(dialect=self.dialect))
        expected = f"""
        ALTER VIEW user_order_window
        AS
        {window_query.strip()}
        """
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_alter_view_with_joins(self):
        """Test ALTER VIEW with various JOIN types."""
        join_query = """
        SELECT
            u.id,
            u.name,
            p.title as product_name,
            o.quantity,
            o.price
        FROM users u
        INNER JOIN orders o ON u.id = o.user_id
        LEFT JOIN products p ON o.product_id = p.id
        RIGHT JOIN categories c ON p.category_id = c.id
        """
        view = View("user_order_details", self.metadata, definition=join_query)
        sql = str(AlterView(view).compile(dialect=self.dialect))
        expected = f"""
        ALTER VIEW user_order_details
        AS
        {join_query.strip()}
        """
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_alter_view_with_aggregation(self):
        """Test ALTER VIEW with aggregation functions."""
        agg_query = """
        SELECT
            category_id,
            COUNT(*) as product_count,
            AVG(price) as avg_price,
            MIN(price) as min_price,
            MAX(price) as max_price,
            SUM(stock_quantity) as total_stock
        FROM products
        GROUP BY category_id
        """
        view = View("category_summary", self.metadata, definition=agg_query)
        sql = str(AlterView(view).compile(dialect=self.dialect))
        expected = f"""
        ALTER VIEW category_summary
        AS
        {agg_query.strip()}
        """
        assert normalize_sql(sql) == normalize_sql(expected)


