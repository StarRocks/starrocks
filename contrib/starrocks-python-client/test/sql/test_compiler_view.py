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

"""CREATE, DROP and ALTER VIEW compilation.

The dialect puts the column list, each view clause and ``AS`` on lines of their own; ``assert_compile`` drops the
line breaks, so each expected string literal below is one line of the generated DDL.
"""

from sqlalchemy import Column, MetaData, Table, select, testing
from sqlalchemy.testing import eq_, fixtures
from sqlalchemy.testing.assertions import AssertsCompiledSQL

from starrocks.datatype import INTEGER, VARCHAR
from starrocks.dialect import StarRocksDialect
from starrocks.sql.ddl import AlterView, CreateView, DropView
from starrocks.sql.schema import View


class CreateViewCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = StarRocksDialect()
    __requires__ = ("sqlalchemy_2",)

    def test_create_view(self):
        view = View("my_view", MetaData(), definition="SELECT * FROM my_table")
        self.assert_compile(CreateView(view), "CREATE VIEW my_view" "AS" "SELECT * FROM my_table")

    def test_create_view_from_selectable(self):
        users = Table(
            "users", MetaData(), Column("id", INTEGER), Column("name", VARCHAR(50)), Column("email", VARCHAR(100))
        )
        view = View(
            "active_users", MetaData(), definition=select(users.c.id, users.c.name).where(users.c.id > 100)
        )
        self.assert_compile(
            CreateView(view),
            "CREATE VIEW active_users" "AS" "SELECT users.id, users.name FROM users WHERE users.id > 100",
        )

    @testing.combinations(
        ("plain", {}, {}, "CREATE VIEW v" "AS" "SELECT c1 FROM test_table"),
        ("or_replace", {}, {"or_replace": True}, "CREATE OR REPLACE VIEW v" "AS" "SELECT c1 FROM test_table"),
        ("if_not_exists", {}, {"if_not_exists": True}, "CREATE VIEW IF NOT EXISTS v" "AS" "SELECT c1 FROM test_table"),
        ("schema", {"schema": "test_db"}, {}, "CREATE VIEW test_db.v" "AS" "SELECT c1 FROM test_table"),
        (
            "comment",
            {"comment": "This is a view with a comment"},
            {},
            "CREATE VIEW v" "COMMENT 'This is a view with a comment'" "AS" "SELECT c1 FROM test_table",
        ),
        (
            "security",
            {"starrocks_security": "INVOKER"},
            {},
            "CREATE VIEW v" "SECURITY INVOKER" "AS" "SELECT c1 FROM test_table",
        ),
        argnames="view_kwargs, create_kwargs, expected",
        id_="iaaa",
    )
    def test_create_view_option(self, view_kwargs, create_kwargs, expected):
        view = View("v", MetaData(), definition="SELECT c1 FROM test_table", **view_kwargs)
        self.assert_compile(CreateView(view, **create_kwargs), expected)

    def test_create_view_with_column_names(self):
        view = View("v", MetaData(), definition="SELECT c1, c2 FROM test_table", columns=["col_a", "col_b"])
        self.assert_compile(
            CreateView(view),
            "CREATE VIEW v ("
            "    col_a,"
            "    col_b"
            ")"
            "AS"
            "SELECT c1, c2 FROM test_table",
        )

    def test_create_view_with_column_dicts(self):
        view = View(
            "v",
            MetaData(),
            definition="SELECT c1, c2 FROM test_table",
            columns=[
                {"name": "col_a", "comment": "This is the first column"},
                {"name": "col_b", "comment": "This is the second column"},
            ],
        )
        self.assert_compile(
            CreateView(view),
            "CREATE VIEW v ("
            "    col_a COMMENT 'This is the first column',"
            "    col_b COMMENT 'This is the second column'"
            ")"
            "AS"
            "SELECT c1, c2 FROM test_table",
        )

    def test_create_view_with_column_objects(self):
        view = View(
            "v",
            MetaData(),
            Column("id", INTEGER),
            Column("name", VARCHAR(50), comment="User name"),
            Column("email", VARCHAR(100)),
            definition="SELECT id, name, email FROM users",
        )
        self.assert_compile(
            CreateView(view),
            "CREATE VIEW v ("
            "    id,"
            "    name COMMENT 'User name',"
            "    email"
            ")"
            "AS"
            "SELECT id, name, email FROM users",
        )

    def test_create_view_with_combined_attributes(self):
        view = View(
            "user_view_combined",
            MetaData(),
            Column("id", INTEGER, comment="User ID"),
            Column("name", VARCHAR(50), comment="User name"),
            definition="SELECT id, name FROM users",
            schema="test_db",
            comment="A comprehensive user view",
            starrocks_security="INVOKER",
        )
        self.assert_compile(
            CreateView(view),
            "CREATE VIEW test_db.user_view_combined ("
            "    id COMMENT 'User ID',"
            "    name COMMENT 'User name'"
            ")"
            "COMMENT 'A comprehensive user view'"
            "SECURITY INVOKER"
            "AS"
            "SELECT id, name FROM users",
        )


class DropViewCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = StarRocksDialect()
    __requires__ = ("sqlalchemy_2",)

    def test_drop_view(self):
        view = View("my_view", MetaData(), definition="SELECT * FROM my_table")
        self.assert_compile(DropView(view), "DROP VIEW my_view")

    def test_drop_view_if_exists(self):
        view = View("my_view", MetaData(), definition="SELECT * FROM my_table")
        self.assert_compile(DropView(view, if_exists=True), "DROP VIEW IF EXISTS my_view")


class AlterViewCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = StarRocksDialect()
    __requires__ = ("sqlalchemy_2",)

    def test_alter_view_ignores_comment_and_security(self):
        """StarRocks cannot change a view's COMMENT or SECURITY with ALTER VIEW, so only the definition is rendered."""
        view = View("my_view", MetaData(), definition="SELECT 2", comment="New Comment", starrocks_security="DEFINER")
        self.assert_compile(AlterView(view), "ALTER VIEW my_view" "AS" "SELECT 2")

    def test_alter_view_with_schema(self):
        view = View("my_view", MetaData(), definition="SELECT id, name FROM users", schema="test_db")
        self.assert_compile(AlterView(view), "ALTER VIEW test_db.my_view" "AS" "SELECT id, name FROM users")

    @testing.combinations(
        (
            "group_by_having",
            (
                "SELECT u.id, u.name, COUNT(o.id) as order_count",
                "FROM users u",
                "LEFT JOIN orders o ON u.id = o.user_id",
                "WHERE u.active = true",
                "GROUP BY u.id, u.name",
                "HAVING COUNT(o.id) > 0",
                "ORDER BY order_count DESC",
            ),
        ),
        (
            "cte",
            (
                "WITH monthly_stats AS (",
                "    SELECT user_id, COUNT(*) as monthly_orders",
                "    FROM orders",
                "    WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)",
                "    GROUP BY user_id",
                ")",
                "SELECT u.name, COALESCE(ms.monthly_orders, 0) as orders",
                "FROM users u",
                "LEFT JOIN monthly_stats ms ON u.id = ms.user_id",
            ),
        ),
        (
            "quoted_identifiers",
            (
                "SELECT `user-id`, `user name`, `email@domain.com`",
                "FROM `user-table`",
                "WHERE `status` = 'active' AND `created-at` > '2023-01-01'",
            ),
        ),
        (
            "window_functions",
            (
                "SELECT",
                "    user_id,",
                "    order_date,",
                "    amount,",
                "    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_date DESC) as rn,",
                "    SUM(amount) OVER (PARTITION BY user_id ORDER BY order_date) as running_total",
                "FROM orders",
            ),
        ),
        (
            "joins",
            (
                "SELECT",
                "    u.id,",
                "    u.name,",
                "    p.title as product_name,",
                "    o.quantity,",
                "    o.price",
                "FROM users u",
                "INNER JOIN orders o ON u.id = o.user_id",
                "LEFT JOIN products p ON o.product_id = p.id",
                "RIGHT JOIN categories c ON p.category_id = c.id",
            ),
        ),
        (
            "aggregation",
            (
                "SELECT",
                "    category_id,",
                "    COUNT(*) as product_count,",
                "    AVG(price) as avg_price,",
                "    MIN(price) as min_price,",
                "    MAX(price) as max_price,",
                "    SUM(stock_quantity) as total_stock",
                "FROM products",
                "GROUP BY category_id",
            ),
        ),
        argnames="definition_lines",
        id_="ia",
    )
    def test_alter_view_keeps_definition_verbatim(self, definition_lines):
        """assert_compile ignores line breaks, so the exact text, line breaks included, is compared as well."""
        definition = "\n".join(definition_lines)
        view = View("v", MetaData(), definition=definition)
        self.assert_compile(AlterView(view), "ALTER VIEW v" "AS" + "".join(definition_lines))
        eq_(str(AlterView(view).compile(dialect=self.__dialect__)), "ALTER VIEW v\nAS\n" + definition)
