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

from alembic import op
from alembic.testing.fixtures import op_fixture
from sqlalchemy import Column, MetaData, Table, exc, func, literal, select, testing
from sqlalchemy.schema import CreateTable, DropTable
from sqlalchemy.testing import eq_, expect_raises_message, fixtures
from sqlalchemy.testing.assertions import AssertsCompiledSQL

import starrocks.alembic  # noqa: F401
from starrocks.common.types import ViewSecurityType
from starrocks.datatype import INTEGER, VARCHAR
from starrocks.dialect import StarRocksDialect
from starrocks.sql.ddl import AlterView, AlterViewSecurity, CreateView, DropView
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

    def test_selectable_definition_is_compiled_for_starrocks(self):
        """Names are backquoted as StarRocks reads them, not double-quoted, and a ``%`` is single in the definition."""
        orders = Table("orders", MetaData(), Column("Id", INTEGER), Column("select", VARCHAR(10)), Column("d", VARCHAR(10)))
        view = View(
            "v_orders",
            MetaData(),
            definition=select(orders.c.Id, orders.c.select, func.date_format(orders.c.d, literal("%Y")).label("y")),
        )
        eq_(view.definition, "SELECT orders.`Id`, orders.`select`, date_format(orders.d, '%Y') AS y \nFROM orders")
        self.assert_compile(
            CreateView(view),
            "CREATE VIEW v_orders" "AS" "SELECT orders.`Id`, orders.`select`, date_format(orders.d, '%%Y') AS y FROM orders",
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


class ViewCommentCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    """A view comment is rendered as an escaped string literal, as table and column comments are."""

    __dialect__ = StarRocksDialect()
    __requires__ = ("sqlalchemy_2",)

    @testing.combinations(
        ("single_quote", "it's a view", "COMMENT 'it''s a view'"),
        ("backslash", r"C:\temp", r"COMMENT 'C:\\temp'"),
        argnames="comment, rendered",
        id_="iaa",
    )
    def test_view_comment_escaping(self, comment, rendered):
        view = View("my_view", MetaData(), definition="SELECT 1 AS one", comment=comment)
        self.assert_compile(CreateView(view), "CREATE VIEW my_view" f"{rendered}" "AS" "SELECT 1 AS one")


class ViewSecurityCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    """StarRocks views have ``SECURITY NONE`` or ``SECURITY INVOKER``; anything else raises before the server."""

    __dialect__ = StarRocksDialect()
    __requires__ = ("sqlalchemy_2",)

    @testing.combinations(("NONE", "NONE"), ("invoker", "INVOKER"), argnames="security, keyword")
    def test_security(self, security, keyword):
        view = View("my_view", MetaData(), definition="SELECT 1 AS one", starrocks_security=security)
        self.assert_compile(CreateView(view), "CREATE VIEW my_view" f"SECURITY {keyword}" "AS" "SELECT 1 AS one")

    @testing.combinations(
        ("text", "DEFINER"), ("deprecated_name", ViewSecurityType.DEFINER), argnames="security", id_="ia"
    )
    def test_definer_raises(self, security):
        """``ViewSecurityType.DEFINER`` still imports, and is refused as the text is; StarRocks has no DEFINER."""
        view = View("my_view", MetaData(), definition="SELECT 1 AS one", starrocks_security=security)
        with expect_raises_message(exc.CompileError, "StarRocks views have SECURITY NONE or INVOKER, not 'DEFINER'"):
            CreateView(view).compile(dialect=self.__dialect__)
        assert security not in ViewSecurityType.ALLOWED_ITEMS


class ViewInMetaDataCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    """``MetaData.create_all()`` and ``drop_all()`` emit ``CreateTable`` and ``DropTable`` for a View."""

    __dialect__ = StarRocksDialect()
    __requires__ = ("sqlalchemy_2",)

    def test_create_table_of_view(self):
        view = View("my_view", MetaData(), definition="SELECT * FROM my_table", comment="c")
        self.assert_compile(CreateTable(view), "CREATE VIEW my_view" "COMMENT 'c'" "AS" "SELECT * FROM my_table")

    @testing.combinations((False, "DROP VIEW my_view"), (True, "DROP VIEW IF EXISTS my_view"), argnames="if_exists, expected")
    def test_drop_table_of_view(self, if_exists, expected):
        view = View("my_view", MetaData(), definition="SELECT * FROM my_table")
        self.assert_compile(DropTable(view, if_exists=if_exists), expected)


class AlterViewCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = StarRocksDialect()
    __requires__ = ("sqlalchemy_2",)

    def test_alter_view_ignores_comment_and_security(self):
        """StarRocks cannot change a view's COMMENT or SECURITY with ALTER VIEW, so only the definition is rendered."""
        view = View("my_view", MetaData(), definition="SELECT 2", comment="New Comment", starrocks_security="INVOKER")
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


class AlterViewSecurityCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = StarRocksDialect()
    __requires__ = ("sqlalchemy_2",)

    @testing.combinations(("none", "NONE"), ("invoker", "invoker"), argnames="security, keyword", id_="aa")
    def test_alter_view_security(self, security, keyword):
        view = View("my_view", MetaData(), definition="SELECT 1", schema="test_db")
        self.assert_compile(
            AlterViewSecurity(view, security), f"ALTER VIEW test_db.my_view SET SECURITY {keyword.upper()}"
        )

    def test_alter_view_security_refuses_definer(self):
        view = View("my_view", MetaData(), definition="SELECT 1")
        with expect_raises_message(exc.CompileError, "SECURITY NONE or INVOKER"):
            AlterViewSecurity(view, "DEFINER").compile(dialect=self.__dialect__)


class AlterViewOperationTest(fixtures.TestBase):
    """``op.alter_view`` emits the statements that apply what it is given."""

    __requires__ = ("sqlalchemy_2",)

    def test_definition_only_is_alter_view_as(self):
        context = op_fixture("starrocks")
        op.alter_view("v", "SELECT 2")
        context.assert_("ALTER VIEW vASSELECT 2")

    def test_definition_with_columns_and_no_comment_is_alter_view_as(self):
        context = op_fixture("starrocks")
        op.alter_view("v", "SELECT 2 AS a", columns=[{"name": "a", "comment": "col a"}])
        context.assert_("ALTER VIEW v (    a COMMENT 'col a')ASSELECT 2 AS a")

    def test_comment_recreates_the_view_with_its_comment(self):
        context = op_fixture("starrocks")
        op.alter_view("v", "SELECT 2", comment="the comment", schema="test_db")
        context.assert_("CREATE OR REPLACE VIEW test_db.vCOMMENT 'the comment'ASSELECT 2")

    def test_removed_comment_recreates_the_view_without_one(self):
        context = op_fixture("starrocks")
        op.alter_view("v", "SELECT 2", existing_comment="old")
        context.assert_("CREATE OR REPLACE VIEW vASSELECT 2")

    def test_comment_only_recreates_the_view_from_its_existing_definition(self):
        context = op_fixture("starrocks")
        op.alter_view(
            "v",
            comment="new",
            existing_definition="SELECT 1 AS a",
            existing_columns=[{"name": "a", "comment": "col a"}],
        )
        context.assert_("CREATE OR REPLACE VIEW v (    a COMMENT 'col a')COMMENT 'new'ASSELECT 1 AS a")

    def test_comment_without_any_definition_is_refused(self):
        op_fixture("starrocks")
        with expect_raises_message(exc.CompileError, "needs the view's definition or existing_definition"):
            op.alter_view("v", comment="new")

    @testing.combinations(
        ("security", {"security": "INVOKER"}),
        ("starrocks_security", {"starrocks_security": "INVOKER"}),
        argnames="kwargs",
        id_="ia",
    )
    def test_security_only_is_set_security(self, kwargs):
        context = op_fixture("starrocks")
        op.alter_view("v", **kwargs)
        context.assert_("ALTER VIEW v SET SECURITY INVOKER")

    def test_security_unchanged_from_the_existing_one_is_not_set(self):
        context = op_fixture("starrocks")
        op.alter_view("v", "SELECT 2", security="INVOKER", existing_security="invoker")
        context.assert_("ALTER VIEW vASSELECT 2")

    def test_definition_and_security_are_two_statements(self):
        context = op_fixture("starrocks")
        op.alter_view("v", "SELECT 2", starrocks_security="NONE")
        context.assert_("ALTER VIEW vASSELECT 2", "ALTER VIEW v SET SECURITY NONE")

    def test_conflicting_security_spellings_are_refused(self):
        op_fixture("starrocks")
        with expect_raises_message(TypeError, "which are the same option"):
            op.alter_view("v", security="NONE", starrocks_security="INVOKER")

    def test_security_the_server_lacks_stops_the_op_before_the_definition_changes(self):
        """StarRocks before 3.4.1 has no view SECURITY, so the op sends nothing, rather than an ALTER VIEW ... AS."""
        context = op_fixture("starrocks")
        context.impl.dialect.server_version_info = (3, 3, 22)
        with expect_raises_message(exc.CompileError, r"View SECURITY \(view 'v'\) needs StarRocks 3.4.1"):
            op.alter_view("v", "SELECT 2 AS a", security="INVOKER")
        context.assert_()
