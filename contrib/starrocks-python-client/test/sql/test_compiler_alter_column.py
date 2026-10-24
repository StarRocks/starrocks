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

"""How the dialect compiles Alembic's MySQL column-alter elements: MODIFY COLUMN, CHANGE COLUMN and ALTER DEFAULT."""

from alembic import op
from alembic.ddl.base import ColumnName
from alembic.ddl.mysql import MySQLAlterDefault, MySQLChangeColumn, MySQLModifyColumn
from alembic.testing.fixtures import op_fixture
from sqlalchemy import exc, testing, text
from sqlalchemy.testing import expect_raises_message, fixtures
from sqlalchemy.testing.assertions import AssertsCompiledSQL

import starrocks.alembic  # noqa: F401
from starrocks.datatype import BIGINT, DATETIME, INTEGER, STRING
from starrocks.dialect import StarRocksDialect


class AlterColumnCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = StarRocksDialect()

    def test_modify_type_and_nullable(self):
        self.assert_compile(
            MySQLModifyColumn(
                "t", "c", schema=None, type_=INTEGER(), nullable=False, default=False, autoincrement=None, comment=False
            ),
            "ALTER TABLE t MODIFY COLUMN c INTEGER NOT NULL",
        )

    def test_modify_type_nullable_default_comment(self):
        self.assert_compile(
            MySQLModifyColumn(
                "t",
                "c",
                schema=None,
                type_=STRING,
                nullable=True,
                default="abc",
                autoincrement=None,
                comment="greeting",
            ),
            "ALTER TABLE t MODIFY COLUMN c STRING NULL DEFAULT 'abc' COMMENT 'greeting'",
        )

    def test_modify_schema_qualified(self):
        self.assert_compile(
            MySQLModifyColumn(
                "t", "c", schema="s", type_=INTEGER(), nullable=True, default=False, autoincrement=None, comment=False
            ),
            "ALTER TABLE s.t MODIFY COLUMN c INTEGER NULL",
        )

    def test_modify_quotes_reserved_and_spaced_names(self):
        self.assert_compile(
            MySQLModifyColumn(
                "order",
                "select",
                schema="my schema",
                type_=INTEGER(),
                nullable=False,
                default=False,
                autoincrement=None,
                comment=False,
            ),
            "ALTER TABLE `my schema`.`order` MODIFY COLUMN `select` INTEGER NOT NULL",
        )

    def test_modify_autoincrement_is_refused(self):
        """StarRocks answers "Column 'c' can not be AUTO_INCREMENT when MODIFY COLUMN"."""
        with expect_raises_message(exc.CompileError, "cannot set AUTO_INCREMENT in MODIFY COLUMN"):
            MySQLModifyColumn(
                "t", "c", schema=None, type_=INTEGER(), nullable=False, default=False, autoincrement=True, comment=False
            ).compile(dialect=self.__dialect__)

    def test_change_to_the_same_name_is_a_modify(self):
        """Alembic's MySQL impl sends every DATETIME change as a CHANGE COLUMN to the column's own name."""
        self.assert_compile(
            MySQLChangeColumn(
                "t",
                "d",
                schema=None,
                newname="d",
                type_=DATETIME(),
                nullable=True,
                default=False,
                autoincrement=None,
                comment="when",
            ),
            "ALTER TABLE t MODIFY COLUMN d DATETIME NULL COMMENT 'when'",
        )

    def test_change_that_renames_and_modifies_is_refused(self):
        """StarRocks refuses MODIFY COLUMN and RENAME COLUMN in one ALTER TABLE."""
        with expect_raises_message(exc.CompileError, "cannot MODIFY and RENAME column 'old' in one ALTER TABLE"):
            MySQLChangeColumn(
                "t",
                "old",
                schema=None,
                newname="new",
                type_=INTEGER(),
                nullable=True,
                default=False,
                autoincrement=None,
                comment=False,
            ).compile(dialect=self.__dialect__)

    def test_rename_column(self):
        self.assert_compile(ColumnName("t", "old", "new", schema="s"), "ALTER TABLE s.t RENAME COLUMN old TO new")

    @testing.combinations(
        ("literal", "abc"),
        ("expression", text("CURRENT_TIMESTAMP")),
        argnames="default",
        id_="ia",
    )
    def test_alter_default_is_refused(self, default):
        """StarRocks cannot change a default, and the MySQL statement lacks the column type it asks for."""
        with expect_raises_message(exc.CompileError, "cannot change a column's default value"):
            MySQLAlterDefault("t", "c", default=default, schema=None).compile(dialect=self.__dialect__)


class AlterColumnDefaultOperationTest(fixtures.TestBase):
    """``op.alter_column(server_default=...)`` refuses a change of default and passes on an unchanged one."""

    def test_new_default_alone_is_refused(self):
        op_fixture("starrocks")
        with expect_raises_message(exc.CompileError, "cannot change a column's default value"):
            op.alter_column("t", "c", server_default="x", existing_type=INTEGER())

    def test_new_default_with_a_type_change_is_refused(self):
        op_fixture("starrocks")
        with expect_raises_message(exc.CompileError, "'c' of table 't'"):
            op.alter_column("t", "c", type_=INTEGER(), server_default="x", existing_server_default="y")

    def test_dropped_default_is_refused(self):
        op_fixture("starrocks")
        with expect_raises_message(exc.CompileError, "cannot change a column's default value"):
            op.alter_column("t", "c", server_default=None, existing_server_default="y", existing_type=INTEGER())

    def test_unchanged_default_stays_in_the_modify(self):
        context = op_fixture("starrocks")
        op.alter_column("t", "c", type_=INTEGER(), server_default="y", existing_server_default="y", nullable=True)
        context.assert_("ALTER TABLE t MODIFY COLUMN c INTEGER NULL DEFAULT 'y'")

    def test_no_default_argument_is_not_refused(self):
        context = op_fixture("starrocks")
        op.alter_column("t", "c", type_=INTEGER(), existing_server_default="y", nullable=True)
        context.assert_("ALTER TABLE t MODIFY COLUMN c INTEGER NULL DEFAULT 'y'")

    def test_default_without_an_existing_one_is_taken_with_no_database_to_read(self):
        """A hand-written script restates the default for StarRocks and leaves existing_server_default out."""
        context = op_fixture("starrocks")
        op.alter_column("t", "c", type_=BIGINT(), existing_type=INTEGER(), server_default="5", nullable=True)
        context.assert_("ALTER TABLE t MODIFY COLUMN c BIGINT NULL DEFAULT '5'")

    @testing.combinations(
        ("function_in_parentheses", text("uuid()"), text("(uuid())"), "DEFAULT uuid()"),
        ("doubled_quote", "it's", text("'it''s'"), "DEFAULT 'it''s'"),
        ("keyword_in_another_case", text("current_timestamp"), text("CURRENT_TIMESTAMP"), "DEFAULT current_timestamp"),
        argnames="server_default, existing_server_default, default_clause",
        id_="iaaa",
    )
    def test_existing_default_as_starrocks_reflects_it_is_no_change(
        self, server_default, existing_server_default, default_clause
    ):
        """Autogenerate writes ``existing_server_default`` as StarRocks reflects it, which may be spelt otherwise."""
        context = op_fixture("starrocks")
        op.alter_column(
            "t",
            "c",
            type_=STRING(),
            server_default=server_default,
            existing_server_default=existing_server_default,
            nullable=True,
        )
        context.assert_(f"ALTER TABLE t MODIFY COLUMN c STRING NULL {default_clause}")


class AlterColumnAutoincrementOperationTest(fixtures.TestBase):
    """``op.alter_column`` leaves an unchanged AUTO_INCREMENT out of MODIFY COLUMN and refuses a changed one."""

    def test_unchanged_autoincrement_is_left_out(self):
        context = op_fixture("starrocks")
        op.alter_column("t", "id", comment="key", existing_type=BIGINT(), existing_nullable=False,
                        autoincrement=True, existing_autoincrement=True)
        context.assert_("ALTER TABLE t MODIFY COLUMN id BIGINT NOT NULL COMMENT 'key'")

    @testing.combinations((True, False), (False, True), argnames="autoincrement,existing")
    def test_changed_autoincrement_is_refused(self, autoincrement, existing):
        op_fixture("starrocks")
        with expect_raises_message(exc.CompileError, "AUTO_INCREMENT on column 'id'"):
            op.alter_column("t", "id", existing_type=BIGINT(), existing_nullable=False,
                            autoincrement=autoincrement, existing_autoincrement=existing)

    def test_autoincrement_without_existing_is_refused_without_a_database(self):
        op_fixture("starrocks")
        with expect_raises_message(exc.CompileError, "pass existing_autoincrement"):
            op.alter_column("t", "id", comment="key", existing_type=BIGINT(), existing_nullable=False,
                            autoincrement=True)

    def test_no_autoincrement_without_existing_is_left_out_without_a_database(self):
        """Autogenerate writes ``autoincrement=False`` for a ``primary_key=True, autoincrement=False`` column."""
        context = op_fixture("starrocks")
        op.alter_column("t", "id", comment="key", existing_type=INTEGER(), existing_nullable=False,
                        autoincrement=False)
        context.assert_("ALTER TABLE t MODIFY COLUMN id INTEGER NOT NULL COMMENT 'key'")


class AlterColumnRenameOperationTest(fixtures.TestBase):
    """``op.alter_column(new_column_name=...)`` renames in an ALTER TABLE of its own, after any other change."""

    def test_rename_alone(self):
        context = op_fixture("starrocks")
        op.alter_column("t", "old", new_column_name="new", existing_type=INTEGER())
        context.assert_("ALTER TABLE t RENAME COLUMN old TO new")

    def test_rename_with_a_type_change_modifies_first(self):
        context = op_fixture("starrocks")
        op.alter_column("t", "old", new_column_name="new", type_=INTEGER(), nullable=True, schema="s")
        context.assert_("ALTER TABLE s.t MODIFY COLUMN old INTEGER NULL", "ALTER TABLE s.t RENAME COLUMN old TO new")

    def test_a_datetime_comment_change_is_a_modify(self):
        context = op_fixture("starrocks")
        op.alter_column("t", "d", comment="when", existing_type=DATETIME(), existing_nullable=True)
        context.assert_("ALTER TABLE t MODIFY COLUMN d DATETIME NULL COMMENT 'when'")
