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

from alembic.ddl.mysql import MySQLAlterDefault, MySQLChangeColumn, MySQLModifyColumn
from sqlalchemy import text
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import AssertsCompiledSQL

from starrocks.datatype import INTEGER, STRING
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

    def test_modify_autoincrement(self):
        self.assert_compile(
            MySQLModifyColumn(
                "t", "c", schema=None, type_=INTEGER(), nullable=False, default=False, autoincrement=True, comment=False
            ),
            "ALTER TABLE t MODIFY COLUMN c INTEGER NOT NULL AUTO_INCREMENT",
        )

    def test_change_rename_only(self):
        """Alembic always passes the column type to CHANGE COLUMN, so a rename also emits a MODIFY COLUMN.

        StarRocks rejects MODIFY and RENAME in one statement; ``test_suite_alembic.BackendAlterColumnTest``
        marks the resulting failure, and this expectation changes when the dialect is fixed.
        """
        self.assert_compile(
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
            ),
            "ALTER TABLE t MODIFY COLUMN old INTEGER NULL, RENAME COLUMN old TO new",
        )

    def test_change_rename_and_modify(self):
        self.assert_compile(
            MySQLChangeColumn(
                "t",
                "old",
                schema=None,
                newname="new",
                type_=INTEGER(),
                nullable=False,
                default="0",
                autoincrement=None,
                comment="changed",
            ),
            "ALTER TABLE t MODIFY COLUMN old INTEGER NOT NULL DEFAULT '0' COMMENT 'changed', RENAME COLUMN old TO new",
        )

    def test_alter_default_literal(self):
        """StarRocks rejects this: MODIFY COLUMN needs the column type, which ALTER DEFAULT does not carry.

        With the type it still fails, because StarRocks cannot change a column's default value at all.
        """
        self.assert_compile(
            MySQLAlterDefault("t", "c", default="abc", schema=None),
            "ALTER TABLE t MODIFY COLUMN c DEFAULT 'abc'",
        )

    def test_alter_default_expression(self):
        self.assert_compile(
            MySQLAlterDefault("t", "c", default=text("CURRENT_TIMESTAMP"), schema=None),
            "ALTER TABLE t MODIFY COLUMN c DEFAULT CURRENT_TIMESTAMP",
        )
