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

# test/sql/test_compiler_alter_column.py
"""
DDL Compiler tests for StarRocks ALTER TABLE ... ALTER COLUMN statements.

Covers:
- MODIFY COLUMN (type/nullable/default/comment)
- CHANGE COLUMN (rename, with optional modify attributes)
- ALTER DEFAULT (MODIFY DEFAULT only)
- Error handling for unsupported AUTO_INCREMENT in ALTER COLUMN
"""
import logging
from unittest.mock import patch

from alembic.ddl.mysql import (
    MySQLAlterDefault,
    MySQLChangeColumn,
    MySQLModifyColumn,
)
import pytest
from sqlalchemy import Integer, String

from starrocks.datatype import INTEGER
from starrocks.dialect import (
    StarRocksDDLCompiler,
    StarRocksDialect,
)


logger = logging.getLogger(__name__)


class TestAlterColumnModify:
    def setup_method(self):
        self.dialect = StarRocksDialect()
        self.compiler = StarRocksDDLCompiler(self.dialect, None)

    def test_modify_type_and_nullable(self):
        with patch('starrocks.dialect.alter_table') as mock_alter_table, \
             patch('starrocks.dialect.format_column_name') as mock_format_col:
            mock_alter_table.return_value = 'ALTER TABLE `t`'
            mock_format_col.return_value = '`c`'

            element = MySQLModifyColumn(
                't',
                'c',
                schema=None,
                type_=Integer(),
                nullable=False,
                default=False,
                autoincrement=None,
                comment=False,
            )

            sql = self.compiler.process(element)
            assert sql == 'ALTER TABLE `t` MODIFY COLUMN `c` INTEGER NOT NULL'

    def test_modify_type_nullable_default_comment(self):
        with patch('starrocks.dialect.alter_table') as mock_alter_table, \
             patch('starrocks.dialect.format_column_name') as mock_format_col, \
             patch('starrocks.dialect.format_server_default') as mock_fmt_def:
            mock_alter_table.return_value = 'ALTER TABLE `t`'
            mock_format_col.return_value = '`c`'
            mock_fmt_def.return_value = "'abc'"

            element = MySQLModifyColumn(
                't',
                'c',
                schema=None,
                type_=String(20),
                nullable=True,
                default="abc",
                autoincrement=None,
                comment='greeting',
            )

            sql = self.compiler.process(element)
            assert sql == 'ALTER TABLE `t` MODIFY COLUMN `c` VARCHAR(20) NULL DEFAULT ' \
                           + "'abc' COMMENT 'greeting'"

    def test_modify_schema_qualified(self):
        with patch('starrocks.dialect.alter_table') as mock_alter_table, \
             patch('starrocks.dialect.format_column_name') as mock_format_col:
            mock_alter_table.return_value = 'ALTER TABLE `s`.`t`'
            mock_format_col.return_value = '`c`'

            element = MySQLModifyColumn(
                't',
                'c',
                schema='s',
                type_=Integer(),
                nullable=True,
                default=False,
                autoincrement=None,
                comment=False,
            )

            sql = self.compiler.process(element)
            assert sql == 'ALTER TABLE `s`.`t` MODIFY COLUMN `c` INTEGER NULL'


class TestAlterColumnChange:
    def setup_method(self):
        self.dialect = StarRocksDialect()
        self.compiler = StarRocksDDLCompiler(self.dialect, None)

    def test_change_rename_only(self):
        with patch('starrocks.dialect.alter_table') as mock_alter_table, \
             patch('starrocks.dialect.format_column_name') as mock_format_col:
            mock_alter_table.return_value = 'ALTER TABLE `t`'
            # first call for old name, second for new name
            mock_format_col.side_effect = ['`old`', '`new`', '`old`']

            element = MySQLChangeColumn(
                't',
                'old',
                schema=None,
                newname='new',
                type_=INTEGER(),
                nullable=True,
                default=False,
                autoincrement=None,
                comment=False,
            )

            sql = self.compiler.process(element)
            logger.debug(f"generated sql: {sql}")
            # StarRocks CHANGE compiles to RENAME only
            assert sql == 'ALTER TABLE `t` MODIFY COLUMN `old` INTEGER NULL, RENAME COLUMN `old` TO `new`'

    def test_change_rename_and_modify(self):
        with patch('starrocks.dialect.alter_table') as mock_alter_table, \
             patch('starrocks.dialect.format_column_name') as mock_format_col, \
             patch('starrocks.dialect.format_server_default') as mock_fmt_def:
            mock_alter_table.return_value = 'ALTER TABLE `t`'
            # used for old and then new name, but modify uses old name again
            mock_format_col.side_effect = ['`old`', '`new`', '`old`']
            mock_fmt_def.return_value = '0'

            element = MySQLChangeColumn(
                't',
                'old',
                schema=None,
                newname='new',
                type_=Integer(),
                nullable=False,
                default=0,
                autoincrement=None,
                comment='changed',
            )

            sql = self.compiler.process(element)
            logger.debug(f"generated sql: {sql}")
            # Expect two clauses: MODIFY then RENAME (order per implementation)
            assert sql == 'ALTER TABLE `t` MODIFY COLUMN `old` INTEGER NOT NULL DEFAULT 0 COMMENT ' \
                           + "'changed', RENAME COLUMN `old` TO `new`"


class TestAlterDefault:
    def setup_method(self):
        self.dialect = StarRocksDialect()
        self.compiler = StarRocksDDLCompiler(self.dialect, None)

    def test_alter_default_set(self):
        with patch('starrocks.dialect.alter_table') as mock_alter_table, \
             patch('starrocks.dialect.format_column_name') as mock_format_col, \
             patch('starrocks.dialect.format_server_default') as mock_fmt_def:
            mock_alter_table.return_value = 'ALTER TABLE `t`'
            mock_format_col.return_value = '`c`'
            mock_fmt_def.return_value = 'CURRENT_TIMESTAMP'

            element = MySQLAlterDefault(
                't',
                'c',
                default='CURRENT_TIMESTAMP',
                schema=None,
            )

            sql = self.compiler.process(element)
            assert sql == 'ALTER TABLE `t` MODIFY COLUMN `c` DEFAULT CURRENT_TIMESTAMP'


class TestAlterColumnErrors:
    def setup_method(self):
        self.dialect = StarRocksDialect()
        self.compiler = StarRocksDDLCompiler(self.dialect, None)

    def test_modify_autoincrement_not_supported(self):
        with patch('starrocks.dialect.alter_table') as mock_alter_table, \
             patch('starrocks.dialect.format_column_name') as mock_format_col:
            mock_alter_table.return_value = 'ALTER TABLE `t`'
            mock_format_col.return_value = '`c`'

            element = MySQLModifyColumn(
                't',
                'c',
                schema=None,
                type_=Integer(),
                nullable=False,
                default=False,
                autoincrement=True,
                comment=False,
            )

            with pytest.raises(Exception):
                _ = self.compiler.process(element)
