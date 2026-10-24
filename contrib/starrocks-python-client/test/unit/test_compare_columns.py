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

from unittest.mock import Mock

from alembic.autogenerate.api import AutogenContext
from alembic.operations.ops import AlterColumnOp
from sqlalchemy import testing
from sqlalchemy.testing import fixtures

from starrocks.alembic.compare import (
    compare_starrocks_column_agg_type,
    compare_starrocks_column_autoincrement,
)
from starrocks.common.params import ColumnAggInfoKey, DialectName
from starrocks.common.types import ColumnAggType


def _starrocks_autogen_context() -> Mock:
    autogen_context = Mock(spec=AutogenContext)
    autogen_context.dialect = Mock()
    autogen_context.dialect.name = DialectName
    return autogen_context


def _column_with_agg_type(agg_type: ColumnAggType) -> Mock:
    return Mock(dialect_options={DialectName: {ColumnAggInfoKey.AGG_TYPE: agg_type}})


class CompareColumnAggTypeTest(fixtures.TestBase):
    def test_same_agg_type_is_silent(self, caplog):
        caplog.set_level("WARNING")
        compare_starrocks_column_agg_type(
            _starrocks_autogen_context(),
            None,
            "test_schema",
            "t1",
            "val1",
            _column_with_agg_type(ColumnAggType.SUM),
            _column_with_agg_type(ColumnAggType.SUM),
        )
        assert not caplog.records

    def test_changed_agg_type_warns(self, caplog):
        """StarRocks cannot change a column's aggregate type, so autogenerate warns and carries on."""
        compare_starrocks_column_agg_type(
            _starrocks_autogen_context(),
            None,
            "test_schema",
            "t1",
            "val1",
            _column_with_agg_type(ColumnAggType.SUM),
            _column_with_agg_type(ColumnAggType.REPLACE),
        )
        assert "StarRocks does not support changing the aggregation type of a column" in caplog.text


class CompareColumnAutoIncrementTest(fixtures.TestBase):
    def setup_test(self):
        self.autogen_context = _starrocks_autogen_context()

    @testing.combinations(
        ("integer_primary_key", "auto", False, False),
        ("declared_and_present", True, True, False),
        ("declared_off_and_absent", False, False, False),
        ("declared_and_absent", True, False, True),
        ("declared_off_and_present", False, True, True),
        argnames="declared,in_database,kept",
        id_="iaaa",
    )
    def test_the_alter_keeps_autoincrement_only_when_it_changes(self, declared, in_database, kept):
        """Alembic puts autoincrement=True on the alter of any integer primary key, which StarRocks would refuse."""
        alter_column_op = AlterColumnOp("t1", "id", modify_comment="new", autoincrement=True)
        compare_starrocks_column_autoincrement(
            self.autogen_context, alter_column_op, "sch", "t1", "id",
            Mock(autoincrement=in_database), Mock(autoincrement=declared),
        )
        assert ("autoincrement" in alter_column_op.kw) is kept

    def test_autoincrement_same_noop(self):
        for autoincrement in (True, False):
            compare_starrocks_column_autoincrement(
                self.autogen_context,
                None,
                "sch",
                "t1",
                "id",
                Mock(autoincrement=autoincrement),
                Mock(autoincrement=autoincrement),
            )

    def test_autoincrement_diff_warns(self, caplog):
        caplog.set_level("WARNING")
        for conn_autoincrement, meta_autoincrement in ((True, False), (False, True)):
            caplog.clear()
            compare_starrocks_column_autoincrement(
                self.autogen_context,
                None,
                "sch",
                "t1",
                "id",
                Mock(autoincrement=conn_autoincrement),
                Mock(autoincrement=meta_autoincrement),
            )
            assert any(
                "cannot add or remove AUTO_INCREMENT" in r.getMessage() and r.levelname == "WARNING"
                for r in caplog.records
            )
