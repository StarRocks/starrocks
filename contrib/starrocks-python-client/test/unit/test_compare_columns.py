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
from unittest.mock import Mock

from alembic.autogenerate.api import AutogenContext
import pytest
from sqlalchemy.exc import NotSupportedError

from starrocks.alembic.compare import (
    compare_starrocks_column_agg_type,
    compare_starrocks_column_autoincrement,
)
from starrocks.common.params import ColumnAggInfoKey, DialectName
from starrocks.common.types import ColumnAggType


logger = logging.getLogger(__name__)


class TestAutogenerateColumnsAggType:
    def test_column_compare_agg_type_logs_options(self, caplog):
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect = Mock()
        autogen_context.dialect.name = DialectName
        caplog.set_level("WARNING")

        # --- Test Case 1: Columns match ---
        conn_col = Mock(
            name="val1",
            dialect_options={
                DialectName: {
                    ColumnAggInfoKey.AGG_TYPE: ColumnAggType.SUM
                }
            }
        )
        meta_col = Mock(
            name="val1",
            dialect_options={
                DialectName: {
                    ColumnAggInfoKey.AGG_TYPE: ColumnAggType.SUM
                }
            }
        )

        caplog.clear()
        compare_starrocks_column_agg_type(autogen_context, None, "test_schema", "t1", "val1", conn_col, meta_col)
        assert not caplog.records

        # --- Test Case 2: Columns differ ---
        meta_col_diff = Mock(
            name="val1",
            dialect_options={
                DialectName: {
                    ColumnAggInfoKey.AGG_TYPE: ColumnAggType.REPLACE
                }
            }
        )

        caplog.clear()
        with pytest.raises(NotSupportedError) as exc_info:
            compare_starrocks_column_agg_type(autogen_context, None, "test_schema", "t1", "val1", conn_col, meta_col_diff)
        logger.debug(f"exc_info.value: {exc_info.value}")
        assert (
            "StarRocks does not support changing the aggregation type of a column"
            in str(exc_info.value)
        )

class TestAutogenerateColumnsAutoIncrement:
    def setup_method(self) -> None:
        self.autogen_context = Mock(spec=AutogenContext)
        self.autogen_context.dialect = Mock()
        self.autogen_context.dialect.name = DialectName

    def teardown_method(self) -> None:
        self.autogen_context = None

    def test_autoincrement_same_noop(self):

        # conn and meta both True
        conn_col = Mock(autoincrement=True)
        meta_col = Mock(autoincrement=True)

        # should not raise
        compare_starrocks_column_autoincrement(self.autogen_context, None, "sch", "t1", "id", conn_col, meta_col)

        # conn and meta both False
        conn_col2 = Mock(autoincrement=False)
        meta_col2 = Mock(autoincrement=False)

        # should not raise
        compare_starrocks_column_autoincrement(self.autogen_context, None, "sch", "t1", "id", conn_col2, meta_col2)

    def test_autoincrement_diff_warns(self, caplog):

        caplog.set_level("WARNING")

        # conn True -> meta False
        conn_col = Mock(autoincrement=True)
        meta_col = Mock(autoincrement=False)

        caplog.clear()
        compare_starrocks_column_autoincrement(self.autogen_context, None, "sch", "t1", "id", conn_col, meta_col)
        assert any(
            "Detected AUTO_INCREMENT is changed" in str(r.getMessage()) and r.levelname == "WARNING"
            for r in caplog.records
        )

        # conn False -> meta True
        conn_col2 = Mock(autoincrement=False)
        meta_col2 = Mock(autoincrement=True)

        caplog.clear()
        compare_starrocks_column_autoincrement(self.autogen_context, None, "sch", "t1", "id", conn_col2, meta_col2)
        assert any(
            "Detected AUTO_INCREMENT is changed" in str(r.getMessage()) and r.levelname == "WARNING"
            for r in caplog.records
        )
