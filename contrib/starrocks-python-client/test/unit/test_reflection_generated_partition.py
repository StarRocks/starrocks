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

"""The hidden column StarRocks 3.5 generates for an expression partition is left out of reflection."""

from types import SimpleNamespace
from typing import Any, Optional
from unittest import mock

from sqlalchemy.testing import eq_, fixtures

from starrocks.common.consts import TableConfigKey
from starrocks.common.params import TableInfoKeyWithPrefix
from starrocks.dialect import StarRocksDialect


DDL_3_5 = """CREATE TABLE `gp_expr` (
  `dt` datetime NULL COMMENT "",
  `s` varchar(10) NULL COMMENT "",
  `__generated_partition_column_0` varchar(1048576) NULL AS substr(`s`, 1, 2) COMMENT "",
  `__generated_partition_column_1` datetime NULL AS time_slice(`dt`, INTERVAL 1 month, ceil) COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`dt`)
PARTITION BY (`__generated_partition_column_0`, `__generated_partition_column_1`)
DISTRIBUTED BY HASH(`dt`) BUCKETS 1
"""


class GeneratedPartitionColumnTest(fixtures.TestBase):
    def test_generated_columns_and_their_expressions(self):
        eq_(
            StarRocksDialect._generated_partition_columns(DDL_3_5),
            {
                "__generated_partition_column_0": "substr(`s`, 1, 2)",
                "__generated_partition_column_1": "time_slice(`dt`, INTERVAL 1 month, ceil)",
            },
        )

    def test_a_table_without_one_has_none(self):
        eq_(StarRocksDialect._generated_partition_columns(DDL_3_5.replace("__generated_partition_column", "c")), {})


def _column_row(name: str, position: int, column_type: str, generation_expression: Optional[str] = None) -> Any:
    """A row of information_schema.columns for the table of DDL_3_5, which StarRocks 3.5 lists all four columns in."""
    return SimpleNamespace(
        COLUMN_NAME=name,
        ORDINAL_POSITION=position,
        COLUMN_TYPE=column_type,
        IS_NULLABLE="YES",
        COLUMN_DEFAULT=None,
        COLUMN_COMMENT="",
        COLUMN_KEY="DUP" if name == "dt" else "",
        GENERATION_EXPRESSION=generation_expression,
    )


class GeneratedPartitionReflectionTest(fixtures.TestBase):
    """The rows StarRocks 3.5 reports for a table partitioned by expressions reflect as the model declares it."""

    @staticmethod
    def _reflected_state() -> Any:
        dialect = StarRocksDialect()
        dialect._show_create_table = mock.Mock(return_value=DDL_3_5)
        column_rows = [
            _column_row("dt", 1, "datetime"),
            _column_row("s", 2, "varchar(10)"),
            _column_row("__generated_partition_column_0", 3, "varchar(1048576)", "substr(`s`, 1, 2)"),
            _column_row("__generated_partition_column_1", 4, "datetime", "time_slice(`dt`, INTERVAL 1 month, ceil)"),
        ]
        return dialect._parse_table_state(
            mock.Mock(),
            "gp_expr",
            "db",
            SimpleNamespace(TABLE_NAME="gp_expr", TABLE_SCHEMA="db", TABLE_COMMENT=""),
            {TableConfigKey.TABLE_MODEL: "DUP_KEYS", TableConfigKey.TABLE_ENGINE: "OLAP"},
            column_rows,
            None,
            read_aggregate_types=False,
        )

    def test_generated_columns_are_left_out(self):
        eq_([column["name"] for column in self._reflected_state().columns], ["dt", "s"])

    def test_partition_clause_names_the_expressions(self):
        partition = self._reflected_state().table_options[TableInfoKeyWithPrefix.PARTITION_BY]
        eq_(partition.partition_method, "substr(`s`, 1, 2), time_slice(`dt`, INTERVAL 1 month, ceil)")
