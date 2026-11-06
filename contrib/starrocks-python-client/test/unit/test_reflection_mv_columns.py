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

"""A materialized view's reflected columns follow the column list of its CREATE statement, not storage order."""

from types import SimpleNamespace

from sqlalchemy import testing
from sqlalchemy.testing import eq_, fixtures

from starrocks.reflection import StarRocksTableDefinitionParser


class MaterializedViewColumnListTest(fixtures.TestBase):
    @testing.combinations(
        ("plain", "CREATE MATERIALIZED VIEW `mv1` (`name`, `k`)\nCOMMENT \"c\"\nAS SELECT name, k FROM t", ["name", "k"]),
        (
            "comments",
            'CREATE MATERIALIZED VIEW `mv1` (`name` COMMENT "a, (b)", `k` COMMENT "it\\"s")\nREFRESH MANUAL',
            ["name", "k"],
        ),
        ("qualified", "CREATE MATERIALIZED VIEW `db`.`mv1` (`a``b`, c) AS SELECT 1, 2", ["a`b", "c"]),
        ("none", 'CREATE MATERIALIZED VIEW mv1\nCOMMENT "x (y)" AS SELECT 1', []),
        ("select_parenthesis", "CREATE MATERIALIZED VIEW mv1 AS SELECT (1)", []),
        id_="iaa",
        argnames="ddl,names",
    )
    def test_column_list(self, ddl, names):
        eq_(StarRocksTableDefinitionParser._mv_column_list(ddl), names)

    def test_storage_order_is_replaced_by_the_column_list(self):
        rows = [SimpleNamespace(COLUMN_NAME="k"), SimpleNamespace(COLUMN_NAME="name")]
        ordered = StarRocksTableDefinitionParser._order_like_mv_column_list(
            rows, "CREATE MATERIALIZED VIEW `mv1` (`name`, `k`) AS SELECT name, k FROM t"
        )
        eq_([row.COLUMN_NAME for row in ordered], ["name", "k"])

    def test_without_a_column_list_storage_order_stays(self):
        rows = [SimpleNamespace(COLUMN_NAME="k"), SimpleNamespace(COLUMN_NAME="name")]
        ordered = StarRocksTableDefinitionParser._order_like_mv_column_list(
            rows, "CREATE MATERIALIZED VIEW mv1 AS SELECT name, k FROM t"
        )
        eq_([row.COLUMN_NAME for row in ordered], ["k", "name"])
