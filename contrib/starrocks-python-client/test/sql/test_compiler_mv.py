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

"""CREATE, DROP and ALTER MATERIALIZED VIEW compilation.

The dialect puts each materialized view clause and ``AS`` on lines of their own; ``assert_compile`` drops the line
breaks, so each expected string literal below is one line of the generated DDL.
"""

from sqlalchemy import Column, MetaData, Table, select, testing
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import AssertsCompiledSQL

from starrocks.datatype import INTEGER, VARCHAR
from starrocks.dialect import StarRocksDialect
from starrocks.sql.ddl import AlterMaterializedView, CreateMaterializedView, DropMaterializedView
from starrocks.sql.schema import MaterializedView


class CreateMaterializedViewCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = StarRocksDialect()
    __requires__ = ("sqlalchemy_2",)

    def test_create_mv(self):
        mv = MaterializedView("my_mv", MetaData(), definition="SELECT * FROM my_table")
        self.assert_compile(CreateMaterializedView(mv), "CREATE MATERIALIZED VIEW my_mv" "AS" "SELECT * FROM my_table")

    @testing.combinations(
        ("or_replace", {}, {"or_replace": True}, "CREATE OR REPLACE MATERIALIZED VIEW mv"),
        ("if_not_exists", {}, {"if_not_exists": True}, "CREATE MATERIALIZED VIEW IF NOT EXISTS mv"),
        ("schema", {"schema": "test_db"}, {}, "CREATE MATERIALIZED VIEW test_db.mv"),
        (
            "comment",
            {"comment": "This is a test comment"},
            {},
            "CREATE MATERIALIZED VIEW mv" "COMMENT 'This is a test comment'",
        ),
        ("refresh_async", {"starrocks_refresh": "ASYNC"}, {}, "CREATE MATERIALIZED VIEW mv" "REFRESH ASYNC"),
        ("refresh_manual", {"starrocks_refresh": "MANUAL"}, {}, "CREATE MATERIALIZED VIEW mv" "REFRESH MANUAL"),
        (
            "refresh_async_schedule",
            {"starrocks_refresh": "ASYNC START('2025-01-01 00:00:00') EVERY(INTERVAL 1 DAY)"},
            {},
            "CREATE MATERIALIZED VIEW mv" "REFRESH ASYNC START('2025-01-01 00:00:00') EVERY(INTERVAL 1 DAY)",
        ),
        (
            "distribution",
            {"starrocks_distributed_by": "HASH(id) BUCKETS 16"},
            {},
            "CREATE MATERIALIZED VIEW mv" "DISTRIBUTED BY HASH(id) BUCKETS 16",
        ),
        (
            "partition",
            {"starrocks_partition_by": "date_trunc('day', event_date)"},
            {},
            "CREATE MATERIALIZED VIEW mv" "PARTITION BY date_trunc('day', event_date)",
        ),
        (
            "order_by",
            {"starrocks_order_by": "user_id, event_date"},
            {},
            "CREATE MATERIALIZED VIEW mv" "ORDER BY (user_id, event_date)",
        ),
        (
            "properties",
            {"starrocks_properties": {"replication_num": "1", "storage_medium": "SSD"}},
            {},
            "CREATE MATERIALIZED VIEW mv" "PROPERTIES(" '    "replication_num"="1",' '    "storage_medium"="SSD"' ")",
        ),
        argnames="mv_kwargs, create_kwargs, expected_header",
        id_="iaaa",
    )
    def test_create_mv_option(self, mv_kwargs, create_kwargs, expected_header):
        mv = MaterializedView("mv", MetaData(), definition="SELECT c1 FROM test_table", **mv_kwargs)
        self.assert_compile(
            CreateMaterializedView(mv, **create_kwargs), expected_header + "AS" "SELECT c1 FROM test_table"
        )

    def test_create_mv_from_selectable(self):
        users = Table("users", MetaData(), Column("id", INTEGER), Column("name", VARCHAR(50)))
        mv = MaterializedView(
            "mv_from_select",
            MetaData(),
            definition=select(users.c.id, users.c.name).where(users.c.id > 10),
            starrocks_refresh="DEFERRED MANUAL",
        )
        self.assert_compile(
            CreateMaterializedView(mv),
            "CREATE MATERIALIZED VIEW mv_from_select"
            "REFRESH DEFERRED MANUAL"
            "AS"
            "SELECT users.id, users.name FROM users WHERE users.id > 10",
        )

    def test_create_mv_complex(self):
        mv = MaterializedView(
            "mv_complex",
            MetaData(),
            definition="SELECT user_id, event_date, COUNT(*) FROM events GROUP BY 1, 2",
            schema="test_db",
            comment="A complex MV",
            starrocks_partition_by="date_trunc('month', event_date)",
            starrocks_distributed_by="HASH(user_id) BUCKETS 32",
            starrocks_order_by="user_id",
            starrocks_refresh="ASYNC EVERY(INTERVAL 1 HOUR)",
            starrocks_properties={"storage_medium": "HDD"},
        )
        self.assert_compile(
            CreateMaterializedView(mv, if_not_exists=True),
            "CREATE MATERIALIZED VIEW IF NOT EXISTS test_db.mv_complex"
            "COMMENT 'A complex MV'"
            "PARTITION BY date_trunc('month', event_date)"
            "DISTRIBUTED BY HASH(user_id) BUCKETS 32"
            "ORDER BY (user_id)"
            "REFRESH ASYNC EVERY(INTERVAL 1 HOUR)"
            "PROPERTIES("
            '    "storage_medium"="HDD"'
            ")"
            "AS"
            "SELECT user_id, event_date, COUNT(*) FROM events GROUP BY 1, 2",
        )


class DropMaterializedViewCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = StarRocksDialect()
    __requires__ = ("sqlalchemy_2",)

    def test_drop_mv(self):
        mv = MaterializedView("my_mv", MetaData(), definition="SELECT 1")
        self.assert_compile(DropMaterializedView(mv), "DROP MATERIALIZED VIEW my_mv")

    def test_drop_mv_if_exists(self):
        mv = MaterializedView("my_mv", MetaData(), definition="SELECT 1")
        self.assert_compile(DropMaterializedView(mv, if_exists=True), "DROP MATERIALIZED VIEW IF EXISTS my_mv")

    def test_drop_mv_with_schema(self):
        mv = MaterializedView("my_mv", MetaData(), definition="SELECT 1", schema="test_db")
        self.assert_compile(DropMaterializedView(mv), "DROP MATERIALIZED VIEW test_db.my_mv")


class AlterMaterializedViewCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    """``AlterMaterializedView`` takes a name, not a ``MaterializedView``, so this also runs on SQLAlchemy 1.4."""

    __dialect__ = StarRocksDialect()

    def test_alter_mv_refresh(self):
        self.assert_compile(
            AlterMaterializedView("my_mv", refresh="ASYNC EVERY(INTERVAL 2 HOUR)"),
            "ALTER MATERIALIZED VIEW my_mv REFRESH ASYNC EVERY(INTERVAL 2 HOUR)",
        )

    def test_alter_mv_properties(self):
        self.assert_compile(
            AlterMaterializedView("my_mv", properties={"replication_num": "2"}),
            'ALTER MATERIALIZED VIEW my_mv SET ("replication_num" = "2")',
        )

    def test_alter_mv_refresh_and_properties_are_separate_statements(self):
        self.assert_compile(
            AlterMaterializedView("my_mv", schema="test_db", refresh="MANUAL", properties={"replication_num": "3"}),
            "ALTER MATERIALIZED VIEW test_db.my_mv REFRESH MANUAL;"
            'ALTER MATERIALIZED VIEW test_db.my_mv SET ("replication_num" = "3")',
        )
