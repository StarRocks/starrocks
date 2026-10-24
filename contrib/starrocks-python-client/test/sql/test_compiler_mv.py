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

import logging

from alembic import op
from alembic.testing.fixtures import op_fixture
from sqlalchemy import Column, MetaData, Table, exc, select, testing
from sqlalchemy.schema import DropTable
from sqlalchemy.testing import expect_raises_message, fixtures
from sqlalchemy.testing.assertions import AssertsCompiledSQL

import starrocks.alembic  # noqa: F401
from starrocks.datatype import INTEGER, VARCHAR
from starrocks.dialect import StarRocksDialect
from starrocks.sql.ddl import AlterMaterializedView, CreateMaterializedView, DropMaterializedView
from starrocks.sql.schema import MaterializedView


def assert_synchronous_mv_warning_logged(caplog, mv_name):
    """Assert that ``starrocks.dialect`` logged, as a warning, that the MV is built synchronously."""
    expected = f"Materialized view {mv_name!r} has neither starrocks_refresh nor starrocks_distributed_by"
    assert [
        record
        for record in caplog.records
        if record.name == "starrocks.dialect"
        and record.levelno == logging.WARNING
        and record.getMessage().startswith(expected)
    ], caplog.text


class CreateMaterializedViewCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = StarRocksDialect()
    __requires__ = ("sqlalchemy_2",)

    def test_create_or_replace_raises(self):
        """No StarRocks release has CREATE OR REPLACE MATERIALIZED VIEW."""
        mv = MaterializedView("my_mv", MetaData(), definition="SELECT 1", starrocks_refresh="MANUAL")
        with expect_raises_message(exc.CompileError, "StarRocks has no CREATE OR REPLACE MATERIALIZED VIEW"):
            CreateMaterializedView(mv, or_replace=True).compile(dialect=self.__dialect__)

    def test_create_mv(self):
        mv = MaterializedView("my_mv", MetaData(), definition="SELECT * FROM my_table", starrocks_refresh="MANUAL")
        self.assert_compile(
            CreateMaterializedView(mv), "CREATE MATERIALIZED VIEW my_mv" "REFRESH MANUAL" "AS" "SELECT * FROM my_table"
        )

    def test_synchronous_mv_warns(self, caplog):
        """With neither a refresh scheme nor a distribution, StarRocks builds a synchronous materialized view.

        Reflection can't see it, so the dialect logs a warning, and creates it as it did before.
        """
        mv = MaterializedView("my_mv", MetaData(), definition="SELECT k, max(v) FROM my_table GROUP BY k")
        with caplog.at_level(logging.WARNING, logger="starrocks.dialect"):
            self.assert_compile(
                CreateMaterializedView(mv), "CREATE MATERIALIZED VIEW my_mv" "AS" "SELECT k, max(v) FROM my_table GROUP BY k"
            )
        assert_synchronous_mv_warning_logged(caplog, "my_mv")

    def test_distribution_without_refresh_logs_no_synchronous_mv_warning(self, caplog):
        """A distribution without a refresh makes StarRocks build an asynchronous MV refreshed by hand."""
        mv = MaterializedView("my_mv", MetaData(), definition="SELECT k FROM my_table", starrocks_distributed_by="HASH(k)")
        with caplog.at_level(logging.WARNING, logger="starrocks.dialect"):
            CreateMaterializedView(mv).compile(dialect=self.__dialect__)
        assert "synchronous materialized view" not in caplog.text

    @testing.combinations(
        (
            "if_not_exists",
            {"starrocks_refresh": "MANUAL"},
            {"if_not_exists": True},
            "CREATE MATERIALIZED VIEW IF NOT EXISTS mv" "REFRESH MANUAL",
        ),
        ("schema", {"schema": "test_db", "starrocks_refresh": "MANUAL"}, {}, "CREATE MATERIALIZED VIEW test_db.mv" "REFRESH MANUAL"),
        (
            "comment",
            {"comment": "This is a test comment", "starrocks_refresh": "MANUAL"},
            {},
            "CREATE MATERIALIZED VIEW mv" "COMMENT 'This is a test comment'" "REFRESH MANUAL",
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
            "refresh_schedule",
            {"starrocks_refresh": "SCHEDULE EVERY(INTERVAL 1 HOUR)"},
            {},
            "CREATE MATERIALIZED VIEW mv" "REFRESH ASYNC EVERY(INTERVAL 1 HOUR)",
        ),
        (
            "refresh_deferred_schedule",
            {"starrocks_refresh": "deferred schedule START('2025-01-01 00:00:00') EVERY(INTERVAL 1 DAY)"},
            {},
            "CREATE MATERIALIZED VIEW mv" "REFRESH deferred ASYNC START('2025-01-01 00:00:00') EVERY(INTERVAL 1 DAY)",
        ),
        (
            "distribution_without_refresh",
            {"starrocks_distributed_by": "HASH(id) BUCKETS 16"},
            {},
            "CREATE MATERIALIZED VIEW mv" "DISTRIBUTED BY HASH(id) BUCKETS 16",
        ),
        (
            "partition",
            {"starrocks_partition_by": "date_trunc('day', event_date)", "starrocks_refresh": "MANUAL"},
            {},
            "CREATE MATERIALIZED VIEW mv" "PARTITION BY date_trunc('day', event_date)" "REFRESH MANUAL",
        ),
        (
            "order_by",
            {"starrocks_order_by": "user_id, event_date", "starrocks_refresh": "MANUAL"},
            {},
            "CREATE MATERIALIZED VIEW mv" "ORDER BY (user_id, event_date)" "REFRESH MANUAL",
        ),
        (
            "properties",
            {"starrocks_properties": {"replication_num": "1", "storage_medium": "SSD"}, "starrocks_refresh": "MANUAL"},
            {},
            "CREATE MATERIALIZED VIEW mv"
            "REFRESH MANUAL"
            "PROPERTIES("
            '    "replication_num"="1",'
            '    "storage_medium"="SSD"'
            ")",
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

    @testing.combinations(
        (False, "DROP MATERIALIZED VIEW my_mv"), (True, "DROP MATERIALIZED VIEW IF EXISTS my_mv"), argnames="if_exists, expected"
    )
    def test_drop_table_of_mv(self, if_exists, expected):
        """``MetaData.drop_all()`` drops a MaterializedView through ``DropTable``."""
        mv = MaterializedView("my_mv", MetaData(), definition="SELECT 1")
        self.assert_compile(DropTable(mv, if_exists=if_exists), expected)


class AlterMaterializedViewCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    """``AlterMaterializedView`` takes a name, not a ``MaterializedView``, so this also runs on SQLAlchemy 1.4."""

    __dialect__ = StarRocksDialect()

    def test_alter_mv_refresh(self):
        self.assert_compile(
            AlterMaterializedView("my_mv", refresh="ASYNC EVERY(INTERVAL 2 HOUR)"),
            "ALTER MATERIALIZED VIEW my_mv REFRESH ASYNC EVERY(INTERVAL 2 HOUR)",
        )

    def test_alter_mv_refresh_schedule_is_async(self):
        """Before 4.1.1 StarRocks knows only ASYNC for the same refresh."""
        self.assert_compile(
            AlterMaterializedView("my_mv", refresh="SCHEDULE EVERY(INTERVAL 2 HOUR)"),
            "ALTER MATERIALIZED VIEW my_mv REFRESH ASYNC EVERY(INTERVAL 2 HOUR)",
        )

    def test_alter_mv_properties(self):
        self.assert_compile(
            AlterMaterializedView("my_mv", properties={"replication_num": "2"}),
            'ALTER MATERIALIZED VIEW my_mv SET ("replication_num" = "2")',
        )

    def test_alter_mv_property_quoting(self):
        """Values are quoted as in ALTER TABLE: backslashes doubled, then double quotes escaped."""
        self.assert_compile(
            AlterMaterializedView("my_mv", properties={"resource_group": r'rg\"1'}),
            r'ALTER MATERIALIZED VIEW my_mv SET ("resource_group" = "rg\\\"1")',
        )

    def test_alter_mv_refresh_and_properties_are_separate_statements(self):
        self.assert_compile(
            AlterMaterializedView("my_mv", schema="test_db", refresh="MANUAL", properties={"replication_num": "3"}),
            "ALTER MATERIALIZED VIEW test_db.my_mv REFRESH MANUAL;"
            'ALTER MATERIALIZED VIEW test_db.my_mv SET ("replication_num" = "3")',
        )


class CreateMaterializedViewOperationTest(fixtures.TestBase):
    """``op.create_materialized_view`` without a refresh scheme builds a synchronous MV, as it always did, and warns."""

    __requires__ = ("sqlalchemy_2",)

    def test_without_a_refresh_warns_and_creates(self, caplog):
        context = op_fixture("starrocks")
        with caplog.at_level(logging.WARNING, logger="starrocks.dialect"):
            op.create_materialized_view("mv", "SELECT k, max(v) FROM t GROUP BY k")
        context.assert_("CREATE MATERIALIZED VIEW mvASSELECT k, max(v) FROM t GROUP BY k")
        assert_synchronous_mv_warning_logged(caplog, "mv")


class AlterMaterializedViewOperationTest(fixtures.TestBase):
    """``op.alter_materialized_view`` alters the refresh and the properties in statements of their own."""

    __requires__ = ("sqlalchemy_2",)

    def test_refresh_and_properties_are_separate_statements(self):
        context = op_fixture("starrocks")
        op.alter_materialized_view("mv", refresh="MANUAL", properties={"session.query_timeout": "900"})
        context.assert_(
            "ALTER MATERIALIZED VIEW mv REFRESH MANUAL",
            'ALTER MATERIALIZED VIEW mv SET ("session.query_timeout" = "900")',
        )

    def test_a_property_with_no_value_stops_the_op_before_the_refresh_changes(self):
        context = op_fixture("starrocks")
        with expect_raises_message(exc.CompileError, "Property 'session.query_timeout' has no value"):
            op.alter_materialized_view("mv", refresh="MANUAL", properties={"session.query_timeout": None})
        context.assert_()
