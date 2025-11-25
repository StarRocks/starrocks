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

from sqlalchemy import Column, Table, select
from sqlalchemy.dialects import registry
from sqlalchemy.schema import MetaData

from starrocks.datatype import INTEGER, VARCHAR
from starrocks.sql.ddl import AlterMaterializedView, CreateMaterializedView, DropMaterializedView
from starrocks.sql.schema import MaterializedView
from test.test_utils import normalize_sql


logger = logging.getLogger(__name__)

class TestMaterializedViewCompilerBase:
    """Base class for MV compiler tests with shared setup."""

    @classmethod
    def setup_class(cls):
        cls.logger = logging.getLogger(__name__)
        cls.dialect = registry.load("starrocks")()
        cls.metadata = MetaData()


class TestCreateMaterializedViewCompiler(TestMaterializedViewCompilerBase):
    """Tests for CREATE MATERIALIZED VIEW statements."""

    def test_create_mv_basic(self):
        mv = MaterializedView("my_mv", self.metadata, definition="SELECT * FROM my_table")
        sql = str(CreateMaterializedView(mv).compile(dialect=self.dialect))
        expected = "CREATE MATERIALIZED VIEW my_mv AS SELECT * FROM my_table"
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_create_mv_variations(self):
        # OR REPLACE
        mv_or_replace = MaterializedView(
            "mv_or_replace", self.metadata, definition="SELECT c1 FROM test_table"
        )
        sql_or_replace = str(CreateMaterializedView(mv_or_replace, or_replace=True).compile(dialect=self.dialect))
        assert normalize_sql(sql_or_replace) == normalize_sql(
            "CREATE OR REPLACE MATERIALIZED VIEW mv_or_replace AS SELECT c1 FROM test_table"
        )

        # IF NOT EXISTS
        mv_if_not_exists = MaterializedView(
            "mv_if_not_exists", self.metadata, definition="SELECT c1 FROM test_table"
        )
        sql_if_not_exists = str(
            CreateMaterializedView(mv_if_not_exists, if_not_exists=True).compile(dialect=self.dialect)
        )
        assert normalize_sql(sql_if_not_exists) == normalize_sql(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_if_not_exists AS SELECT c1 FROM test_table"
        )

        # WITH SCHEMA
        mv_with_schema = MaterializedView(
            "mv_with_schema", self.metadata, definition="SELECT c1 FROM test_table", schema="test_db"
        )
        sql_with_schema = str(CreateMaterializedView(mv_with_schema).compile(dialect=self.dialect))
        assert normalize_sql(sql_with_schema) == normalize_sql(
            "CREATE MATERIALIZED VIEW test_db.mv_with_schema AS SELECT c1 FROM test_table"
        )

        # WITH COMMENT
        mv_with_comment = MaterializedView(
            "mv_with_comment",
            self.metadata,
            definition="SELECT c1 FROM test_table",
            comment="This is a test comment",
        )
        sql_with_comment = str(CreateMaterializedView(mv_with_comment).compile(dialect=self.dialect))
        assert normalize_sql(sql_with_comment) == normalize_sql(
            "CREATE MATERIALIZED VIEW mv_with_comment COMMENT 'This is a test comment' AS SELECT c1 FROM test_table"
        )

    def test_create_mv_from_selectable(self):
        """Test CREATE MV with SQLAlchemy Selectable object."""
        users = Table('users', self.metadata,
                     Column('id', INTEGER),
                     Column('name', VARCHAR(50)))
        stmt = select(users.c.id, users.c.name).where(users.c.id > 10)
        mv = MaterializedView("mv_from_select", self.metadata, definition=stmt, starrocks_refresh="DEFERRED")
        sql = str(CreateMaterializedView(mv).compile(dialect=self.dialect))
        normalized = normalize_sql(sql)
        assert "CREATE MATERIALIZED VIEW mv_from_select REFRESH DEFERRED" in normalized
        assert "SELECT users.id,users.name" in normalized
        assert "FROM users" in normalized
        assert "WHERE users.id > 10" in normalized

    def test_create_mv_with_refresh_strategy(self):
        # ASYNC
        mv_async = MaterializedView(
            "mv_async", self.metadata, definition="SELECT 1", starrocks_refresh="ASYNC"
        )
        sql_async = str(CreateMaterializedView(mv_async).compile(dialect=self.dialect))
        assert "REFRESH ASYNC" in normalize_sql(sql_async)

        # MANUAL
        mv_manual = MaterializedView(
            "mv_manual", self.metadata, definition="SELECT 1", starrocks_refresh="MANUAL"
        )
        sql_manual = str(CreateMaterializedView(mv_manual).compile(dialect=self.dialect))
        assert "REFRESH MANUAL" in normalize_sql(sql_manual)

        # ASYNC with START WITH and EVERY
        mv_async_schedule = MaterializedView(
            "mv_async_schedule",
            self.metadata,
            definition="SELECT 1",
            starrocks_refresh="ASYNC START('2025-01-01 00:00:00') EVERY(INTERVAL 1 DAY)",
        )
        sql_async_schedule = str(CreateMaterializedView(mv_async_schedule).compile(dialect=self.dialect))
        normalized = normalize_sql(sql_async_schedule)
        assert 'REFRESH ASYNC START("2025-01-01 00:00:00")EVERY(INTERVAL 1 DAY)' in normalized

    def test_create_mv_with_distribution(self):
        mv = MaterializedView(
            "mv_dist",
            self.metadata,
            definition="SELECT id, name FROM users",
            starrocks_distributed_by="HASH(id) BUCKETS 16",
        )
        sql = str(CreateMaterializedView(mv).compile(dialect=self.dialect))
        normalized = normalize_sql(sql)
        assert "DISTRIBUTED BY HASH(id)BUCKETS 16" in normalized

    def test_create_mv_with_partition(self):
        mv = MaterializedView(
            "mv_part",
            self.metadata,
            definition="SELECT event_date, user_id FROM events",
            starrocks_partition_by="date_trunc('day', event_date)",
        )
        sql = str(CreateMaterializedView(mv).compile(dialect=self.dialect))
        normalized = normalize_sql(sql)
        assert 'PARTITION BY date_trunc("day",event_date)' in normalized

    def test_create_mv_with_order_by(self):
        mv = MaterializedView(
            "mv_order",
            self.metadata,
            definition="SELECT user_id, event_date FROM events",
            starrocks_order_by="user_id, event_date",
        )
        sql = str(CreateMaterializedView(mv).compile(dialect=self.dialect))
        normalized = normalize_sql(sql)
        assert "ORDER BY(user_id,event_date)" in normalized

    def test_create_mv_with_properties(self):
        mv = MaterializedView(
            "mv_props",
            self.metadata,
            definition="SELECT * FROM my_table",
            starrocks_properties={"replication_num": "1", "storage_medium": "SSD"},
        )
        sql = str(CreateMaterializedView(mv).compile(dialect=self.dialect))
        normalized = normalize_sql(sql)
        assert normalize_sql('PROPERTIES ("replication_num" = "1", "storage_medium" = "SSD")') in normalized

    def test_create_mv_complex(self):
        mv = MaterializedView(
            "mv_complex",
            self.metadata,
            definition="SELECT user_id, event_date, COUNT(*) FROM events GROUP BY 1, 2",
            schema="test_db",
            comment="A complex MV",
            starrocks_partition_by="date_trunc('month', event_date)",
            starrocks_distributed_by="HASH(user_id) BUCKETS 32",
            starrocks_order_by="user_id",
            starrocks_refresh="ASYNC EVERY(INTERVAL 1 HOUR)",
            starrocks_properties={"storage_medium": "HDD"},
        )
        sql = str(CreateMaterializedView(mv, if_not_exists=True).compile(dialect=self.dialect))
        expected = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS test_db.mv_complex
        COMMENT 'A complex MV'
        PARTITION BY date_trunc("month",event_date)
        DISTRIBUTED BY HASH(user_id) BUCKETS 32
        ORDER BY (user_id)
        REFRESH ASYNC EVERY(INTERVAL 1 HOUR)
        PROPERTIES ("storage_medium" = "HDD")
        AS SELECT user_id, event_date, COUNT(*) FROM events GROUP BY 1, 2
        """
        assert normalize_sql(sql) == normalize_sql(expected)


class TestDropMaterializedViewCompiler(TestMaterializedViewCompilerBase):
    """Tests for DROP MATERIALIZED VIEW statements."""

    def test_drop_mv(self):
        mv = MaterializedView("my_mv", self.metadata, definition="SELECT 1")
        sql = str(DropMaterializedView(mv).compile(dialect=self.dialect))
        assert normalize_sql(sql) == normalize_sql("DROP MATERIALIZED VIEW my_mv")

    def test_drop_mv_if_exists(self):
        mv = MaterializedView("my_mv", self.metadata, definition="SELECT 1")
        sql = str(DropMaterializedView(mv, if_exists=True).compile(dialect=self.dialect))
        assert normalize_sql(sql) == normalize_sql("DROP MATERIALIZED VIEW IF EXISTS my_mv")

    def test_drop_mv_with_schema(self):
        mv = MaterializedView("my_mv", self.metadata, definition="SELECT 1", schema="test_db")
        sql = str(DropMaterializedView(mv).compile(dialect=self.dialect))
        assert normalize_sql(sql) == normalize_sql("DROP MATERIALIZED VIEW test_db.my_mv")


class TestAlterMaterializedViewCompiler(TestMaterializedViewCompilerBase):
    """Tests for ALTER MATERIALIZED VIEW statements."""

    # def test_alter_mv_rename(self):
    #     sql = str(AlterMaterializedView("my_mv").compile(dialect=self.dialect, rename_to="new_mv_name"))
    #     assert normalize_sql(sql) == normalize_sql("ALTER MATERIALIZED VIEW my_mv RENAME new_mv_name")

    def test_alter_mv_refresh(self):
        sql = str(
            AlterMaterializedView("my_mv", refresh="ASYNC EVERY(INTERVAL 2 HOUR)").compile(
                dialect=self.dialect
            )
        )
        assert normalize_sql(sql) == normalize_sql(
            "ALTER MATERIALIZED VIEW my_mv REFRESH ASYNC EVERY(INTERVAL 2 HOUR)"
        )

    def test_alter_mv_properties(self):
        sql = str(
            AlterMaterializedView("my_mv", properties={"replication_num": "2"}).compile(
                dialect=self.dialect
            )
        )
        assert normalize_sql(sql) == normalize_sql(
            'ALTER MATERIALIZED VIEW my_mv SET ("replication_num" = "2")'
        )

    def test_alter_mv_complex(self):
        sql = str(
            AlterMaterializedView(
                "my_mv",
                schema="test_db",
                refresh="MANUAL",
                properties={"replication_num": "3"},
            ).compile(dialect=self.dialect)
        )
        expected = """
        ALTER MATERIALIZED VIEW test_db.my_mv REFRESH MANUAL;
        ALTER MATERIALIZED VIEW test_db.my_mv SET ("replication_num" = "3")
        """
        assert normalize_sql(sql) == normalize_sql(expected)


