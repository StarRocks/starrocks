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

from uuid import uuid4

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, exc, inspect, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import eq_, is_true

from starrocks.common.types import SystemRunMode


class _RunModeLookupForbidder:
    def __init__(self, engine: Engine, message: str):
        self.engine = engine
        self.message = message
        self.call_count = 0
        self._dialect = engine.dialect
        self._original_get_run_mode = None

    def __enter__(self):
        self._original_get_run_mode = self._dialect._get_run_mode
        self._dialect._run_mode = None

        def _forbid_get_run_mode(*args, **kwargs):
            self.call_count += 1
            raise AssertionError(self.message)

        self._dialect._get_run_mode = _forbid_get_run_mode
        return self

    def __exit__(self, exc_type, exc, tb):
        self._dialect._get_run_mode = self._original_get_run_mode


@pytest.mark.integration
class TestRunModeLazyForCommonTablePaths(fixtures.TablesTest):
    __only_on__ = "starrocks"
    run_define_tables = "each"
    run_create_tables = "each"
    run_inserts = None
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "test_run_mode_lazy_common_ops",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(32)),
            starrocks_distributed_by="HASH(id) BUCKETS 8",
            starrocks_properties={"replication_num": "1"},
        )
        Table(
            "test_run_mode_lazy_autoload",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(32)),
            starrocks_distributed_by="HASH(id) BUCKETS 8",
            starrocks_properties={"replication_num": "1"},
        )

    def test_insert_select_and_inspect_do_not_require_run_mode(self, connection) -> None:
        table = self.tables.test_run_mode_lazy_common_ops

        with _RunModeLookupForbidder(
            self.bind,
            "common table read/write paths should not query run_mode",
        ) as forbidder:
            connection.execute(table.insert().values(id=1, name="alice"))
            rows = connection.execute(select(table.c.name).where(table.c.id == 1)).fetchall()
            is_true(rows and rows[0][0] == "alice")

            inspector = inspect(self.bind)
            is_true(inspector.has_table(table.name))
            columns = inspector.get_columns(table.name)
            is_true(any(col["name"].lower() == "id" for col in columns))
            is_true(any(col["name"].lower() == "name" for col in columns))
            inspector.get_table_options(table.name)

        eq_(forbidder.call_count, 0)

    def test_autoload_table_does_not_require_run_mode(self) -> None:
        table_name = self.tables.test_run_mode_lazy_autoload.name

        with _RunModeLookupForbidder(
            self.bind,
            "table autoload should not query run_mode",
        ) as forbidder:
            reflected = Table(table_name, MetaData(), autoload_with=self.bind)
            eq_(reflected.name, table_name)
            is_true("id" in reflected.c)
            is_true("name" in reflected.c)

        eq_(forbidder.call_count, 0)


@pytest.mark.integration
class TestRunModeLimitedUser(fixtures.TestBase):
    __only_on__ = "starrocks"

    def test_limited_table_rw_user_can_resolve_run_mode(self, connection) -> None:
        table_name = "test_run_mode_limited_user_table"
        database = connection.engine.url.database
        username = f"run_mode_u_{uuid4().hex[:10]}"
        password = f"P_{uuid4().hex[:14]}"
        user_spec = f"'{username}'@'%'"
        user_engine = None

        try:
            connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            connection.execute(
                text(
                    f"""
                        CREATE TABLE {table_name} (
                            id INT,
                            name VARCHAR(32)
                        )
                        DISTRIBUTED BY HASH(id) BUCKETS 8
                        PROPERTIES("replication_num" = "1")
                        """
                )
            )

            try:
                connection.execute(text(f"CREATE USER {user_spec} IDENTIFIED BY '{password}'"))
                connection.execute(text(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {database}.{table_name} TO {user_spec}"))
            except exc.DBAPIError as create_user_exc:
                pytest.skip(
                    f"Unable to create/grant limited test user in this environment: {create_user_exc}"
                )

            user_url = connection.engine.url.set(username=username, password=password)
            user_engine = create_engine(user_url, pool_pre_ping=True)

            with user_engine.begin() as conn:
                conn.execute(text(f"INSERT INTO {table_name} (id, name) VALUES (1, 'alice')"))
                rows = conn.execute(text(f"SELECT name FROM {table_name} WHERE id = 1")).fetchall()
                is_true(rows and rows[0][0] == "alice")

            run_mode = user_engine.dialect.run_mode
            is_true(run_mode in (SystemRunMode.SHARED_DATA, SystemRunMode.SHARED_NOTHING))
        finally:
            if user_engine is not None:
                user_engine.dispose()
            try:
                connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                connection.execute(text(f"DROP USER {user_spec}"))
            except exc.DBAPIError:
                pass
