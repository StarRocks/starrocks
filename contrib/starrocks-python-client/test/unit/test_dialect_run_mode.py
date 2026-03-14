#! /usr/bin/python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from unittest.mock import MagicMock, Mock

from sqlalchemy import exc
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import eq_, is_

from starrocks.common.types import SystemRunMode
from starrocks.dialect import MySQLDialect_pymysql, StarRocksDialect


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class TestRunModeUnit(fixtures.TestBase):
    __only_on__ = "starrocks"

    def test_initialize_does_not_fetch_run_mode(self, monkeypatch):
        # Keep the parent initialize behavior out of this unit test.
        monkeypatch.setattr(MySQLDialect_pymysql, "initialize", lambda self, conn: None)

        dialect = StarRocksDialect()
        dialect._get_run_mode = Mock(side_effect=AssertionError("run_mode should not be queried in initialize"))

        conn = Mock()
        conn.engine = Mock()

        dialect.initialize(conn)

        dialect._get_run_mode.assert_not_called()
        is_(dialect._run_mode, None)
        is_(dialect._bind_engine, conn.engine)

    def test_run_mode_property_is_lazy_and_cached(self):
        dialect = StarRocksDialect()
        fake_conn = Mock()
        fake_engine = MagicMock()
        fake_engine.connect.return_value.__enter__.return_value = fake_conn
        fake_engine.connect.return_value.__exit__.return_value = None

        dialect._bind_engine = fake_engine
        dialect._get_run_mode = Mock(return_value=SystemRunMode.SHARED_DATA)

        eq_(dialect.run_mode, SystemRunMode.SHARED_DATA)
        eq_(dialect.run_mode, SystemRunMode.SHARED_DATA)
        dialect._get_run_mode.assert_called_once_with(fake_conn)

    def test_get_run_mode_fallback_to_storage_volumes(self):
        dialect = StarRocksDialect()
        conn = Mock()

        def _execute(stmt):
            sql = str(stmt)
            if "ADMIN SHOW FRONTEND CONFIG LIKE 'run_mode'" in sql:
                return _FakeResult([])
            if "SHOW STORAGE VOLUMES" in sql:
                return _FakeResult([("builtin_storage_volume",)])
            raise AssertionError(f"Unexpected SQL: {sql}")

        conn.execute.side_effect = _execute

        eq_(dialect._get_run_mode(conn), SystemRunMode.SHARED_DATA)

    def test_get_run_mode_defaults_to_shared_data_on_query_failures(self):
        dialect = StarRocksDialect()
        conn = Mock()

        def _execute(stmt):
            sql = str(stmt)
            if "ADMIN SHOW FRONTEND CONFIG LIKE 'run_mode'" in sql:
                raise exc.OperationalError(sql, {}, Exception("access denied"))
            if "SHOW STORAGE VOLUMES" in sql:
                raise exc.OperationalError(sql, {}, Exception("access denied"))
            raise AssertionError(f"Unexpected SQL: {sql}")

        conn.execute.side_effect = _execute

        eq_(dialect._get_run_mode(conn), SystemRunMode.SHARED_NOTHING)

    def test_common_table_sql_compile_does_not_access_run_mode(self):
        from sqlalchemy import Column, Integer, MetaData, Table, insert, select
        from sqlalchemy.schema import CreateTable

        dialect = StarRocksDialect()
        dialect._get_run_mode = Mock(side_effect=AssertionError("run_mode should not be used for normal table sql compile"))

        table = Table("t_compile", MetaData(), Column("id", Integer))

        str(CreateTable(table).compile(dialect=dialect))
        str(select(table).compile(dialect=dialect))
        str(insert(table).values(id=1).compile(dialect=dialect))
