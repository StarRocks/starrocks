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

"""Offline tests for run_mode detection in the dialect."""

from unittest.mock import MagicMock

from sqlalchemy import exc

from starrocks.common.types import SystemRunMode
from starrocks.dialect import StarRocksDialect


def _dbapi_error(msg: str) -> exc.DBAPIError:
    return exc.DBAPIError(msg, None, Exception(msg))


def _connection(responses: dict):
    """Build a mock connection; ``responses`` maps a SQL prefix to a result or an exception."""
    conn = MagicMock()

    def execute(stmt):
        sql = str(stmt)
        for prefix, response in responses.items():
            if sql.startswith(prefix):
                if isinstance(response, Exception):
                    raise response
                return response
        raise AssertionError(f"unexpected statement: {sql}")

    conn.execute.side_effect = execute
    return conn


def _scalar_result(value):
    result = MagicMock()
    result.scalar.return_value = value
    return result


def _rows_result(rows):
    result = MagicMock()
    result.fetchall.return_value = rows
    return result


class TestGetRunMode:
    def test_uses_run_mode_variable(self):
        conn = _connection({"SELECT @@run_mode": _scalar_result("SHARED_DATA")})
        assert StarRocksDialect()._get_run_mode(conn) == SystemRunMode.SHARED_DATA
        # The privileged ADMIN statement must not run when the variable answers.
        assert conn.execute.call_count == 1

    def test_falls_back_to_frontend_config(self):
        conn = _connection({
            "SELECT @@run_mode": _dbapi_error("Unknown system variable 'run_mode'"),
            "ADMIN SHOW FRONTEND CONFIG": _rows_result(
                [("run_mode", "[]", "shared_data", "String", "false", "")]
            ),
        })
        assert StarRocksDialect()._get_run_mode(conn) == SystemRunMode.SHARED_DATA

    def test_defaults_to_shared_nothing_when_both_fail(self):
        conn = _connection({
            "SELECT @@run_mode": _dbapi_error("Unknown system variable 'run_mode'"),
            "ADMIN SHOW FRONTEND CONFIG": _dbapi_error("Access denied; need OPERATE privilege"),
        })
        assert StarRocksDialect()._get_run_mode(conn) == SystemRunMode.SHARED_NOTHING
