#!/usr/bin/env python
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

import unittest
from unittest.mock import Mock

from pyhive.exc import ProgrammingError

from lib.sr_sql_lib import StarrocksSQLApiLib


class ExternalSqlExecutionTest(unittest.TestCase):
    def test_statement_without_result_set(self):
        cursor = Mock(description=None)
        cursor.fetchall.side_effect = ProgrammingError("No result set")
        conn = Mock()
        conn.cursor.return_value = cursor

        result = StarrocksSQLApiLib.conn_execute_sql(None, conn, "CREATE SCHEMA test_db;")

        self.assertEqual({"status": True, "result": "", "msg": "OK"}, result)
        cursor.execute.assert_called_once_with("CREATE SCHEMA test_db")
        cursor.fetchall.assert_called_once()
        cursor.close.assert_called_once()

    def test_statement_with_result_set(self):
        # Some drivers fill their description only when rows are fetched.
        cursor = Mock(description=None)
        cursor.fetchall.return_value = [(1, "a"), (2, "b")]
        conn = Mock()
        conn.cursor.return_value = cursor

        result = StarrocksSQLApiLib.conn_execute_sql(None, conn, "SELECT value FROM test_table")

        self.assertEqual({"status": True, "result": "1\ta\n2\tb", "msg": "OK"}, result)
        cursor.fetchall.assert_called_once()
        cursor.close.assert_called_once()

    def test_other_driver_errors_are_not_ignored(self):
        cursor = Mock(description=None)
        cursor.fetchall.side_effect = ProgrammingError("unexpected error")
        conn = Mock()
        conn.cursor.return_value = cursor

        with self.assertRaisesRegex(ProgrammingError, "unexpected error"):
            StarrocksSQLApiLib.conn_execute_sql(None, conn, "SELECT value FROM test_table")


if __name__ == "__main__":
    unittest.main()
