# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run from test/: python3 -m unittest lib.test_mv_rewrite_helpers. No cluster required."""

import unittest
from unittest.mock import Mock, patch

from lib.sr_sql_lib import StarrocksSQLApiLib


class TestMVRewritePolling(unittest.TestCase):
    def setUp(self):
        self.api = StarrocksSQLApiLib.__new__(StarrocksSQLApiLib)
        self.rewrite = "false"
        self.api.execute_sql = self.execute_sql

    def execute_sql(self, sql, raw):
        if sql.startswith("show variables"):
            return {"status": True, "result": [("enable_materialized_view_rewrite", self.rewrite)]}
        self.rewrite = sql.split(" = ")[1].rstrip(";")
        return {"status": True}

    @patch("lib.sr_sql_lib.time.sleep")
    @patch("lib.sr_sql_lib.time.monotonic", return_value=0)
    def test_absence_does_not_poll_until_a_later_hit(self, clock, sleep):
        self.api.retry_execute_sql = Mock(side_effect=[
            {"status": True, "result": [("IcebergScanNode",)]},
            {"status": True, "result": [("TABLE: mv1",)]},
        ])
        self.assertFalse(self.api.print_hit_materialized_view("select 1", "mv1", expect_hit=False))
        self.assertEqual(1, self.api.retry_execute_sql.call_count)
        sleep.assert_not_called()
        self.assertEqual("false", self.rewrite)

    @patch("lib.sr_sql_lib.time.sleep")
    @patch("lib.sr_sql_lib.time.monotonic", return_value=0)
    def test_absence_waits_past_a_transient_hit(self, clock, sleep):
        self.api.retry_execute_sql = Mock(side_effect=[
            {"status": True, "result": [("TABLE: mv1",)]},
            {"status": True, "result": [("IcebergScanNode",)]},
        ])
        self.assertFalse(self.api.print_hit_materialized_view("select 1", "mv1", expect_hit=False))
        self.assertEqual(2, self.api.retry_execute_sql.call_count)
        sleep.assert_called_once_with(0.2)
        self.assertEqual("false", self.rewrite)

    @patch("lib.sr_sql_lib.time.monotonic", side_effect=[0, 10])
    def test_absence_timeout_reports_the_hit(self, clock):
        self.api.retry_execute_sql = Mock(return_value={"status": True, "result": [("TABLE: mv1",)]})
        self.assertTrue(self.api.print_hit_materialized_view("select 1", "mv1", expect_hit=False))
        self.assertEqual("false", self.rewrite)

    def test_explain_error_is_not_absence_and_restores_session(self):
        self.api.retry_execute_sql = Mock(return_value={"status": False, "msg": "probe EXPLAIN failure"})
        with self.assertRaisesRegex(AssertionError, "probe EXPLAIN failure"):
            self.api.print_hit_materialized_view("select 1", "mv1", expect_hit=False)
        self.assertEqual("false", self.rewrite)

    @patch("lib.sr_sql_lib.time.sleep")
    @patch("lib.sr_sql_lib.time.monotonic", return_value=0)
    def test_default_presence_still_requires_every_marker(self, clock, sleep):
        self.api.retry_execute_sql = Mock(side_effect=[
            {"status": True, "result": [("TABLE: mv1",)]},
            {"status": True, "result": [("UNION TABLE: mv1",)]},
        ])
        self.assertTrue(self.api.print_hit_materialized_view("select 1", "mv1", "UNION"))
        self.assertEqual(2, self.api.retry_execute_sql.call_count)
        self.assertEqual("false", self.rewrite)


if __name__ == "__main__":
    unittest.main()
