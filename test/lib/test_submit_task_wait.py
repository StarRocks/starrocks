# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run from test/: python -m unittest lib.test_submit_task_wait -v."""

import unittest
from unittest.mock import Mock, call, patch

from lib.sr_sql_lib import StarrocksSQLApiLib


class SubmitTaskWaitTest(unittest.TestCase):
    def setUp(self):
        self.client = object.__new__(StarrocksSQLApiLib)
        self.client.execute_sql = Mock()

    @staticmethod
    def task_state(state=None):
        return {"status": True, "result": () if state is None else ((state,),), "msg": "", "desc": None}

    @patch("lib.sr_sql_lib.time.sleep")
    def test_waits_through_empty_snapshots_until_task_finishes(self, sleep):
        states = (None, "PENDING", None, "SUBMITTED", "RUNNING", "SUCCESS")
        self.client.execute_sql.side_effect = [self.task_state(state) for state in states]

        self.assertEqual("", self.client.wait_submit_task_ready("etl_test"))

        query = "select STATE from information_schema.task_runs where TASK_NAME = 'etl_test'"
        self.assertEqual([call(query, True)] * len(states), self.client.execute_sql.call_args_list)
        self.assertEqual([call(1)] * (len(states) - 1), sleep.call_args_list)

    @patch("lib.sr_sql_lib.time.sleep")
    def test_terminal_states_return_without_delay(self, sleep):
        for state in ("SUCCESS", "FAILED", "MERGED"):
            with self.subTest(state=state):
                self.client.execute_sql.return_value = self.task_state(state)
                self.assertEqual("", self.client.wait_submit_task_ready("etl_test"))
        sleep.assert_not_called()

    @patch("lib.sr_sql_lib.time.sleep")
    def test_sql_error_is_not_retried(self, sleep):
        self.client.execute_sql.return_value = {"status": False, "msg": "query failed"}

        with self.assertRaisesRegex(AssertionError, "acquire task state error"):
            self.client.wait_submit_task_ready("etl_test")

        self.assertEqual(1, self.client.execute_sql.call_count)
        sleep.assert_not_called()

    # sql_annotation.timeout raises the built-in TimeoutError on the case deadline.
    @patch("lib.sr_sql_lib.time.sleep", side_effect=TimeoutError("case timeout"))
    def test_empty_snapshots_do_not_swallow_case_timeout(self, sleep):
        self.client.execute_sql.return_value = self.task_state()

        with self.assertRaisesRegex(TimeoutError, "case timeout"):
            self.client.wait_submit_task_ready("etl_test")

        self.assertEqual(1, self.client.execute_sql.call_count)


if __name__ == "__main__":
    unittest.main()
