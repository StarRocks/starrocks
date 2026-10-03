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

"""Exercise the real SQL-Tester parser, runner and tearDown without a cluster.

Run from test/ with SQL-Tester dependencies: python -m unittest lib.test_sql_case_cleanup.
Only case selection and external operations are mocked; UUID resolution and the
normal/failing test lifecycle use the production implementation.
"""

import importlib.util
import os
from pathlib import Path
import re
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import nose.case

from lib import choose_cases


CASE = '''-- name: cleanup_regression
function: create_fixture("${uuid0}", "${uuid1}")
-- result:
None
-- !result
select 1;
-- result:
1
-- !result
CLEANUP {
function: remove_fixture("${uuid0}", "${uuid1}")
} END CLEANUP
CLEANUP {
function: remove_fixture("${uuid1}")
} END CLEANUP
'''


class CleanupTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        case_path = Path(self.tmp.name) / "case"
        case_path.write_text(CASE)
        chooser = object.__new__(choose_cases.ChooseCase)
        chooser.case_list = []
        with patch.dict(os.environ, {"attr": ""}):
            chooser.read_t_r_file(str(case_path), None)
        self.case = chooser.case_list[0]
        spec = importlib.util.spec_from_file_location(
            "cleanup_runner_under_test", Path(__file__).resolve().parents[1] / "test_sql_cases.py")
        self.module = importlib.util.module_from_spec(spec)
        # Avoid module-level case discovery, configuration reads and cluster connections.
        with patch.object(choose_cases, "choose_cases", return_value=SimpleNamespace(case_list=[self.case])), \
                patch.dict(os.environ, {"record_mode": "false"}):
            spec.loader.exec_module(self.module)
        self.runner = object.__new__(self.module.TestSQLCases)
        self.runner.keep_alive = False
        self.runner.connection_pool = None
        for name in ("_set_up", "_clear_db_and_resource_if_exists", "_create_and_use_db",
                     "close_starrocks", "close_trino", "close_spark", "close_hive", "check"):
            setattr(self.runner, name, Mock())
        self.runner.execute_single_statement = Mock(return_value=("", [], None, False))
        for name in ("self_print", "log"):
            patcher = patch.object(self.module, name)
            patcher.start()
            self.addCleanup(patcher.stop)

    def assert_cleanup_matches_created_ids(self):
        calls = self.runner.execute_single_statement.call_args_list
        created = calls[0].args[0]
        ids = re.findall(r'"([a-f0-9]{32})"', created)
        self.assertEqual(len(ids), 2)
        self.assertNotEqual(ids[0], ids[1])
        self.assertEqual([call.args for call in calls[-2:]], [
            ('function: remove_fixture("%s", "%s")' % tuple(ids), -1, False),
            ('function: remove_fixture("%s")' % ids[1], -1, False),
        ])
        # The parsed statements remain reusable and retain their original placeholders.
        self.assertIn("${uuid0}", self.case.sql[0])

    def test_cleanup_uses_same_uuids_after_success(self):
        self.runner.cleanup_regression()
        self.assertEqual(self.runner.execute_single_statement.call_count, 2)
        self.runner.tearDown()
        self.assert_cleanup_matches_created_ids()

    def test_cleanup_uses_same_uuids_after_assertion_failure(self):
        self.runner.check.side_effect = [None, AssertionError("deliberate result mismatch")]
        with self.assertRaisesRegex(AssertionError, "deliberate result mismatch"):
            try:
                self.runner.cleanup_regression()
            finally:
                self.runner.tearDown()
        self.assert_cleanup_matches_created_ids()

    def test_later_cleanup_still_runs_if_first_cleanup_fails(self):
        self.runner.cleanup_regression()
        self.runner.execute_single_statement.side_effect = [RuntimeError("cleanup failed"), None]
        with self.assertRaisesRegex(RuntimeError, "CLEANUP failed"):
            self.runner.tearDown()
        self.assert_cleanup_matches_created_ids()
        self.module.log.warning.assert_called_once()
        self.runner.close_starrocks.assert_called_once()
        self.runner.close_hive.assert_called_once()

    def run_with_cleanup_failure(self, fail_query=False):
        if fail_query:
            self.runner.check.side_effect = [None, AssertionError("original query failure")]
        self.runner.execute_single_statement.side_effect = [
            ("", [], None, False), ("", [], None, False),
            RuntimeError("first cleanup failure"), RuntimeError("second cleanup failure"),
        ]
        result = unittest.TestResult()
        nose.case.FunctionTestCase(self.runner.cleanup_regression, tearDown=self.runner.tearDown).run(result)
        self.assert_cleanup_matches_created_ids()
        self.runner.close_starrocks.assert_called_once()
        self.assertFalse(result.wasSuccessful())
        self.assertEqual(len(result.errors), 1)
        self.assertIn("CLEANUP failed", result.errors[0][1])
        self.assertIn("first cleanup failure", result.errors[0][1])
        self.assertIn("second cleanup failure", result.errors[0][1])
        return result

    def test_cleanup_failure_marks_successful_case_as_error(self):
        result = self.run_with_cleanup_failure()
        self.assertEqual(len(result.failures), 0)

    def test_cleanup_failure_preserves_original_query_failure(self):
        result = self.run_with_cleanup_failure(fail_query=True)
        self.assertEqual(len(result.failures), 1)
        self.assertIn("original query failure", result.failures[0][1])

    def test_cleanup_failure_does_not_save_recorded_results(self):
        self.runner.cleanup_regression()
        self.runner.save_r_into_db = Mock()
        self.runner.execute_single_statement.side_effect = [RuntimeError("cleanup failed"), None]
        with patch.object(self.module, "record_mode", True):
            with self.assertRaisesRegex(RuntimeError, "CLEANUP failed"):
                self.runner.tearDown()
        self.runner.save_r_into_db.assert_not_called()
        self.runner.close_starrocks.assert_called_once()


if __name__ == "__main__":
    unittest.main()
