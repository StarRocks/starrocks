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

"""Run from test/: python -m unittest lib.test_paimon_fixture -v."""

import importlib.util
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import nose.case

from lib import choose_cases, sr_sql_lib


class PaimonFixtureTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        (self.root / "db.db/t").mkdir(parents=True)
        (self.root / "db.db/t/data").write_bytes(b"fixture")
        root_patch = patch.object(sr_sql_lib, "PAIMON_FIXTURE_ROOT", self.root)
        root_patch.start()
        self.addCleanup(root_patch.stop)
        self.client = object.__new__(sr_sql_lib.StarrocksSQLApiLib)
        self.client.oss_bucket = "bucket"
        self.client.oss_endpoint = "endpoint"
        self.client.oss_ak = "test-key"
        self.client.oss_sk = "test-secret"
        self.client.execute_sql = Mock(return_value={"status": True})
        self.client._paimon_oss = Mock()

    def test_stage_and_cleanup_use_same_target(self):
        self.client.paimon_fixture_prefix = "custom/fixtures"
        self.client.paimon_stage("bucket", "run-1", " db.t ")
        self.client.create_paimon_catalog("catalog", "filesystem", "run-1")
        warehouse = "oss://bucket/custom/fixtures/run-1/"
        self.assertIn(warehouse, self.client.execute_sql.call_args.args[0])
        self.client._paimon_oss.assert_called_once_with(
            "cp", "-r", "-f", str(self.root / "db.db/t") + "/", warehouse + "db.db/t/")
        self.client.paimon_cleanup()
        self.client._paimon_oss.assert_called_with("rm", "-r", "-f", warehouse)
        calls = self.client._paimon_oss.call_count
        self.client.paimon_cleanup()
        self.assertEqual(self.client._paimon_oss.call_count, calls)

    def test_partial_upload_is_cleaned_without_catalog(self):
        self.client._paimon_oss.side_effect = RuntimeError("upload failed")
        with self.assertRaisesRegex(RuntimeError, "upload failed"):
            self.client.paimon_stage("bucket", "run-1", "db.t")
        self.client._paimon_oss.side_effect = None
        self.client.paimon_cleanup()
        self.client._paimon_oss.assert_called_with("rm", "-r", "-f", "oss://bucket/paimon_ci_test/run-1/")
        self.client.execute_sql.assert_not_called()

    def test_catalog_failure_still_cleans_objects(self):
        self.client.paimon_stage("bucket", "run-1", "db.t")
        self.client.execute_sql.return_value = {"status": False}
        with self.assertRaisesRegex(RuntimeError, "create"):
            self.client.create_paimon_catalog("catalog", "filesystem", "run-1")
        with self.assertRaisesRegex(RuntimeError, "drop"):
            self.client.paimon_cleanup()
        self.client._paimon_oss.assert_called_with("rm", "-r", "-f", "oss://bucket/paimon_ci_test/run-1/")
        self.client.execute_sql.return_value = {"status": True}
        self.client.paimon_cleanup()
        self.assertIsNone(self.client._paimon_cleanup_warehouse)

    def test_failed_object_cleanup_can_be_retried(self):
        self.client.paimon_stage("bucket", "run-1", "db.t")
        self.client._paimon_oss.side_effect = RuntimeError("delete failed")
        with self.assertRaisesRegex(RuntimeError, "delete failed"):
            self.client.paimon_cleanup()
        self.client._paimon_oss.side_effect = None
        self.client.paimon_cleanup()
        self.assertIsNone(self.client._paimon_cleanup_warehouse)

    def test_invalid_selection_uploads_nothing(self):
        for selection in ("", "db.t,", "db.t, ,db.t", "db.t,db.missing", "../t"):
            with self.subTest(selection=selection), self.assertRaises(ValueError):
                self.client.paimon_stage("bucket", "run-1", selection)
        self.client._paimon_oss.assert_not_called()
        self.client.paimon_cleanup()
        self.client._paimon_oss.assert_not_called()

    def test_unscoped_target_is_rejected(self):
        for run in ("", "..", "a/b", "*"):
            with self.subTest(run=run), self.assertRaises(ValueError):
                self.client.paimon_stage("bucket", run, "db.t")
        self.client._paimon_oss.assert_not_called()

    def test_reader_trace_rejects_empty_or_mixed_routes(self):
        client = self.client
        prefix = "Paimon.metadata.reader.t."

        def trace(native, jni):
            return {"status": True, "result": [(prefix + "paimonNativeReaderReadNum: " + str(native),),
                    (prefix + "jniReaderReadNum: " + str(jni),), (prefix + "starRocksNativeReaderReadNum: 0",)]}
        client.execute_sql.return_value = trace(2, 0)
        client.assert_paimon_reader("select * from t", "t", "native")
        for result in (trace(0, 0), trace(1, 1), {"status": True, "result": []}):
            client.execute_sql.return_value = result
            with self.assertRaises(AssertionError):
                client.assert_paimon_reader("select * from t", "t", "native")

    def test_native_profile_restores_settings_on_failure(self):
        client = self.client
        client.execute_sql.side_effect = [
            {"status": True, "result": [(False, True)]},
            {"status": True}, {"status": True}, {"status": False, "msg": "query failed"},
            {"status": True}, {"status": True},
        ]
        with self.assertRaises(AssertionError):
            client.assert_paimon_native_profile("select * from t")
        self.assertEqual([call.args[0] for call in client.execute_sql.call_args_list[-2:]],
                         ["SET enable_profile = false", "SET enable_async_profile = true"])

    def test_existing_runner_cleanup_lifecycle(self):
        case_path = Path(__file__).resolve().parents[1] / "sql/test_paimon_catalog/R/test_paimon_reader_modes"
        for failure in (None, "query", "upload", "cleanup"):
            with self.subTest(failure=failure):
                chooser = object.__new__(choose_cases.ChooseCase)
                chooser.case_list = []
                with patch.dict(os.environ, {"attr": ""}):
                    chooser.read_t_r_file(str(case_path), None)
                spec = importlib.util.spec_from_file_location(
                    "paimon_runner_test", Path(__file__).resolve().parents[1] / "test_sql_cases.py")
                module = importlib.util.module_from_spec(spec)
                with patch.object(choose_cases, "choose_cases", return_value=chooser), \
                        patch.dict(os.environ, {"record_mode": "false"}):
                    spec.loader.exec_module(module)
                runner = object.__new__(module.TestSQLCases)
                runner.__dict__.update(self.client.__dict__)
                runner.keep_alive = False
                runner.connection_pool = None
                runner.res_log = []
                runner.execute_sql = Mock(return_value={"status": True, "result": ""})
                for name in ("_set_up", "_clear_db_and_resource_if_exists", "_create_and_use_db",
                             "close_starrocks", "close_trino", "close_spark", "close_hive", "check",
                             "assert_paimon_reader", "assert_paimon_native_profile"):
                    setattr(runner, name, Mock())
                if failure == "query":
                    runner.check.side_effect = [None, None, AssertionError("query failed")]
                operations = []

                def oss(*args):
                    operations.append(args)
                    if failure == "upload" and args[0] == "cp":
                        raise RuntimeError("upload failed")
                    if failure == "cleanup" and args[0] == "rm":
                        raise RuntimeError("cleanup failed")
                runner._paimon_oss = oss
                with patch.object(sr_sql_lib, "PAIMON_FIXTURE_ROOT", case_path.parents[1] / "data"), \
                        patch.object(module, "self_print"), patch.object(module, "log"):
                    result = unittest.TestResult()
                    nose.case.FunctionTestCase(runner.test_paimon_reader_modes, tearDown=runner.tearDown).run(result)
                self.assertEqual(result.wasSuccessful(), failure is None, result.errors)
                uploads = [args for args in operations if args[0] == "cp"]
                deletes = [args for args in operations if args[0] == "rm"]
                self.assertEqual(len(uploads), 1)
                self.assertEqual(len(deletes), 2 if failure == "cleanup" else 1)
                self.assertEqual(uploads[0][-1], deletes[0][-1] + "paimon_test.db/scalar_types/")
                self.assertNotIn("${", deletes[0][-1])

    def test_existing_component_filter_detects_oss_dependency(self):
        for configured in (False, True):
            with self.subTest(configured=configured):
                chooser = object.__new__(choose_cases.ChooseCase)
                chooser.sr_lib_obj = SimpleNamespace(
                    oss_bucket="bucket" if configured else "",
                    component_status={"oss": {"status": configured, "keys": ["oss_bucket"]}})
                case = SimpleNamespace(name="paimon", sql=[
                    'function: paimon_stage("${oss_bucket}", "${uuid0}", "db.t")'])
                chooser.case_list = [case]
                with patch.object(choose_cases.sr_sql_lib, "self_print"):
                    chooser.filter_cases_by_component_status()
                self.assertEqual(chooser.case_list, [case] if configured else [])


if __name__ == "__main__":
    unittest.main()
