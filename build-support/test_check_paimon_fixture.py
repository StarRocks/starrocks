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

import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "test" / "lib"))
from paimon_fixture import PaimonFixtureMixin, load_manifest, validate_transition, warehouse_uri


class FixtureTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        table = self.root / "basic.db" / "t"
        table.mkdir(parents=True)
        (table / "data").write_bytes(b"fixture")
        self.manifest = {
            "budget": {"total_bytes": 100, "per_table_bytes": 100, "per_change_bytes": 100},
            "tables": {"basic.t": {"bytes": 7, "files": {"data": hashlib.md5(b"fixture").hexdigest()}}},
            "retired": [],
        }

    def load(self):
        (self.root / "MANIFEST.json").write_text(json.dumps(self.manifest))
        return load_manifest(self.root)

    def test_valid_fixture(self):
        self.assertEqual(self.load(), self.manifest)

    def test_corrupted_file(self):
        (self.root / "basic.db/t/data").write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "checksum"):
            self.load()

    def test_unlisted_file(self):
        (self.root / "basic.db/t/extra").touch()
        with self.assertRaisesRegex(ValueError, "file list"):
            self.load()

    def test_unmanaged_root_data(self):
        (self.root / "forgotten.parquet").touch()
        with self.assertRaisesRegex(ValueError, "unmanaged"):
            self.load()

    def test_missing_table(self):
        self.manifest["tables"]["basic.missing"] = {"files": {}, "bytes": 0}
        with self.assertRaisesRegex(ValueError, "file list"):
            self.load()

    def test_unlisted_table(self):
        (self.root / "basic.db/extra").mkdir()
        with self.assertRaisesRegex(ValueError, "directories"):
            self.load()

    def test_symlink(self):
        (self.root / "basic.db/t/link").symlink_to("data")
        with self.assertRaisesRegex(ValueError, "symlink"):
            self.load()

    def test_budget(self):
        self.manifest["budget"]["total_bytes"] = 6
        with self.assertRaisesRegex(ValueError, "total_bytes"):
            self.load()

    def test_mutation_and_retirement(self):
        old = json.loads(json.dumps(self.manifest))
        self.manifest["tables"]["basic.t"]["files"]["data"] = "0" * 32
        with self.assertRaisesRegex(ValueError, "immutable"):
            validate_transition(old, self.manifest)
        self.manifest["tables"] = {}
        with self.assertRaisesRegex(ValueError, "retired"):
            validate_transition(old, self.manifest)
        self.manifest["retired"] = ["basic.t"]
        validate_transition(old, self.manifest)
        with self.assertRaisesRegex(ValueError, "retired"):
            validate_transition(self.manifest, old)

    def test_cleanup_target_is_scoped(self):
        self.assertEqual(warehouse_uri("bucket", "joobin/fixtures", "run-1"),
                         "oss://bucket/joobin/fixtures/run-1/")
        for run in ("", "..", "a/b", "*"):
            with self.assertRaises(ValueError):
                warehouse_uri("bucket", "joobin/fixtures", run)
        with self.assertRaises(ValueError):
            warehouse_uri("bucket", "../fixtures", "run-1")

    def test_budget_change_needs_reason(self):
        old = json.loads(json.dumps(self.manifest))
        self.manifest["budget"]["total_bytes"] += 1
        with self.assertRaisesRegex(ValueError, "budget.note"):
            validate_transition(old, self.manifest)
        self.manifest["budget"]["note"] = "Add schema evolution fixtures"
        validate_transition(old, self.manifest)

    def client(self):
        client = PaimonFixtureMixin()
        client.oss_bucket = "bucket"
        client.paimon_fixture_prefix = "joobin/fixtures"
        client.paimon_fixture_source = "repo"
        client.paimon_fixture_root = self.root
        client._paimon_oss = Mock()
        client.execute_sql = Mock(return_value={"status": True})
        return client

    def test_invalid_selection_uploads_nothing(self):
        self.load()
        client = self.client()
        with self.assertRaisesRegex(ValueError, "unknown"):
            client.paimon_stage("run-1", "basic.t,basic.missing")
        client._paimon_oss.assert_not_called()

    def test_cleanup_objects_even_if_catalog_drop_fails(self):
        client = self.client()
        client.execute_sql.return_value = {"status": False}
        with self.assertRaisesRegex(RuntimeError, "drop"):
            client.paimon_cleanup("catalog", "run-1")
        client._paimon_oss.assert_called_once_with("rm", "-r", "-f", "oss://bucket/joobin/fixtures/run-1/")

    def test_cleanup_rejects_unscoped_delete(self):
        client = self.client()
        with self.assertRaises(ValueError):
            client.paimon_cleanup("catalog", "..")
        client.execute_sql.assert_not_called()
        client._paimon_oss.assert_not_called()

    def test_reader_trace_rejects_empty_or_mixed_routes(self):
        client = self.client()
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
        client = self.client()
        client.execute_sql.side_effect = [
            {"status": True, "result": [(False, True)]},
            {"status": True}, {"status": True}, {"status": False, "msg": "query failed"},
            {"status": True}, {"status": True},
        ]
        with self.assertRaises(AssertionError):
            client.assert_paimon_native_profile("select * from t")
        self.assertEqual([call.args[0] for call in client.execute_sql.call_args_list[-2:]],
                         ["SET enable_profile = false", "SET enable_async_profile = true"])


if __name__ == "__main__":
    unittest.main()
