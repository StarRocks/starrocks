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

import unittest
from unittest.mock import Mock

from lib.sr_sql_lib import StarrocksSQLApiLib


class PaimonReaderTest(unittest.TestCase):
    def setUp(self):
        self.client = object.__new__(StarrocksSQLApiLib)
        self.client.execute_sql = Mock()

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

    def test_mixed_readers_and_selection_reasons(self):
        prefix = "Paimon.metadata.reader.t."
        def trace(native=1, jni=2, reason=1):
            return {"status": True, "result": [(prefix + "paimonNativeReaderReadNum: " + str(native),),
                    (prefix + "jniReaderReadNum: " + str(jni),), (prefix + "starRocksNativeReaderReadNum: 0",),
                    (prefix + "selectionReason.INDEXED_SPLIT: " + str(reason),)]}
        self.client.execute_sql.return_value = trace()
        self.client.assert_paimon_readers("select * from t", "t", "native, jni", "INDEXED_SPLIT")
        for result in (trace(native=0), trace(jni=0), trace(reason=0), {"status": True, "result": []}):
            self.client.execute_sql.return_value = result
            with self.assertRaises(AssertionError):
                self.client.assert_paimon_readers("select * from t", "t", "native,jni", "INDEXED_SPLIT")
        self.client.execute_sql.return_value = trace()
        with self.assertRaises(AssertionError):
            self.client.assert_paimon_readers("select * from t", "t", "native,jni", "FILE_FORMAT")
        with self.assertRaises(ValueError):
            self.client.assert_paimon_readers("select * from t", "t", "native,unknown")

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


if __name__ == "__main__":
    unittest.main()
