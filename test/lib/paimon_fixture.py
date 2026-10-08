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

"""Paimon fixture staging and reader assertions for SQL-Tester."""

import json
from pathlib import Path
import re
import subprocess

FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "sql/test_paimon_catalog/data"


def table_path(name):
    if not re.fullmatch(r"[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*", name):
        raise ValueError("invalid fixture table: %s" % name)
    database, table = name.split(".")
    return "%s.db/%s" % (database, table)


def warehouse_uri(bucket, prefix, run_id):
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]+[a-z0-9]", bucket):
        raise ValueError("invalid OSS bucket")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise ValueError("invalid fixture run ID")
    if not prefix or any(not re.fullmatch(r"[A-Za-z0-9_-]+", part) for part in prefix.split("/")):
        raise ValueError("invalid fixture OSS prefix")
    return "oss://%s/%s/%s/" % (bucket, prefix, run_id)


class PaimonFixtureMixin:
    """Methods exposed to SQL-Tester via StarrocksSQLApiLib."""

    def _paimon_warehouse(self, run_id):
        prefix = getattr(self, "paimon_fixture_prefix", "paimon_ci_test")
        return warehouse_uri(self.oss_bucket, prefix, run_id)

    def _paimon_oss(self, *args):
        # Use the runner's ossutil credentials, never put keys on the command line.
        result = subprocess.run(
            ["ossutil64", *args, "-e", self.oss_endpoint],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=120,
        )
        if result.returncode:
            raise RuntimeError("Paimon fixture OSS operation failed (%s): %s" % (args[0], result.stderr))

    def paimon_stage(self, bucket, run_id, tables):
        # The explicit bucket argument lets existing component filtering detect OSS usage.
        names = [name.strip() for name in tables.split(",")]
        paths = [table_path(name) for name in names]
        for relative in paths:
            directory = FIXTURE_ROOT / relative
            if not directory.is_dir() or directory.is_symlink():
                raise ValueError("missing or invalid fixture directory: %s" % relative)
        prefix = getattr(self, "paimon_fixture_prefix", "paimon_ci_test")
        warehouse = warehouse_uri(bucket, prefix, run_id)
        self.paimon_cleanup()
        # Save the resolved target before uploading so CLEANUP can remove partial uploads.
        self._paimon_cleanup_warehouse = warehouse
        self._paimon_cleanup_catalog = None
        for relative in paths:
            self._paimon_oss("cp", "-r", "-f", str(FIXTURE_ROOT / relative) + "/", warehouse + relative + "/")

    def create_paimon_catalog(self, catalog, catalog_type, run_id):
        if catalog_type != "filesystem":
            raise ValueError("fixture catalogs must use filesystem")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
            raise ValueError("invalid fixture catalog name")
        self._paimon_cleanup_catalog = catalog
        properties = {
            "type": "paimon", "paimon.catalog.type": "filesystem",
            "paimon.catalog.warehouse": self._paimon_warehouse(run_id),
            "aws.s3.access_key": self.oss_ak, "aws.s3.secret_key": self.oss_sk,
            "aws.s3.endpoint": self.oss_endpoint,
        }
        properties_sql = ",".join("%s=%s" % (json.dumps(k), json.dumps(v)) for k, v in properties.items())
        result = self.execute_sql("CREATE EXTERNAL CATALOG `%s` PROPERTIES (%s)" % (catalog, properties_sql))
        if not result["status"]:
            # Catalog SQL contains credentials; do not include it in the failure message.
            raise RuntimeError("failed to create Paimon fixture catalog %s" % catalog)

    def paimon_cleanup(self):
        warehouse = getattr(self, "_paimon_cleanup_warehouse", None)
        if warehouse is None:
            return
        catalog = self._paimon_cleanup_catalog
        try:
            if catalog is not None:
                self.execute_sql("SET CATALOG default_catalog")
                result = self.execute_sql("DROP CATALOG IF EXISTS `%s`" % catalog)
                if not result["status"]:
                    raise RuntimeError("failed to drop Paimon fixture catalog %s" % catalog)
        finally:
            self._paimon_oss("rm", "-r", "-f", warehouse)
        self._paimon_cleanup_warehouse = None
        self._paimon_cleanup_catalog = None

    def assert_paimon_reader(self, query, table, expected):
        """Assert FE scan routing using existing per-table EXTERNAL trace counters."""
        counters = {"native": "paimonNativeReaderReadNum", "jni": "jniReaderReadNum",
                    "starrocks": "starRocksNativeReaderReadNum"}
        if expected not in counters:
            raise ValueError("expected reader must be native, jni, or starrocks")
        result = self.execute_sql("TRACE VALUES EXTERNAL " + query, True)
        if not result["status"]:
            raise AssertionError("Paimon reader trace failed: %s" % result.get("msg"))
        trace = "\n".join(str(value) for row in result["result"] for value in row)
        for reader, counter in counters.items():
            key = "Paimon.metadata.reader.%s.%s" % (table, counter)
            values = re.findall(re.escape(key) + r"\s*:\s*(\d+)", trace)
            if not values or any((int(value) > 0) != (reader == expected) for value in values):
                raise AssertionError("expected %s reader for %s; trace:\n%s" % (expected, table, trace))

    def assert_paimon_native_profile(self, query):
        """Verify an executed paimon-cpp scan, using the existing BE profile section."""
        settings = self.execute_sql("SELECT @@enable_profile, @@enable_async_profile", True)
        if not settings["status"]:
            raise AssertionError("cannot read profile settings")
        enabled, asynchronous = settings["result"][0]
        try:
            for sql in ("SET enable_profile = true", "SET enable_async_profile = false", query):
                result = self.execute_sql(sql, True)
                if not result["status"]:
                    raise AssertionError("Paimon profile query failed: %s" % result.get("msg"))
            query_id = self.execute_sql("SELECT last_query_id()", True)["result"][0][0]
            result = self.execute_sql("SELECT get_query_profile('%s')" % query_id, True)
            if not result["status"]:
                raise AssertionError("cannot read Paimon query profile")
            profile = "\n".join(str(value) for row in result["result"] for value in row)
            if not re.search(r"^\s*-\s*PaimonNativeReader:", profile, re.MULTILINE):
                raise AssertionError("executed query has no PaimonNativeReader profile section")
        finally:
            self.execute_sql("SET enable_profile = %s" % str(enabled).lower(), True)
            self.execute_sql("SET enable_async_profile = %s" % str(asynchronous).lower(), True)
