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

"""Versioned Paimon fixtures and SQL-Tester helpers. No third-party dependencies."""

import hashlib
import json
from pathlib import Path
import re
import subprocess


def table_path(name):
    if not re.fullmatch(r"[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*", name):
        raise ValueError("invalid fixture table: %s" % name)
    database, table = name.split(".")
    return "%s.db/%s" % (database, table)


def load_manifest(root):
    root = Path(root)
    manifest = json.loads((root / "MANIFEST.json").read_text())
    for key in ("total_bytes", "per_table_bytes", "per_change_bytes"):
        if type(manifest["budget"][key]) is not int or manifest["budget"][key] <= 0:
            raise ValueError("%s must be a positive integer" % key)
    for path in root.rglob("*"):
        if path.is_symlink():
            raise ValueError("fixture symlink is not allowed: %s" % path)
    for path in root.iterdir():
        if path.name not in ("MANIFEST.json", "datagen") and not (path.is_dir() and path.name.endswith(".db")):
            raise ValueError("unmanaged fixture path: %s" % path)
    expected = set()
    total = 0
    for name, entry in manifest["tables"].items():
        relative = table_path(name)
        expected.add(relative)
        directory = root / relative
        files = {p.relative_to(directory).as_posix(): p for p in directory.rglob("*") if p.is_file()}
        if not files or set(files) != set(entry["files"]):
            raise ValueError("%s: file list differs from MANIFEST" % name)
        size = 0
        for filename, path in files.items():
            size_on_disk = path.stat().st_size
            if size_on_disk > manifest["budget"]["per_table_bytes"]:
                raise ValueError("%s: exceeds per_table_bytes" % name)
            data = path.read_bytes()
            if hashlib.md5(data).hexdigest() != entry["files"][filename]:
                raise ValueError("%s/%s: checksum mismatch" % (name, filename))
            size += len(data)
        if size != entry["bytes"]:
            raise ValueError("%s: bytes differs from MANIFEST" % name)
        if size > manifest["budget"]["per_table_bytes"]:
            raise ValueError("%s: exceeds per_table_bytes" % name)
        total += size
    actual = {p.relative_to(root).as_posix() for db in root.glob("*.db") for p in db.iterdir()}
    if actual != expected:
        raise ValueError("table directories differ from MANIFEST: %s" % (actual ^ expected))
    if total > manifest["budget"]["total_bytes"]:
        raise ValueError("fixtures exceed total_bytes")
    if set(manifest["tables"]) & set(manifest["retired"]):
        raise ValueError("retired table names cannot be reused")
    return manifest


def validate_transition(old, new):
    if old["budget"] != new["budget"] and (
            not new["budget"].get("note") or new["budget"].get("note") == old["budget"].get("note")):
        raise ValueError("budget changes require a new budget.note explaining why")
    if not set(old["retired"]) <= set(new["retired"]):
        raise ValueError("retired names cannot be removed")
    if set(old["retired"]) & set(new["tables"]):
        raise ValueError("retired table names cannot be reused")
    for name, entry in old["tables"].items():
        if name in new["tables"]:
            if entry["files"] != new["tables"][name]["files"]:
                raise ValueError("%s: existing fixture is immutable; use a new table name" % name)
        elif name not in new["retired"]:
            raise ValueError("%s: deleted table must be retired" % name)


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
        return warehouse_uri(self.oss_bucket, self.paimon_fixture_prefix, run_id)

    def _paimon_oss(self, *args):
        # Use the runner's ossutil credentials, never put keys on the command line.
        result = subprocess.run(
            ["ossutil64", *args, "-e", self.oss_endpoint],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=120,
        )
        if result.returncode:
            raise RuntimeError("Paimon fixture OSS operation failed (%s): %s" % (args[0], result.stderr))

    def paimon_stage(self, run_id, tables):
        if self.paimon_fixture_source != "repo":
            raise ValueError("only paimon_fixture_source=repo is supported")
        root = Path(self.paimon_fixture_root)
        manifest = load_manifest(root)
        names = tables.split(",")
        if not names or any(name not in manifest["tables"] for name in names):
            raise ValueError("unknown fixture table in %s" % tables)
        warehouse = self._paimon_warehouse(run_id)
        # Validate the complete selection before uploading anything. CLEANUP also handles partial uploads.
        for name in names:
            relative = table_path(name)
            self._paimon_oss("cp", "-r", "-f", str(root / relative) + "/", warehouse + relative + "/")

    def create_paimon_catalog(self, catalog, catalog_type, run_id):
        if catalog_type != "filesystem":
            raise ValueError("fixture catalogs must use filesystem")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
            raise ValueError("invalid fixture catalog name")
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

    def paimon_cleanup(self, catalog, run_id):
        warehouse = self._paimon_warehouse(run_id)
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
            raise ValueError("invalid fixture catalog name")
        try:
            self.execute_sql("SET CATALOG default_catalog")
            result = self.execute_sql("DROP CATALOG IF EXISTS `%s`" % catalog)
            if not result["status"]:
                raise RuntimeError("failed to drop Paimon fixture catalog %s" % catalog)
        finally:
            self._paimon_oss("rm", "-r", "-f", warehouse)

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
