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

import hashlib
import importlib.util
import json
from pathlib import Path
import platform
import os
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("installer", Path(__file__).with_name("install-lance.py"))
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)


class LanceInstallTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "package"
        self.prefix = self.root / "installed/lance"
        self.library = "lib/libstarrocks_lance" + (".dylib" if platform.system() == "Darwin" else ".so")
        for name in (self.library, "include/lance_rs_ffi.h", "lib/cmake/StarRocksLance/StarRocksLanceConfig.cmake",
                     "lib/cmake/StarRocksLance/StarRocksLanceConfigVersion.cmake"):
            file = self.source / name
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_bytes(b"package")
        manifest = {"SYSTEM": platform.system(), "MACHINE": platform.machine().replace("arm64", "aarch64"),
                    "LIBRARY_SHA256": hashlib.sha256(b"package").hexdigest(),
                    "HEADER_SHA256": hashlib.sha256(b"package").hexdigest()}
        self.manifest = self.source / "share/lance/manifest.json"
        self.manifest.parent.mkdir(parents=True)
        self.manifest.write_text(json.dumps(manifest))

    def test_package_selection_downloads_only_the_binary_archive(self):
        env = dict(os.environ, TP_DIR=str(Path(__file__).parent), TP_ARCHIVES="LANCE PROTOBUF GTEST")
        archives = subprocess.check_output([
            "bash", "-c", '. "$TP_DIR/package-manifest.sh"; starrocks_package_archives lance'
        ], env=env, text=True)
        self.assertEqual("LANCE", archives.strip())
        for supported in ("0", "1"):
            env["LANCE_SUPPORTED"] = supported
            packages = subprocess.check_output([
                "bash", "-c", '. "$TP_DIR/package-manifest.sh"; starrocks_set_default_packages x86_64; '
                'echo "${STARROCKS_THIRDPARTY_ALL_PACKAGES[@]}"'
            ], env=env, text=True).split()
            self.assertEqual(supported == "1", "lance" in packages)

    def test_install_and_upgrade(self):
        installer.install(self.source, self.prefix)
        (self.prefix / "stale").touch()
        installer.install(self.source, self.prefix)
        self.assertFalse((self.prefix / "stale").exists())
        self.assertEqual(b"package", (self.prefix / self.library).read_bytes())

    def test_corruption_and_wrong_platform_leave_existing_install_untouched(self):
        installer.install(self.source, self.prefix)
        (self.source / self.library).write_bytes(b"corrupt")
        with self.assertRaisesRegex(RuntimeError, "checksum"):
            installer.install(self.source, self.prefix)
        with patch.object(installer.platform, "machine", return_value="unsupported"):
            with self.assertRaisesRegex(RuntimeError, "platform"):
                installer.install(self.source, self.prefix)
        self.assertEqual(b"package", (self.prefix / self.library).read_bytes())

    def test_new_install_parents_remain_traversable_under_restrictive_umask(self):
        previous = os.umask(0o077)
        try:
            prefix = self.root / "fresh/nested/lance"
            installer.install(self.source, prefix)
        finally:
            os.umask(previous)
        for path in (prefix, prefix.parent, prefix.parent.parent):
            self.assertEqual(0o755, path.stat().st_mode & 0o777)

    def test_unowned_prefix_is_preserved(self):
        self.prefix.mkdir(parents=True)
        (self.prefix / "unrelated").touch()
        with self.assertRaisesRegex(RuntimeError, "non-Lance"):
            installer.install(self.source, self.prefix)
        self.assertTrue((self.prefix / "unrelated").exists())

    def test_failed_publication_restores_previous_install(self):
        installer.install(self.source, self.prefix)
        original = Path.rename

        def fail_stage(path, target):
            if path.name == "new":
                raise OSError("publication failed")
            return original(path, target)

        with patch.object(Path, "rename", fail_stage):
            with self.assertRaisesRegex(OSError, "publication failed"):
                installer.install(self.source, self.prefix)
        self.assertEqual(b"package", (self.prefix / self.library).read_bytes())
