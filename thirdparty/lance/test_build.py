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

from contextlib import contextmanager
import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("lance_build", Path(__file__).with_name("build.py"))
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)


@contextmanager
def restrictive_umask():
    previous = os.umask(0o077)
    try:
        yield
    finally:
        os.umask(previous)


class LanceBuildTest(unittest.TestCase):
    def test_lance_target_downloads_only_its_protobuf_prerequisite(self):
        env = dict(os.environ, TP_DIR=str(builder.SOURCE.parent),
                   TP_ARCHIVES="PROTOBUF PAIMON_CPP OPENSSL")
        for packages, expected in [("lance", {"PROTOBUF"}),
                                   ("lance paimon_cpp", {"PROTOBUF", "PAIMON_CPP"})]:
            with self.subTest(packages=packages):
                archives = subprocess.check_output(
                    ["bash", "-c", '. "$TP_DIR/package-manifest.sh"; starrocks_package_archives "$@"',
                     "test", *packages.split()], env=env, text=True)
                self.assertEqual(expected, set(archives.split()))

    def test_standalone_image_downloads_only_lance_build_prerequisites(self):
        env = dict(os.environ, TP_DIR=str(builder.SOURCE.parent),
                   TP_ARCHIVES="PROTOBUF GTEST ZLIB PAIMON_CPP OPENSSL")
        archives = subprocess.check_output(
            ["bash", "-c", '. "$TP_DIR/package-manifest.sh"; starrocks_package_archives zlib protobuf lance'],
            env=env, text=True)
        self.assertEqual({"ZLIB", "PROTOBUF", "GTEST"}, set(archives.split()))

    def test_missing_or_old_rust_is_rejected(self):
        with patch.object(builder.shutil, "which", return_value=None):
            with self.assertRaisesRegex(RuntimeError, "Install Rust"):
                builder.toolchain({})
        with patch.object(builder.shutil, "which", side_effect=lambda name, **kwargs: name), \
                patch.object(builder.subprocess, "check_output", return_value="rustc 1.95.0"):
            with self.assertRaisesRegex(RuntimeError, "requires Rust"):
                builder.toolchain({})

    def test_cross_target_is_rejected(self):
        with patch.object(builder.shutil, "which", side_effect=lambda name, **kwargs: name), \
                patch.object(builder.subprocess, "check_output", side_effect=[
                    "rustc 1.96.1", "host: x86_64-unknown-linux-gnu\n"]):
            with self.assertRaisesRegex(RuntimeError, "natively"):
                builder.toolchain({"CARGO_BUILD_TARGET": "aarch64-unknown-linux-gnu"})

    def test_install_preserves_unowned_prefix(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stage, prefix = root / "stage", root / "installed"
            stage.mkdir()
            prefix.mkdir()
            (prefix / "unrelated").write_text("keep")
            with self.assertRaisesRegex(RuntimeError, "non-Lance"):
                builder.install(stage, prefix)
            self.assertEqual("keep", (prefix / "unrelated").read_text())

    def test_failed_install_restores_previous_package(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stage, prefix = root / "missing-stage", root / "installed"
            (prefix / "share/lance").mkdir(parents=True)
            (prefix / "share/lance/manifest.json").write_text("previous")
            with self.assertRaises(FileNotFoundError):
                builder.install(stage, prefix)
            self.assertEqual("previous", (prefix / "share/lance/manifest.json").read_text())

    def test_native_package_is_installed_only_after_tests_pass(self):
        for fail_tests, missing_fixture, encoded_flags, fresh_parent in [
                (True, False, "-Cdebuginfo=1", False), (False, True, "-Cdebuginfo=1", False),
                (False, False, "-Cdebuginfo=1", False), (False, False, "", False),
                (False, False, None, False), (False, False, "", True)]:
            with self.subTest(fail_tests=fail_tests, missing_fixture=missing_fixture,
                              encoded_flags=encoded_flags, fresh_parent=fresh_parent), \
                    tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                prefix = root / "new/nested/install" if fresh_parent else root / "install"
                target = root / "target"
                marker = prefix / "share/lance/manifest.json"
                if not fresh_parent:
                    marker.parent.mkdir(parents=True)
                    marker.write_text("previous")
                args = SimpleNamespace(prefix=prefix, target_dir=target, protoc=Path("protoc"), jobs=2, offline=True)
                commands = []
                build_env = dict(os.environ, RUSTFLAGS="-Copt-level=2")
                build_env.pop("CARGO_ENCODED_RUSTFLAGS", None)
                if encoded_flags is not None:
                    build_env["CARGO_ENCODED_RUSTFLAGS"] = encoded_flags
                expected_flags = (["-Copt-level=2"] if encoded_flags is None else
                                  [encoded_flags] if encoded_flags else [])
                expected_flags.append("-Cforce-frame-pointers=yes")

                def run(command, *, env, check):
                    self.assertTrue(check)
                    self.assertIn("--locked", command)
                    self.assertIn("--offline", command)
                    self.assertEqual(expected_flags, env["CARGO_ENCODED_RUSTFLAGS"].split("\x1f"))
                    commands.append(command[1])
                    if command[1] == "build":
                        library = target / "x86_64-unknown-linux-gnu/release/libstarrocks_lance.so"
                        library.parent.mkdir(parents=True)
                        library.write_bytes(b"test library")
                    elif fail_tests:
                        raise subprocess.CalledProcessError(1, command)
                    elif not missing_fixture:
                        versions = Path(env["LANCE_TEST_DATASET"]) / "_versions"
                        versions.mkdir(parents=True)
                        (versions / "1.manifest").write_bytes(b"fixture")

                with patch.object(builder.platform, "system", return_value="Linux"), \
                        patch.object(builder.platform, "machine", return_value="x86_64"), \
                        patch.object(builder, "toolchain", return_value=("cargo", "rustc", "x86_64-unknown-linux-gnu", "rustc 1.96.1")), \
                        patch.object(builder.shutil, "which", return_value="/usr/bin/protoc"), \
                        patch.object(builder.subprocess, "run", side_effect=run), \
                        patch.dict(os.environ, build_env, clear=True), restrictive_umask():
                    if fail_tests or missing_fixture:
                        with self.assertRaises((subprocess.CalledProcessError, RuntimeError)):
                            builder.build(args)
                        self.assertEqual("previous", marker.read_text())
                    else:
                        builder.build(args)
                        self.assertNotEqual("previous", marker.read_text())
                        # A clean install must also expose parents created under umask 077.
                        if fresh_parent:
                            for parent in (prefix.parent, prefix.parent.parent):
                                self.assertEqual(0o755, parent.stat().st_mode & 0o777, str(parent))
                        # Never relax the permissions of an existing ancestor.
                        self.assertEqual(0o700, root.stat().st_mode & 0o777)
                        self.assertEqual(0o755, prefix.stat().st_mode & 0o777)
                        for path in prefix.rglob("*"):
                            executable = path.is_dir() or path.name == "libstarrocks_lance.so"
                            self.assertEqual(0o755 if executable else 0o644,
                                             path.stat().st_mode & 0o777, str(path))
                        config = (prefix / "lib/cmake/StarRocksLance/StarRocksLanceConfig.cmake").read_text()
                        self.assertIn(builder.sha256(prefix / "lib/libstarrocks_lance.so"), config)
                        self.assertNotIn(str(root), config)  # The installed package is relocatable.
                self.assertEqual(["build", "test"], commands)
                self.assertEqual([], list(root.glob(".lance-install-*")))


if __name__ == "__main__":
    unittest.main()
