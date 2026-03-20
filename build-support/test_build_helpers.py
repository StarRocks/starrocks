#!/usr/bin/env python3

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

from __future__ import annotations

import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
HELPERS_PATH = REPO_ROOT / "build-support" / "build_helpers.sh"


def _make_executable(path: Path, content: str) -> None:
    path.write_text(content)
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


class BuildHelpersTest(unittest.TestCase):
    def _run_helper(self, shell_body: str, *, ostype: str, thirdparty_root: Path, host_bin: Path) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env["OSTYPE"] = ostype
        env["STARROCKS_THIRDPARTY"] = str(thirdparty_root)
        env["PATH"] = f"{host_bin}:{env['PATH']}"

        script = f"""\
set -euo pipefail
source "{HELPERS_PATH}"
{shell_body}
"""

        return subprocess.run(
            ["bash", "-c", script],
            cwd=REPO_ROOT,
            env=env,
            text=True,
            capture_output=True,
        )

    def test_darwin_be_requires_bundled_protoc(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            thirdparty_root = tmp_path / "thirdparty"
            host_bin = tmp_path / "host-bin"
            (thirdparty_root / "installed" / "bin").mkdir(parents=True)
            host_bin.mkdir()
            _make_executable(host_bin / "protoc", "#!/bin/sh\necho host protoc\n")

            result = self._run_helper(
                'starrocks_require_gensrc_tool "protoc" "1"',
                ostype="darwin23",
                thirdparty_root=thirdparty_root,
                host_bin=host_bin,
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("thirdparty/installed/bin/protoc", result.stderr)
        self.assertIn("3.14.0", result.stderr)

    def test_darwin_format_lib_requires_bundled_thrift(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            thirdparty_root = tmp_path / "thirdparty"
            host_bin = tmp_path / "host-bin"
            (thirdparty_root / "installed" / "bin").mkdir(parents=True)
            host_bin.mkdir()
            _make_executable(host_bin / "thrift", "#!/bin/sh\necho host thrift\n")

            result = self._run_helper(
                'starrocks_require_gensrc_tool "thrift" "1"',
                ostype="darwin23",
                thirdparty_root=thirdparty_root,
                host_bin=host_bin,
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("thirdparty/installed/bin/thrift", result.stderr)
        self.assertIn("Host thrift on PATH is not supported", result.stderr)

    def test_darwin_be_prefers_bundled_protoc(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            thirdparty_root = tmp_path / "thirdparty"
            bundled_bin = thirdparty_root / "installed" / "bin"
            host_bin = tmp_path / "host-bin"
            bundled_bin.mkdir(parents=True)
            host_bin.mkdir()
            _make_executable(bundled_bin / "protoc", "#!/bin/sh\necho bundled protoc\n")
            _make_executable(host_bin / "protoc", "#!/bin/sh\necho host protoc\n")

            result = self._run_helper(
                'starrocks_require_gensrc_tool "protoc" "1"',
                ostype="darwin23",
                thirdparty_root=thirdparty_root,
                host_bin=host_bin,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), str(bundled_bin / "protoc"))

    def test_linux_be_can_fall_back_to_host_protoc(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            thirdparty_root = tmp_path / "thirdparty"
            host_bin = tmp_path / "host-bin"
            (thirdparty_root / "installed" / "bin").mkdir(parents=True)
            host_bin.mkdir()
            host_protoc = host_bin / "protoc"
            _make_executable(host_protoc, "#!/bin/sh\necho host protoc\n")

            result = self._run_helper(
                'starrocks_require_gensrc_tool "protoc" "0"',
                ostype="linux-gnu",
                thirdparty_root=thirdparty_root,
                host_bin=host_bin,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), str(host_protoc))

    def test_darwin_fe_only_does_not_require_bundled_codegen_tools(self) -> None:
        result = self._run_helper(
            'if starrocks_should_require_bundled_codegen_tools "0" "0"; then echo require; else echo allow; fi',
            ostype="darwin23",
            thirdparty_root=REPO_ROOT / "thirdparty",
            host_bin=REPO_ROOT,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "allow")


if __name__ == "__main__":
    unittest.main()
