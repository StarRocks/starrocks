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

import platform
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parent / "check_paimon_cpp_runtime.sh"


@unittest.skipUnless(platform.system() == "Linux", "ELF runtime validation requires Linux")
class CheckPaimonCppRuntimeTest(unittest.TestCase):
    def setUp(self) -> None:
        for command in ("gcc", "readelf", "ldd", "nm"):
            if shutil.which(command) is None:
                self.skipTest(f"{command} is not installed")

        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.runtime = self.root / "runtime"
        self.runtime.mkdir()
        self.arch = platform.machine()
        if self.arch not in ("x86_64", "aarch64"):
            self.skipTest(f"unsupported test architecture: {self.arch}")

        self._compile_shared("libpaimon.so", "int paimon_symbol(void) { return 1; }")
        self._compile_shared("libpaimon_global_index.so", "int global_index_symbol(void) { return 2; }")
        if self.arch == "x86_64":
            self._compile_shared("liblumina.so", "int lumina_symbol(void) { return 3; }")
            self._compile(
                "libpaimon_lumina_index.so",
                "int lumina_symbol(void); int lumina_index_symbol(void) { return lumina_symbol(); }",
                [f"-L{self.runtime}", "-Wl,--no-as-needed", "-Wl,-rpath,$ORIGIN", "-l:liblumina.so"],
            )
        self._compile_shim(with_runpath=True)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_accepts_relocatable_runtime(self) -> None:
        result = self._check()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("Paimon runtime package check passed", result.stdout)

    def test_rejects_missing_origin_runpath(self) -> None:
        self._compile_shim(with_runpath=False)
        result = self._check()
        self.assertNotEqual(0, result.returncode)
        self.assertIn("must carry an $ORIGIN", result.stderr)

    @unittest.skipUnless(platform.machine() == "x86_64", "Lumina is x86-64 only")
    def test_rejects_absolute_needed_entry(self) -> None:
        source = self._write_source("absolute.c", "int lumina_symbol(void); int bad(void) { return lumina_symbol(); }")
        absolute_plugin = self.runtime / "libpaimon_lumina_index.so"
        self._run(
            [
                "gcc",
                "-shared",
                "-fPIC",
                str(source),
                str(self.runtime / "liblumina.so"),
                "-o",
                str(absolute_plugin),
            ]
        )
        result = self._check()
        self.assertNotEqual(0, result.returncode)
        self.assertIn("absolute DT_NEEDED", result.stderr)

    def _compile_shim(self, *, with_runpath: bool) -> None:
        source = textwrap.dedent(
            """\
            int paimon_symbol(void);
            int global_index_symbol(void);
            #if defined(WITH_LUMINA)
            int lumina_symbol(void);
            int lumina_index_symbol(void);
            #endif
            int shim_symbol(void) {
                int result = paimon_symbol() + global_index_symbol();
            #if defined(WITH_LUMINA)
                result += lumina_symbol() + lumina_index_symbol();
            #endif
                return result;
            }
            """
        )
        flags = [f"-L{self.runtime}", "-Wl,--no-as-needed", "-l:libpaimon.so", "-l:libpaimon_global_index.so"]
        if self.arch == "x86_64":
            flags.extend(["-DWITH_LUMINA", "-l:libpaimon_lumina_index.so", "-l:liblumina.so"])
        if with_runpath:
            flags.append("-Wl,-rpath,$ORIGIN")
        self._compile("libstarrocks_paimon.so", source, flags)

    def _compile_shared(self, name: str, source: str) -> None:
        self._compile(name, source, [])

    def _compile(self, name: str, source: str, flags: list[str]) -> None:
        source_path = self._write_source(f"{name}.c", source)
        self._run(["gcc", "-shared", "-fPIC", str(source_path), *flags, "-o", str(self.runtime / name)])

    def _write_source(self, name: str, source: str) -> Path:
        path = self.root / name
        path.write_text(source)
        return path

    def _run(self, command: list[str]) -> None:
        subprocess.run(command, check=True, text=True, capture_output=True)

    def _check(self) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(SCRIPT), str(self.runtime), self.arch],
            check=False,
            text=True,
            capture_output=True,
        )


if __name__ == "__main__":
    unittest.main()
