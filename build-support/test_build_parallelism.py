#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0

import os
import subprocess
import tempfile
import unittest
import unittest.mock

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUILD_HELPERS = os.path.join(REPO_ROOT, "build-support", "build_helpers.sh")


class TestBuildParallelism(unittest.TestCase):
    def _clean_env(self, env_overrides: dict = None) -> dict:
        env = os.environ.copy()
        for key in (
            "STARROCKS_LINKER",
            "STARROCKS_CXX_LINKER_FLAGS",
            "STARROCKS_CXX_COMMON_FLAGS",
            "LDFLAGS",
            "CXXFLAGS",
            "STARROCKS_GCC_HOME",
            "STARROCKS_LLVM_HOME",
            "CC",
            "CXX",
            "PARALLEL",
            "STARROCKS_CGROUP_ROOT",
            "STARROCKS_PROC_SELF_CGROUP",
            "CMAKE_BUILD_DIR",
            "CMAKE_BUILD_PREFIX",
            "BUILD_TYPE",
        ):
            env.pop(key, None)
        if env_overrides:
            env.update(env_overrides)
        return env

    def run_bash(self, cmd: str, env_overrides: dict = None) -> str:
        env = self._clean_env(env_overrides)
        script = f'. "{BUILD_HELPERS}"\n{cmd}'
        res = subprocess.run(
            ["bash", "-c", script],
            cwd=REPO_ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
        return res.stdout.strip()

    def test_detect_total_ram_gb_positive(self):
        ram = self.run_bash("starrocks_detect_total_ram_gb")
        self.assertTrue(ram.isdigit())
        self.assertGreater(int(ram), 0)

    def test_detect_linker_type_lld(self):
        linker_type = self.run_bash(
            "starrocks_detect_linker_type", {"STARROCKS_LINKER": "lld"}
        )
        self.assertEqual(linker_type, "modern")

    def test_detect_linker_type_bfd(self):
        linker_type = self.run_bash(
            "starrocks_detect_linker_type", {"STARROCKS_LINKER": "bfd"}
        )
        self.assertEqual(linker_type, "legacy")

    def test_detect_linker_type_gold(self):
        linker_type = self.run_bash(
            "starrocks_detect_linker_type", {"STARROCKS_LINKER": "gold"}
        )
        self.assertEqual(linker_type, "modern")

    def test_detect_linker_type_mold(self):
        linker_type = self.run_bash(
            "starrocks_detect_linker_type", {"STARROCKS_LINKER": "mold"}
        )
        self.assertEqual(linker_type, "modern")

    def test_detect_linker_type_cmake_gold_fallback(self):
        cmd = """
        starrocks_is_darwin() { return 1; }
        uname() { echo "x86_64"; }
        g++() { echo "14.3.0"; }
        ldd() { echo "ldd (GNU libc) 2.17"; }
        starrocks_detect_linker_type
        """
        linker_type = self.run_bash(cmd, {"STARROCKS_LINKER": ""})
        self.assertEqual(linker_type, "modern")

    def test_detect_linker_type_cmake_gold_fallback_cxx_mismatch(self):
        cmd = """
        starrocks_is_darwin() { return 1; }
        uname() { echo "x86_64"; }
        gcc() { echo "14.3.0"; }
        g++() { echo "13.2.0"; }
        ldd() { echo "ldd (GNU libc) 2.17"; }
        ld() { echo "GNU ld (GNU Binutils) 2.27"; }
        starrocks_detect_linker_type
        """
        linker_type = self.run_bash(cmd, {"STARROCKS_LINKER": ""})
        self.assertEqual(linker_type, "legacy")

    def test_detect_linker_type_cmake_starrocks_gcc_home(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bin_dir = os.path.join(tmpdir, "bin")
            os.makedirs(bin_dir, exist_ok=True)
            mock_gxx = os.path.join(bin_dir, "g++")
            with open(mock_gxx, "w") as f:
                f.write("#!/bin/sh\necho '14.3.0'\n")
            os.chmod(mock_gxx, 0o755)

            cmd = """
            starrocks_is_darwin() { return 1; }
            uname() { echo "x86_64"; }
            g++() { echo "11.2.0"; }
            ldd() { echo "ldd (GNU libc) 2.17"; }
            starrocks_detect_linker_type
            """
            linker_type = self.run_bash(
                cmd, {"STARROCKS_LINKER": "", "STARROCKS_GCC_HOME": tmpdir, "CXX": ""}
            )
            self.assertEqual(linker_type, "modern")

    def test_detect_linker_type_cmake_starrocks_gcc_home_older_version(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bin_dir = os.path.join(tmpdir, "bin")
            os.makedirs(bin_dir, exist_ok=True)
            mock_gxx = os.path.join(bin_dir, "g++")
            with open(mock_gxx, "w") as f:
                f.write("#!/bin/sh\necho '12.1.0'\n")
            os.chmod(mock_gxx, 0o755)

            cmd = """
            starrocks_is_darwin() { return 1; }
            uname() { echo "x86_64"; }
            g++() { echo "14.3.0"; }
            ldd() { echo "ldd (GNU libc) 2.17"; }
            ld() { echo "GNU ld (GNU Binutils) 2.27"; }
            starrocks_detect_linker_type
            """
            linker_type = self.run_bash(
                cmd, {"STARROCKS_LINKER": "", "STARROCKS_GCC_HOME": tmpdir, "CXX": ""}
            )
            self.assertEqual(linker_type, "legacy")

    def test_detect_linker_type_cxx_overrides_starrocks_gcc_home(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bin_dir = os.path.join(tmpdir, "bin")
            os.makedirs(bin_dir, exist_ok=True)
            mock_gxx = os.path.join(bin_dir, "g++")
            with open(mock_gxx, "w") as f:
                f.write("#!/bin/sh\necho '12.1.0'\n")
            os.chmod(mock_gxx, 0o755)

            mock_cxx = os.path.join(tmpdir, "custom-cxx")
            with open(mock_cxx, "w") as f:
                f.write("#!/bin/sh\necho '14.3.0'\n")
            os.chmod(mock_cxx, 0o755)

            cmd = """
            starrocks_is_darwin() { return 1; }
            uname() { echo "x86_64"; }
            ldd() { echo "ldd (GNU libc) 2.17"; }
            starrocks_detect_linker_type
            """
            linker_type = self.run_bash(
                cmd,
                {
                    "STARROCKS_LINKER": "",
                    "STARROCKS_GCC_HOME": tmpdir,
                    "CC": "gcc",
                    "CXX": mock_cxx,
                },
            )
            self.assertEqual(linker_type, "modern")

    def test_detect_linker_type_cxx_without_cc_is_ignored(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bin_dir = os.path.join(tmpdir, "bin")
            os.makedirs(bin_dir, exist_ok=True)
            mock_gxx = os.path.join(bin_dir, "g++")
            with open(mock_gxx, "w") as f:
                f.write("#!/bin/sh\necho '12.1.0'\n")
            os.chmod(mock_gxx, 0o755)

            mock_cxx = os.path.join(tmpdir, "custom-cxx")
            with open(mock_cxx, "w") as f:
                f.write("#!/bin/sh\necho '14.3.0'\n")
            os.chmod(mock_cxx, 0o755)

            cmd = """
            starrocks_is_darwin() { return 1; }
            uname() { echo "x86_64"; }
            ldd() { echo "ldd (GNU libc) 2.17"; }
            ld() { echo "GNU ld (GNU Binutils) 2.27"; }
            starrocks_detect_linker_type
            """
            linker_type = self.run_bash(
                cmd,
                {
                    "STARROCKS_LINKER": "",
                    "STARROCKS_GCC_HOME": tmpdir,
                    "CC": "",
                    "CXX": mock_cxx,
                },
            )
            self.assertEqual(linker_type, "legacy")

    def test_detect_linker_type_cc_without_cxx_is_ignored(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bin_dir = os.path.join(tmpdir, "bin")
            os.makedirs(bin_dir, exist_ok=True)
            mock_gxx = os.path.join(bin_dir, "g++")
            with open(mock_gxx, "w") as f:
                f.write("#!/bin/sh\necho '12.1.0'\n")
            os.chmod(mock_gxx, 0o755)

            mock_cxx = os.path.join(tmpdir, "custom-cxx")
            with open(mock_cxx, "w") as f:
                f.write("#!/bin/sh\necho '14.3.0'\n")
            os.chmod(mock_cxx, 0o755)

            cmd = """
            starrocks_is_darwin() { return 1; }
            uname() { echo "x86_64"; }
            ldd() { echo "ldd (GNU libc) 2.17"; }
            ld() { echo "GNU ld (GNU Binutils) 2.27"; }
            starrocks_detect_linker_type
            """
            linker_type = self.run_bash(
                cmd,
                {
                    "STARROCKS_LINKER": "",
                    "STARROCKS_GCC_HOME": tmpdir,
                    "CC": mock_cxx,
                    "CXX": "",
                },
            )
            self.assertEqual(linker_type, "legacy")

    def test_detect_linker_type_cc_and_cxx_pair_overrides_starrocks_gcc_home(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bin_dir = os.path.join(tmpdir, "bin")
            os.makedirs(bin_dir, exist_ok=True)
            mock_gxx = os.path.join(bin_dir, "g++")
            with open(mock_gxx, "w") as f:
                f.write("#!/bin/sh\necho '12.1.0'\n")
            os.chmod(mock_gxx, 0o755)

            mock_cxx = os.path.join(tmpdir, "custom-cxx")
            with open(mock_cxx, "w") as f:
                f.write("#!/bin/sh\necho '14.3.0'\n")
            os.chmod(mock_cxx, 0o755)

            cmd = """
            starrocks_is_darwin() { return 1; }
            uname() { echo "x86_64"; }
            ldd() { echo "ldd (GNU libc) 2.17"; }
            ld() { echo "GNU ld (GNU Binutils) 2.27"; }
            starrocks_detect_linker_type
            """
            linker_type = self.run_bash(
                cmd,
                {
                    "STARROCKS_LINKER": "",
                    "STARROCKS_GCC_HOME": tmpdir,
                    "CC": "gcc",
                    "CXX": mock_cxx,
                },
            )
            self.assertEqual(linker_type, "modern")

    def test_run_bash_sanitizes_ambient_compiler_env(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bin_dir = os.path.join(tmpdir, "bin")
            os.makedirs(bin_dir, exist_ok=True)
            mock_gxx = os.path.join(bin_dir, "g++")
            with open(mock_gxx, "w") as f:
                f.write("#!/bin/sh\necho '14.3.0'\n")
            os.chmod(mock_gxx, 0o755)

            ambient = {
                "STARROCKS_GCC_HOME": tmpdir,
                "STARROCKS_LLVM_HOME": tmpdir,
                "CC": "gcc",
                "CXX": os.path.join(tmpdir, "bin", "g++"),
                "STARROCKS_CXX_LINKER_FLAGS": "-fuse-ld=lld",
                "STARROCKS_CXX_COMMON_FLAGS": "-fuse-ld=mold",
                "LDFLAGS": "-fuse-ld=gold",
                "CXXFLAGS": "-Wl,-fuse-ld=lld",
            }
            with unittest.mock.patch.dict(os.environ, ambient, clear=False):
                cmd = """
                starrocks_is_darwin() { return 1; }
                uname() { echo "x86_64"; }
                g++() { echo "13.2.0"; }
                ldd() { echo "ldd (GNU libc) 2.17"; }
                ld() { echo "GNU ld (GNU Binutils) 2.27"; }
                starrocks_detect_linker_type
                """
                linker_type = self.run_bash(cmd)
                self.assertEqual(linker_type, "legacy")

    def test_detect_linker_type_version_14_0_0_is_legacy(self):
        cmd = """
        starrocks_is_darwin() { return 1; }
        uname() { echo "x86_64"; }
        g++() { echo "14.0.0"; }
        ldd() { echo "ldd (GNU libc) 2.17"; }
        ld() { echo "GNU ld (GNU Binutils) 2.27"; }
        starrocks_detect_linker_type
        """
        linker_type = self.run_bash(cmd, {"STARROCKS_LINKER": ""})
        self.assertEqual(linker_type, "legacy")

    def test_detect_linker_type_version_14_0_1_is_modern(self):
        cmd = """
        starrocks_is_darwin() { return 1; }
        uname() { echo "x86_64"; }
        g++() { echo "14.0.1"; }
        ldd() { echo "ldd (GNU libc) 2.17"; }
        ld() { echo "GNU ld (GNU Binutils) 2.27"; }
        starrocks_detect_linker_type
        """
        linker_type = self.run_bash(cmd, {"STARROCKS_LINKER": ""})
        self.assertEqual(linker_type, "modern")

    def test_detect_linker_type_queries_dumpfullversion(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cxx_modern = os.path.join(tmpdir, "mock_cxx_modern")
            with open(cxx_modern, "w") as f:
                f.write(
                    '#!/bin/sh\ncase "$*" in *-dumpfullversion*) echo "14.3.0" ;; *) echo "14" ;; esac\n'
                )
            os.chmod(cxx_modern, 0o755)

            cxx_legacy = os.path.join(tmpdir, "mock_cxx_legacy")
            with open(cxx_legacy, "w") as f:
                f.write(
                    '#!/bin/sh\ncase "$*" in *-dumpfullversion*) echo "14.0.0" ;; *) echo "14" ;; esac\n'
                )
            os.chmod(cxx_legacy, 0o755)

            cmd = """
            starrocks_is_darwin() { return 1; }
            uname() { echo "x86_64"; }
            ldd() { echo "ldd (GNU libc) 2.17"; }
            ld() { echo "GNU ld (GNU Binutils) 2.27"; }
            starrocks_detect_linker_type
            """
            modern_type = self.run_bash(
                cmd,
                {
                    "STARROCKS_LINKER": "",
                    "CC": "gcc",
                    "CXX": cxx_modern,
                },
            )
            self.assertEqual(modern_type, "modern")

            legacy_type = self.run_bash(
                cmd,
                {
                    "STARROCKS_LINKER": "",
                    "CC": "gcc",
                    "CXX": cxx_legacy,
                },
            )
            self.assertEqual(legacy_type, "legacy")

    def test_detect_linker_type_cmake_cache_overrides_env(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            gcc12_dir = os.path.join(tmpdir, "gcc12", "bin")
            os.makedirs(gcc12_dir, exist_ok=True)
            mock_gcc12 = os.path.join(gcc12_dir, "gcc")
            mock_gxx12 = os.path.join(gcc12_dir, "g++")
            for path in (mock_gcc12, mock_gxx12):
                with open(path, "w") as f:
                    f.write("#!/bin/sh\necho '12.1.0'\n")
                os.chmod(path, 0o755)

            gcc14_home = os.path.join(tmpdir, "gcc14")
            gcc14_dir = os.path.join(gcc14_home, "bin")
            os.makedirs(gcc14_dir, exist_ok=True)
            mock_gcc14 = os.path.join(gcc14_dir, "gcc")
            mock_gxx14 = os.path.join(gcc14_dir, "g++")
            for path in (mock_gcc14, mock_gxx14):
                with open(path, "w") as f:
                    f.write("#!/bin/sh\necho '14.3.0'\n")
                os.chmod(path, 0o755)

            build_dir = os.path.join(tmpdir, "ut_build_ASAN")
            os.makedirs(build_dir, exist_ok=True)
            with open(os.path.join(build_dir, "CMakeCache.txt"), "w") as f:
                f.write(
                    f"CMAKE_C_COMPILER:FILEPATH={mock_gcc12}\n"
                    f"CMAKE_CXX_COMPILER:FILEPATH={mock_gxx12}\n"
                )

            env = {
                "STARROCKS_LINKER": "",
                "STARROCKS_GCC_HOME": gcc14_home,
                "CC": mock_gcc14,
                "CXX": mock_gxx14,
            }
            cmd_arg = f"""
            starrocks_is_darwin() {{ return 1; }}
            uname() {{ echo "x86_64"; }}
            ldd() {{ echo "ldd (GNU libc) 2.17"; }}
            ld() {{ echo "GNU ld (GNU Binutils) 2.27"; }}
            starrocks_detect_linker_type "{build_dir}"
            """
            self.assertEqual(self.run_bash(cmd_arg, env), "legacy")

            cmd_env = """
            starrocks_is_darwin() { return 1; }
            uname() { echo "x86_64"; }
            ldd() { echo "ldd (GNU libc) 2.17"; }
            ld() { echo "GNU ld (GNU Binutils) 2.27"; }
            starrocks_detect_linker_type
            """
            env_with_build_dir = dict(env, CMAKE_BUILD_DIR=build_dir)
            self.assertEqual(self.run_bash(cmd_env, env_with_build_dir), "legacy")

    def test_detect_linker_type_cmake_cache_modern_compiler(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            gcc12_home = os.path.join(tmpdir, "gcc12")
            gcc12_dir = os.path.join(gcc12_home, "bin")
            os.makedirs(gcc12_dir, exist_ok=True)
            mock_gxx12 = os.path.join(gcc12_dir, "g++")
            with open(mock_gxx12, "w") as f:
                f.write("#!/bin/sh\necho '12.1.0'\n")
            os.chmod(mock_gxx12, 0o755)

            gcc14_dir = os.path.join(tmpdir, "gcc14", "bin")
            os.makedirs(gcc14_dir, exist_ok=True)
            mock_gxx14 = os.path.join(gcc14_dir, "g++")
            with open(mock_gxx14, "w") as f:
                f.write("#!/bin/sh\necho '14.3.0'\n")
            os.chmod(mock_gxx14, 0o755)

            build_dir = os.path.join(tmpdir, "ut_build_ASAN")
            os.makedirs(build_dir, exist_ok=True)
            with open(os.path.join(build_dir, "CMakeCache.txt"), "w") as f:
                f.write(f"CMAKE_CXX_COMPILER:FILEPATH={mock_gxx14}\n")

            cmd = f"""
            starrocks_is_darwin() {{ return 1; }}
            uname() {{ echo "x86_64"; }}
            ldd() {{ echo "ldd (GNU libc) 2.17"; }}
            ld() {{ echo "GNU ld (GNU Binutils) 2.27"; }}
            starrocks_detect_linker_type "{build_dir}"
            """
            linker_type = self.run_bash(
                cmd,
                {
                    "STARROCKS_LINKER": "",
                    "STARROCKS_GCC_HOME": gcc12_home,
                },
            )
            self.assertEqual(linker_type, "modern")

    def test_detect_linker_type_cmake_compiler_cmake_fallback(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            build_dir = os.path.join(tmpdir, "ut_build_ASAN")
            cmake_files_dir = os.path.join(build_dir, "CMakeFiles", "3.26.4")
            os.makedirs(cmake_files_dir, exist_ok=True)
            with open(os.path.join(build_dir, "CMakeCache.txt"), "w") as f:
                f.write("CMAKE_CXX_COMPILER:FILEPATH=/nonexistent/uncallable/g++\n")
            with open(
                os.path.join(cmake_files_dir, "CMakeCXXCompiler.cmake"), "w"
            ) as f:
                f.write('set(CMAKE_CXX_COMPILER_VERSION "14.3.0")\n')

            cmd = f"""
            starrocks_is_darwin() {{ return 1; }}
            uname() {{ echo "x86_64"; }}
            ldd() {{ echo "ldd (GNU libc) 2.17"; }}
            ld() {{ echo "GNU ld (GNU Binutils) 2.27"; }}
            starrocks_detect_linker_type "{build_dir}"
            """
            linker_type = self.run_bash(cmd, {"STARROCKS_LINKER": ""})
            self.assertEqual(linker_type, "modern")

    def test_detect_linker_type_honors_cxx_linker_flags_env(self):
        cmd_default_legacy = """
        starrocks_is_darwin() { return 1; }
        uname() { echo "x86_64"; }
        g++() { echo "12.1.0"; }
        ldd() { echo "ldd (GNU libc) 2.35"; }
        ld() { echo "GNU ld (GNU Binutils) 2.38"; }
        starrocks_detect_linker_type
        """
        self.assertEqual(
            self.run_bash(
                cmd_default_legacy,
                {"STARROCKS_CXX_LINKER_FLAGS": "-fuse-ld=lld"},
            ),
            "modern",
        )
        self.assertEqual(
            self.run_bash(
                cmd_default_legacy,
                {"STARROCKS_CXX_COMMON_FLAGS": "-fuse-ld=gold"},
            ),
            "modern",
        )
        self.assertEqual(
            self.run_bash(
                cmd_default_legacy,
                {"LDFLAGS": "-Wl,-O1 --ld-path=/usr/bin/ld.mold"},
            ),
            "modern",
        )
        # be/CMakeLists.txt:480-498 overwrites CMAKE_CXX_FLAGS, so ENV{CXXFLAGS} is shadowed
        self.assertEqual(
            self.run_bash(
                cmd_default_legacy,
                {"CXXFLAGS": "-fuse-ld=lld"},
            ),
            "legacy",
        )
        # -Wl,-fuse-ld=lld is passed to ld rather than the compiler driver and does not select lld
        self.assertEqual(
            self.run_bash(
                cmd_default_legacy,
                {"STARROCKS_CXX_LINKER_FLAGS": "-Wl,-fuse-ld=lld"},
            ),
            "legacy",
        )

        self.assertEqual(
            self.run_bash(
                cmd_default_legacy,
                {
                    "STARROCKS_CXX_COMMON_FLAGS": "-fuse-ld=lld",
                    "STARROCKS_CXX_LINKER_FLAGS": "-fuse-ld=bfd",
                },
            ),
            "legacy",
        )

        cmd_default_modern_ld = """
        starrocks_is_darwin() { return 1; }
        uname() { echo "x86_64"; }
        g++() { echo "12.1.0"; }
        ldd() { echo "ldd (GNU libc) 2.35"; }
        ld() { echo "LLD 17.0.6"; }
        starrocks_detect_linker_type
        """
        self.assertEqual(
            self.run_bash(
                cmd_default_modern_ld,
                {"STARROCKS_CXX_LINKER_FLAGS": "-fuse-ld=bfd"},
            ),
            "legacy",
        )
        self.assertEqual(
            self.run_bash(
                cmd_default_modern_ld,
                {"STARROCKS_CXX_LINKER_FLAGS": "-fuse-ld=custom-unknown"},
            ),
            "legacy",
        )

        # On glibc < 2.29 + GCC > 14.0.0, be/CMakeLists.txt:984 appends
        # add_link_options("-fuse-ld=gold") (<LINK_FLAGS>) after CMAKE_EXE_LINKER_FLAGS
        # when STARROCKS_LINKER is empty, but a non-empty unknown STARROCKS_LINKER must not fall through.
        cmd_auto_gold = """
        starrocks_is_darwin() { return 1; }
        uname() { echo "x86_64"; }
        g++() { echo "14.3.0"; }
        ldd() { echo "ldd (GNU libc) 2.17"; }
        ld() { echo "GNU ld (GNU Binutils) 2.27"; }
        starrocks_detect_linker_type
        """
        self.assertEqual(
            self.run_bash(
                cmd_auto_gold,
                {"STARROCKS_CXX_LINKER_FLAGS": "-fuse-ld=bfd"},
            ),
            "modern",
        )
        self.assertEqual(
            self.run_bash(
                cmd_auto_gold,
                {"STARROCKS_LINKER": "custom-unknown-linker"},
            ),
            "legacy",
        )

    def test_detect_linker_type_honors_cmake_cache_linker_flags(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            build_dir = os.path.join(tmpdir, "ut_build_ASAN")
            os.makedirs(build_dir, exist_ok=True)
            cache_file = os.path.join(build_dir, "CMakeCache.txt")

            cmd = f"""
            starrocks_is_darwin() {{ return 1; }}
            uname() {{ echo "x86_64"; }}
            g++() {{ echo "12.1.0"; }}
            ldd() {{ echo "ldd (GNU libc) 2.35"; }}
            ld() {{ echo "GNU ld (GNU Binutils) 2.38"; }}
            starrocks_detect_linker_type "{build_dir}"
            """

            with open(cache_file, "w") as f:
                f.write("CMAKE_EXE_LINKER_FLAGS:STRING=-fuse-ld=lld\n")
            self.assertEqual(self.run_bash(cmd), "modern")

            # Cached CMAKE_CXX_FLAGS is overwritten by be/CMakeLists.txt:480-498,
            # and UT executables do not use CMAKE_SHARED_LINKER_FLAGS.
            for shadowed_cache_entry in (
                "CMAKE_CXX_FLAGS:STRING=-fuse-ld=mold\n",
                "CMAKE_SHARED_LINKER_FLAGS:STRING=-fuse-ld=gold\n",
            ):
                with open(cache_file, "w") as f:
                    f.write(shadowed_cache_entry)
                self.assertEqual(self.run_bash(cmd), "legacy")

            # Once CMAKE_EXE_LINKER_FLAGS is present in CMakeCache.txt (even empty),
            # CMake ignores ambient ENV{LDFLAGS}, whereas STARROCKS_CXX_LINKER_FLAGS is still appended.
            for cached_exe_entry in (
                "CMAKE_EXE_LINKER_FLAGS:STRING=\n",
                "CMAKE_EXE_LINKER_FLAGS:STRING=-fuse-ld=bfd\n",
            ):
                with open(cache_file, "w") as f:
                    f.write(cached_exe_entry)
                self.assertEqual(self.run_bash(cmd), "legacy")
                self.assertEqual(
                    self.run_bash(cmd, {"LDFLAGS": "-fuse-ld=lld"}),
                    "legacy",
                )
                self.assertEqual(
                    self.run_bash(
                        cmd,
                        {"STARROCKS_CXX_LINKER_FLAGS": "-fuse-ld=lld"},
                    ),
                    "modern",
                )

    def test_detect_linker_type_probes_compiler_prog_name_ld(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bin_dir = os.path.join(tmpdir, "bin")
            os.makedirs(bin_dir, exist_ok=True)
            mock_gold = os.path.join(tmpdir, "custom-ld")
            with open(mock_gold, "w") as f:
                f.write("#!/bin/sh\necho 'GNU gold (GNU Binutils 2.38) 1.16'\n")
            os.chmod(mock_gold, 0o755)

            mock_gxx = os.path.join(bin_dir, "g++")
            with open(mock_gxx, "w") as f:
                f.write(
                    f'#!/bin/sh\ncase "$*" in *-print-prog-name=ld*) echo "{mock_gold}" ;; *) echo "12.1.0" ;; esac\n'
                )
            os.chmod(mock_gxx, 0o755)

            cmd = """
            starrocks_is_darwin() { return 1; }
            uname() { echo "x86_64"; }
            ldd() { echo "ldd (GNU libc) 2.35"; }
            ld() { echo "GNU ld (GNU Binutils) 2.38"; }
            starrocks_detect_linker_type
            """
            self.assertEqual(
                self.run_bash(cmd, {"STARROCKS_GCC_HOME": tmpdir}),
                "modern",
            )

        with tempfile.TemporaryDirectory() as llvm_tmpdir:
            llvm_bin = os.path.join(llvm_tmpdir, "bin")
            os.makedirs(llvm_bin, exist_ok=True)
            mock_clangxx = os.path.join(llvm_bin, "clang++")
            with open(mock_clangxx, "w") as f:
                f.write(
                    '#!/bin/sh\ncase "$*" in *-Wl,--version*) echo "LLD 17.0.6" ;; *-print-prog-name=ld*) echo "/usr/bin/ld" ;; *) echo "17.0.6" ;; esac\n'
                )
            os.chmod(mock_clangxx, 0o755)

            self.assertEqual(
                self.run_bash(cmd, {"STARROCKS_LLVM_HOME": llvm_tmpdir}),
                "modern",
            )

    def test_detect_linker_type_aarch64_honors_compiler_and_flags(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bin_dir = os.path.join(tmpdir, "bin")
            os.makedirs(bin_dir, exist_ok=True)
            mock_lld = os.path.join(tmpdir, "custom-lld")
            with open(mock_lld, "w") as f:
                f.write("#!/bin/sh\necho 'LLD 17.0.6'\n")
            os.chmod(mock_lld, 0o755)

            mock_gxx = os.path.join(bin_dir, "g++")
            with open(mock_gxx, "w") as f:
                f.write(
                    f'#!/bin/sh\ncase "$*" in *-print-prog-name=ld*) echo "{mock_lld}" ;; *) echo "12.1.0" ;; esac\n'
                )
            os.chmod(mock_gxx, 0o755)

            cmd = """
            starrocks_is_darwin() { return 1; }
            uname() { echo "aarch64"; }
            ldd() { echo "ldd (GNU libc) 2.17"; }
            ld() { echo "GNU ld (GNU Binutils) 2.38"; }
            starrocks_detect_linker_type
            """
            self.assertEqual(
                self.run_bash(
                    cmd,
                    {"STARROCKS_CXX_LINKER_FLAGS": "-fuse-ld=lld"},
                ),
                "modern",
            )
            self.assertEqual(
                self.run_bash(
                    cmd,
                    {"STARROCKS_GCC_HOME": tmpdir},
                ),
                "modern",
            )

    def test_run_be_ut_cached_compiler_and_clean_refresh(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            gcc12_dir = os.path.join(tmpdir, "gcc12", "bin")
            os.makedirs(gcc12_dir, exist_ok=True)
            mock_gxx12 = os.path.join(gcc12_dir, "g++")
            with open(mock_gxx12, "w") as f:
                f.write("#!/bin/sh\necho '12.1.0'\n")
            os.chmod(mock_gxx12, 0o755)

            gcc14_home = os.path.join(tmpdir, "gcc14")
            gcc14_dir = os.path.join(gcc14_home, "bin")
            os.makedirs(gcc14_dir, exist_ok=True)
            mock_gxx14 = os.path.join(gcc14_dir, "g++")
            with open(mock_gxx14, "w") as f:
                f.write("#!/bin/sh\necho '14.3.0'\n")
            os.chmod(mock_gxx14, 0o755)

            build_prefix = os.path.join(tmpdir, "build_prefix")
            build_dir = os.path.join(build_prefix, "ut_build_ASAN")
            cache_file = os.path.join(build_dir, "CMakeCache.txt")

            def populate_cache():
                os.makedirs(build_dir, exist_ok=True)
                with open(cache_file, "w") as f:
                    f.write(f"CMAKE_CXX_COMPILER:FILEPATH={mock_gxx12}\n")

            def run_slice(*args: str) -> str:
                quoted_args = " ".join(f'"{a}"' for a in args)
                cmd = f"""
                export STARROCKS_HOME="{tmpdir}"
                export CMAKE_BUILD_PREFIX="{build_prefix}"
                export STARROCKS_GCC_HOME="{gcc14_home}"
                starrocks_is_darwin() {{ return 1; }}
                uname() {{ echo "x86_64"; }}
                ldd() {{ echo "ldd (GNU libc) 2.17"; }}
                ld() {{ echo "GNU ld (GNU Binutils) 2.27"; }}
                starrocks_detect_parallelism() {{ echo 16; }}
                starrocks_detect_total_ram_gb() {{ echo 64; }}
                set -- {quoted_args}
                eval "$(awk '
                    /\\. \\$\\{{STARROCKS_HOME\\}}\\/env\\.sh/ {{ seen_env=1; next }}
                    seen_env && !started && /^fi$/ {{ started=1; next }}
                    /^source \\$\\{{STARROCKS_HOME\\}}\\/bin\\/common\\.sh/ {{ exit }}
                    started {{ print }}
                ' "{REPO_ROOT}/run-be-ut.sh")"
                echo "RESULT=${{SR_LINKER_TYPE}}:${{PARALLEL}}"
                """
                return self.run_bash(cmd)

            populate_cache()
            out_no_clean = run_slice()
            self.assertEqual(out_no_clean.splitlines()[-1].strip(), "RESULT=legacy:5")

            populate_cache()
            out_clean = run_slice("--clean")
            self.assertFalse(os.path.exists(cache_file))
            self.assertEqual(out_clean.splitlines()[-1].strip(), "RESULT=modern:16")

            populate_cache()
            out_clean_j = run_slice("--clean", "-j", "7")
            self.assertEqual(out_clean_j.splitlines()[-1].strip(), "RESULT=modern:7")

    def test_detect_total_ram_gb_unreadable_fallback(self):
        cmd = """
        starrocks_is_darwin() { return 1; }
        # simulate unreadable /proc/meminfo and unavailable cgroup limits
        awk() { return 1; }
        cat() { return 1; }
        starrocks_detect_total_ram_gb
        """
        ram = self.run_bash(cmd)
        self.assertEqual(ram, "0")

    def test_detect_total_ram_gb_cgroup_constrained(self):
        # Mock awk returning 64 GiB host memory, and cat returning 16 GiB cgroup memory
        cmd = """
        starrocks_is_darwin() { return 1; }
        awk() { echo 67108864; }
        cat() { echo 17179869184; }
        starrocks_detect_total_ram_gb
        """
        ram = self.run_bash(cmd)
        self.assertEqual(ram, "16")

    def test_detect_total_ram_gb_cgroup_unconstrained(self):
        # Mock awk returning 64 GiB host memory, and cat returning 'max'
        cmd = """
        starrocks_is_darwin() { return 1; }
        awk() { echo 67108864; }
        cat() { echo "max"; }
        starrocks_detect_total_ram_gb
        """
        ram = self.run_bash(cmd)
        self.assertEqual(ram, "64")

    def test_detect_total_ram_gb_cgroup_huge(self):
        # Mock awk returning 64 GiB host memory, and cat returning cgroup v1 unconstrained (9223372036854771712)
        cmd = """
        starrocks_is_darwin() { return 1; }
        awk() { echo 67108864; }
        cat() { echo 9223372036854771712; }
        starrocks_detect_total_ram_gb
        """
        ram = self.run_bash(cmd)
        self.assertEqual(ram, "64")

    def test_detect_total_ram_gb_cgroup_sub_gib(self):
        # Mock awk returning 64 GiB host memory, and cat returning 512 MiB cgroup limit (536870912)
        cmd = """
        starrocks_is_darwin() { return 1; }
        awk() { echo 67108864; }
        cat() { echo 536870912; }
        starrocks_detect_total_ram_gb
        """
        ram = self.run_bash(cmd)
        self.assertEqual(ram, "1")

    def test_detect_total_ram_gb_unreadable_meminfo_with_cgroup(self):
        # Mock unreadable host memory (awk fails), but active 8 GiB container cgroup (8589934592)
        cmd = """
        starrocks_is_darwin() { return 1; }
        awk() { return 1; }
        cat() { echo 8589934592; }
        starrocks_detect_total_ram_gb
        """
        ram = self.run_bash(cmd)
        self.assertEqual(ram, "8")

    def test_detect_total_ram_gb_cgroup_v1_unbounded_sentinel(self):
        # Mock unreadable host memory and unbounded cgroup v1 (9223372036854771712) -> returns 0
        cmd = """
        starrocks_is_darwin() { return 1; }
        awk() { return 1; }
        cat() { echo 9223372036854771712; }
        starrocks_detect_total_ram_gb
        """
        ram = self.run_bash(cmd)
        self.assertEqual(ram, "0")

    def test_parallelism_unknown_ram_conservative_fallback(self):
        # Mock 64 cpus, 0 RAM (unknown/unreadable) on modern linker -> fallback to conservative 64 // 4 + 1 = 17
        cmd = """
        starrocks_detect_parallelism() { echo 64; }
        starrocks_detect_total_ram_gb() { echo 0; }
        starrocks_detect_linker_type() { echo modern; }
        starrocks_detect_ut_parallelism
        """
        p = self.run_bash(cmd, {"PARALLEL": ""})
        self.assertEqual(p, "17")

    def test_parallelism_preserves_caller_env(self):
        p = self.run_bash("starrocks_detect_ut_parallelism", {"PARALLEL": "99"})
        self.assertEqual(p, "99")

    def test_parallelism_ram_constrained_topology(self):
        # Mock 16 cpus, but only 8 GB RAM on modern linker -> clamp to min(16, 8 // 2) = 4
        cmd = """
        starrocks_detect_parallelism() { echo 16; }
        starrocks_detect_total_ram_gb() { echo 8; }
        starrocks_detect_linker_type() { echo modern; }
        starrocks_detect_ut_parallelism
        """
        p = self.run_bash(cmd, {"PARALLEL": ""})
        self.assertEqual(p, "4")

    def test_parallelism_high_spec_topology(self):
        # Mock 18 cpus, 64 GB RAM on modern linker (typical CI) -> min(18, 64 // 2) = 18
        cmd = """
        starrocks_detect_parallelism() { echo 18; }
        starrocks_detect_total_ram_gb() { echo 64; }
        starrocks_detect_linker_type() { echo modern; }
        starrocks_detect_ut_parallelism
        """
        p = self.run_bash(cmd, {"PARALLEL": ""})
        self.assertEqual(p, "18")

    def test_parallelism_legacy_bfd_fallback(self):
        # Mock 16 cpus on legacy bfd -> 16 // 4 + 1 = 5
        cmd = """
        starrocks_detect_parallelism() { echo 16; }
        starrocks_detect_total_ram_gb() { echo 64; }
        starrocks_detect_linker_type() { echo legacy; }
        starrocks_detect_ut_parallelism
        """
        p = self.run_bash(cmd, {"PARALLEL": ""})
        self.assertEqual(p, "5")

    def test_parallelism_zero_or_negative_clamp(self):
        cmd = """
        starrocks_detect_parallelism() { echo 0; }
        starrocks_detect_total_ram_gb() { echo 0; }
        starrocks_detect_linker_type() { echo modern; }
        starrocks_detect_ut_parallelism
        """
        p = self.run_bash(cmd, {"PARALLEL": ""})
        self.assertEqual(p, "1")

    def test_detect_parallelism_cgroup_v2_cfs_quota(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cpu_max_file = os.path.join(tmpdir, "cpu.max")
            with open(cpu_max_file, "w") as f:
                f.write("200000 100000\n")

            cmd = """
            starrocks_is_darwin() { return 1; }
            nproc() { echo 64; }
            starrocks_detect_parallelism
            """
            p = self.run_bash(cmd, {"STARROCKS_CGROUP_ROOT": tmpdir})
            self.assertEqual(p, "2")

    def test_detect_parallelism_cgroup_v2_unconstrained(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cpu_max_file = os.path.join(tmpdir, "cpu.max")
            with open(cpu_max_file, "w") as f:
                f.write("max 100000\n")

            cmd = """
            starrocks_is_darwin() { return 1; }
            nproc() { echo 64; }
            starrocks_detect_parallelism
            """
            p = self.run_bash(cmd, {"STARROCKS_CGROUP_ROOT": tmpdir})
            self.assertEqual(p, "64")

    def test_detect_parallelism_cgroup_v1_cfs_quota(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cpu_dir = os.path.join(tmpdir, "cpu")
            os.makedirs(cpu_dir, exist_ok=True)
            with open(os.path.join(cpu_dir, "cpu.cfs_quota_us"), "w") as f:
                f.write("400000\n")
            with open(os.path.join(cpu_dir, "cpu.cfs_period_us"), "w") as f:
                f.write("100000\n")

            cmd = """
            starrocks_is_darwin() { return 1; }
            nproc() { echo 32; }
            starrocks_detect_parallelism
            """
            p = self.run_bash(cmd, {"STARROCKS_CGROUP_ROOT": tmpdir})
            self.assertEqual(p, "4")

    def test_detect_parallelism_cgroup_v1_unconstrained(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cpu_dir = os.path.join(tmpdir, "cpu")
            os.makedirs(cpu_dir, exist_ok=True)
            with open(os.path.join(cpu_dir, "cpu.cfs_quota_us"), "w") as f:
                f.write("-1\n")

            cmd = """
            starrocks_is_darwin() { return 1; }
            nproc() { echo 32; }
            starrocks_detect_parallelism
            """
            p = self.run_bash(cmd, {"STARROCKS_CGROUP_ROOT": tmpdir})
            self.assertEqual(p, "32")

    def test_detect_parallelism_cgroup_sub_core_clamp(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cpu_max_file = os.path.join(tmpdir, "cpu.max")
            with open(cpu_max_file, "w") as f:
                f.write("50000 100000\n")

            cmd = """
            starrocks_is_darwin() { return 1; }
            nproc() { echo 16; }
            starrocks_detect_parallelism
            """
            p = self.run_bash(cmd, {"STARROCKS_CGROUP_ROOT": tmpdir})
            self.assertEqual(p, "1")

    def test_detect_parallelism_cgroup_higher_than_host(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cpu_max_file = os.path.join(tmpdir, "cpu.max")
            with open(cpu_max_file, "w") as f:
                f.write("1600000 100000\n")

            cmd = """
            starrocks_is_darwin() { return 1; }
            nproc() { echo 4; }
            starrocks_detect_parallelism
            """
            p = self.run_bash(cmd, {"STARROCKS_CGROUP_ROOT": tmpdir})
            self.assertEqual(p, "4")

    def test_detect_parallelism_cgroup_v2_nested_subpath(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "cpu.max"), "w") as f:
                f.write("max 100000\n")
            nested_dir = os.path.join(tmpdir, "system.slice", "runner.service")
            os.makedirs(nested_dir, exist_ok=True)
            with open(os.path.join(nested_dir, "cpu.max"), "w") as f:
                f.write("400000 100000\n")
            proc_cgroup = os.path.join(tmpdir, "proc_self_cgroup")
            with open(proc_cgroup, "w") as f:
                f.write("0::/system.slice/runner.service\n")

            cmd = """
            starrocks_is_darwin() { return 1; }
            nproc() { echo 32; }
            starrocks_detect_parallelism
            """
            p = self.run_bash(
                cmd,
                {
                    "STARROCKS_CGROUP_ROOT": tmpdir,
                    "STARROCKS_PROC_SELF_CGROUP": proc_cgroup,
                },
            )
            self.assertEqual(p, "4")

    def test_detect_parallelism_cgroup_v2_parent_stricter_than_leaf(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "cpu.max"), "w") as f:
                f.write("max 100000\n")
            parent_dir = os.path.join(tmpdir, "system.slice")
            leaf_dir = os.path.join(parent_dir, "runner.service")
            os.makedirs(leaf_dir, exist_ok=True)
            with open(os.path.join(parent_dir, "cpu.max"), "w") as f:
                f.write("200000 100000\n")
            with open(os.path.join(leaf_dir, "cpu.max"), "w") as f:
                f.write("600000 100000\n")
            proc_cgroup = os.path.join(tmpdir, "proc_self_cgroup")
            with open(proc_cgroup, "w") as f:
                f.write("0::/system.slice/runner.service\n")

            cmd = """
            starrocks_is_darwin() { return 1; }
            nproc() { echo 32; }
            starrocks_detect_parallelism
            """
            p = self.run_bash(
                cmd,
                {
                    "STARROCKS_CGROUP_ROOT": tmpdir,
                    "STARROCKS_PROC_SELF_CGROUP": proc_cgroup,
                },
            )
            self.assertEqual(p, "2")

            with open(os.path.join(leaf_dir, "cpu.max"), "w") as f:
                f.write("max 100000\n")
            p_unconstrained_leaf = self.run_bash(
                cmd,
                {
                    "STARROCKS_CGROUP_ROOT": tmpdir,
                    "STARROCKS_PROC_SELF_CGROUP": proc_cgroup,
                },
            )
            self.assertEqual(p_unconstrained_leaf, "2")

    def test_detect_parallelism_cgroup_v1_nested_subpath(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cpu_root = os.path.join(tmpdir, "cpu")
            nested_cpu = os.path.join(cpu_root, "docker", "abc")
            os.makedirs(nested_cpu, exist_ok=True)
            with open(os.path.join(cpu_root, "cpu.cfs_quota_us"), "w") as f:
                f.write("-1\n")
            with open(os.path.join(cpu_root, "cpu.cfs_period_us"), "w") as f:
                f.write("100000\n")
            with open(os.path.join(nested_cpu, "cpu.cfs_quota_us"), "w") as f:
                f.write("600000\n")
            with open(os.path.join(nested_cpu, "cpu.cfs_period_us"), "w") as f:
                f.write("100000\n")
            proc_cgroup = os.path.join(tmpdir, "proc_self_cgroup")
            with open(proc_cgroup, "w") as f:
                f.write("5:cpu,cpuacct:/docker/abc\n")

            cmd = """
            starrocks_is_darwin() { return 1; }
            nproc() { echo 32; }
            starrocks_detect_parallelism
            """
            p = self.run_bash(
                cmd,
                {
                    "STARROCKS_CGROUP_ROOT": tmpdir,
                    "STARROCKS_PROC_SELF_CGROUP": proc_cgroup,
                },
            )
            self.assertEqual(p, "6")

        with tempfile.TemporaryDirectory() as tmpdir2:
            cpuacct_root = os.path.join(tmpdir2, "cpu,cpuacct")
            nested_cpuacct = os.path.join(cpuacct_root, "docker", "abc")
            os.makedirs(nested_cpuacct, exist_ok=True)
            with open(os.path.join(cpuacct_root, "cpu.cfs_quota_us"), "w") as f:
                f.write("-1\n")
            with open(os.path.join(cpuacct_root, "cpu.cfs_period_us"), "w") as f:
                f.write("100000\n")
            with open(os.path.join(nested_cpuacct, "cpu.cfs_quota_us"), "w") as f:
                f.write("600000\n")
            with open(os.path.join(nested_cpuacct, "cpu.cfs_period_us"), "w") as f:
                f.write("100000\n")
            proc_cgroup2 = os.path.join(tmpdir2, "proc_self_cgroup")
            with open(proc_cgroup2, "w") as f:
                f.write("5:cpu,cpuacct:/docker/abc\n")

            p2 = self.run_bash(
                cmd,
                {
                    "STARROCKS_CGROUP_ROOT": tmpdir2,
                    "STARROCKS_PROC_SELF_CGROUP": proc_cgroup2,
                },
            )
            self.assertEqual(p2, "6")

        with tempfile.TemporaryDirectory() as tmpdir_hybrid:
            with open(os.path.join(tmpdir_hybrid, "cpu.max"), "w") as f:
                f.write("800000 100000\n")
            cpuacct_cpu_root = os.path.join(tmpdir_hybrid, "cpuacct,cpu")
            nested_hybrid = os.path.join(cpuacct_cpu_root, "docker", "abc")
            os.makedirs(nested_hybrid, exist_ok=True)
            with open(os.path.join(nested_hybrid, "cpu.cfs_quota_us"), "w") as f:
                f.write("300000\n")
            with open(os.path.join(nested_hybrid, "cpu.cfs_period_us"), "w") as f:
                f.write("100000\n")
            proc_cgroup_hybrid = os.path.join(tmpdir_hybrid, "proc_self_cgroup")
            with open(proc_cgroup_hybrid, "w") as f:
                f.write("0::/\n5:cpuacct,cpu:/docker/abc\n")

            p_hybrid = self.run_bash(
                cmd,
                {
                    "STARROCKS_CGROUP_ROOT": tmpdir_hybrid,
                    "STARROCKS_PROC_SELF_CGROUP": proc_cgroup_hybrid,
                },
            )
            self.assertEqual(p_hybrid, "3")

        with tempfile.TemporaryDirectory() as tmpdir_multi_v1:
            cpu_sub = os.path.join(tmpdir_multi_v1, "cpu", "docker", "abc")
            cpuacct_sub = os.path.join(tmpdir_multi_v1, "cpu,cpuacct", "docker", "abc")
            os.makedirs(cpu_sub, exist_ok=True)
            os.makedirs(cpuacct_sub, exist_ok=True)
            with open(os.path.join(cpu_sub, "cpu.cfs_quota_us"), "w") as f:
                f.write("-1\n")
            with open(os.path.join(cpuacct_sub, "cpu.cfs_quota_us"), "w") as f:
                f.write("500000\n")
            with open(os.path.join(cpuacct_sub, "cpu.cfs_period_us"), "w") as f:
                f.write("100000\n")
            proc_cgroup_multi = os.path.join(tmpdir_multi_v1, "proc_self_cgroup")
            with open(proc_cgroup_multi, "w") as f:
                f.write("5:cpu,cpuacct:/docker/abc\n")

            p_multi = self.run_bash(
                cmd,
                {
                    "STARROCKS_CGROUP_ROOT": tmpdir_multi_v1,
                    "STARROCKS_PROC_SELF_CGROUP": proc_cgroup_multi,
                },
            )
            self.assertEqual(p_multi, "5")

    def test_detect_total_ram_gb_nested_cgroup_v2_and_v1(self):
        with tempfile.TemporaryDirectory() as tmpdir_v2:
            with open(os.path.join(tmpdir_v2, "memory.max"), "w") as f:
                f.write("max\n")
            parent_v2 = os.path.join(tmpdir_v2, "system.slice")
            nested_v2 = os.path.join(parent_v2, "runner.service")
            os.makedirs(nested_v2, exist_ok=True)
            with open(os.path.join(nested_v2, "memory.max"), "w") as f:
                f.write("12884901888\n")
            proc_cgroup_v2 = os.path.join(tmpdir_v2, "proc_self_cgroup")
            with open(proc_cgroup_v2, "w") as f:
                f.write("0::/system.slice/runner.service\n")

            cmd = """
            starrocks_is_darwin() { return 1; }
            awk() { echo 67108864; }
            starrocks_detect_total_ram_gb
            """
            ram_v2 = self.run_bash(
                cmd,
                {
                    "STARROCKS_CGROUP_ROOT": tmpdir_v2,
                    "STARROCKS_PROC_SELF_CGROUP": proc_cgroup_v2,
                },
            )
            self.assertEqual(ram_v2, "12")

            with open(os.path.join(parent_v2, "memory.max"), "w") as f:
                f.write("8589934592\n")
            ram_v2_parent = self.run_bash(
                cmd,
                {
                    "STARROCKS_CGROUP_ROOT": tmpdir_v2,
                    "STARROCKS_PROC_SELF_CGROUP": proc_cgroup_v2,
                },
            )
            self.assertEqual(ram_v2_parent, "8")

        with tempfile.TemporaryDirectory() as tmpdir_v1:
            mem_root = os.path.join(tmpdir_v1, "memory")
            nested_v1 = os.path.join(mem_root, "kubepods", "pod1")
            os.makedirs(nested_v1, exist_ok=True)
            with open(os.path.join(mem_root, "memory.limit_in_bytes"), "w") as f:
                f.write("9223372036854771712\n")
            with open(os.path.join(nested_v1, "memory.limit_in_bytes"), "w") as f:
                f.write("8589934592\n")
            proc_cgroup_v1 = os.path.join(tmpdir_v1, "proc_self_cgroup")
            with open(proc_cgroup_v1, "w") as f:
                f.write("4:memory:/kubepods/pod1\n")

            ram_v1 = self.run_bash(
                cmd,
                {
                    "STARROCKS_CGROUP_ROOT": tmpdir_v1,
                    "STARROCKS_PROC_SELF_CGROUP": proc_cgroup_v1,
                },
            )
            self.assertEqual(ram_v1, "8")

        with tempfile.TemporaryDirectory() as tmpdir_both_root:
            with open(os.path.join(tmpdir_both_root, "memory.max"), "w") as f:
                f.write("34359738368\n")
            mem_root_both = os.path.join(tmpdir_both_root, "memory")
            os.makedirs(mem_root_both, exist_ok=True)
            with open(os.path.join(mem_root_both, "memory.limit_in_bytes"), "w") as f:
                f.write("8589934592\n")
            proc_cgroup_both = os.path.join(tmpdir_both_root, "proc_self_cgroup")
            with open(proc_cgroup_both, "w") as f:
                f.write("0::/\n")

            ram_both = self.run_bash(
                cmd,
                {
                    "STARROCKS_CGROUP_ROOT": tmpdir_both_root,
                    "STARROCKS_PROC_SELF_CGROUP": proc_cgroup_both,
                },
            )
            self.assertEqual(ram_both, "8")

    def test_run_be_ut_detect_parallelism(self):
        # 1. Verify -j is documented in run-be-ut.sh help
        res = subprocess.run(
            ["bash", "-c", "./run-be-ut.sh --help || true"],
            cwd=REPO_ROOT,
            env=self._clean_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.assertIn("-j", res.stdout)
        self.assertIn("build parallel", res.stdout)

        # 2. Verify run-be-ut.sh contains the uncapped parallelism call and diagnostic logging
        run_be_ut_path = os.path.join(REPO_ROOT, "run-be-ut.sh")
        with open(run_be_ut_path, "r") as f:
            content = f.read()
        self.assertIn(
            'PARALLEL="$(starrocks_detect_ut_parallelism "${SR_LINKER_TYPE}" "${SR_RAM_GB}")"',
            content,
        )
        self.assertIn(
            'echo "[INFO] BE UT Build System: ${BUILD_SYSTEM}, Parallelism: -j${PARALLEL} (Linker: ${SR_LINKER_TYPE}, RAM: ${SR_RAM_GB} GiB)"',
            content,
        )

        # 3. Verify preamble execution wires up starrocks_detect_ut_parallelism
        cmd_init = """
        export STARROCKS_HOME="${PWD}"
        . "${STARROCKS_HOME}/build-support/build_helpers.sh"
        starrocks_detect_ut_parallelism() { echo 42; }
        if starrocks_is_darwin; then
            starrocks_setup_darwin_build_env >/dev/null 2>&1 || true
        else
            . "${STARROCKS_HOME}/env.sh" >/dev/null 2>&1 || true
        fi
        PARALLEL=$(starrocks_detect_ut_parallelism)
        echo "INIT_PARALLEL=${PARALLEL}"
        """
        out_init = self.run_bash(cmd_init)
        self.assertEqual(out_init.splitlines()[-1].strip(), "INIT_PARALLEL=42")

        # 4. Verify CLI flag -j overrides PARALLEL during argument parsing
        cmd_flag = """
        export STARROCKS_HOME="${PWD}"
        eval "$(awk '/^done$/ {print; exit} {print}' run-be-ut.sh)"
        echo "FLAG_PARALLEL=${PARALLEL}"
        """
        res_flag = subprocess.run(
            ["bash", "-c", cmd_flag, "_", "-j", "11"],
            cwd=REPO_ROOT,
            env=self._clean_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.assertIn("FLAG_PARALLEL=11", res_flag.stdout)


if __name__ == "__main__":
    unittest.main()
