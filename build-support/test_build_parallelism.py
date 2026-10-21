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
    def run_bash(self, cmd: str, env_overrides: dict = None) -> str:
        env = os.environ.copy()
        for key in (
            "STARROCKS_LINKER",
            "STARROCKS_GCC_HOME",
            "STARROCKS_LLVM_HOME",
            "CC",
            "CXX",
            "PARALLEL",
            "STARROCKS_CGROUP_ROOT",
            "CMAKE_BUILD_DIR",
            "CMAKE_BUILD_PREFIX",
            "BUILD_TYPE",
        ):
            env.pop(key, None)
        if env_overrides:
            env.update(env_overrides)
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

    def test_run_be_ut_detect_parallelism(self):
        # 1. Verify -j is documented in run-be-ut.sh help
        res = subprocess.run(
            ["bash", "-c", "./run-be-ut.sh --help || true"],
            cwd=REPO_ROOT,
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
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.assertIn("FLAG_PARALLEL=11", res_flag.stdout)


if __name__ == "__main__":
    unittest.main()
