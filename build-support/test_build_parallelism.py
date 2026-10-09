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

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUILD_HELPERS = os.path.join(REPO_ROOT, "build-support", "build_helpers.sh")

class TestBuildParallelism(unittest.TestCase):
    def run_bash(self, cmd: str, env_overrides: dict = None) -> str:
        env = os.environ.copy()
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
            check=True
        )
        return res.stdout.strip()

    def test_detect_total_ram_gb_positive(self):
        ram = self.run_bash("starrocks_detect_total_ram_gb")
        self.assertTrue(ram.isdigit())
        self.assertGreater(int(ram), 0)

    def test_detect_linker_type_lld(self):
        linker_type = self.run_bash(
            "starrocks_detect_linker_type",
            {"STARROCKS_LINKER": "lld"}
        )
        self.assertEqual(linker_type, "modern")

    def test_detect_linker_type_bfd(self):
        linker_type = self.run_bash(
            "starrocks_detect_linker_type",
            {"STARROCKS_LINKER": "bfd"}
        )
        self.assertEqual(linker_type, "legacy")

    def test_parallelism_preserves_caller_env(self):
        p = self.run_bash(
            "starrocks_detect_ut_parallelism",
            {"PARALLEL": "99"}
        )
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

if __name__ == "__main__":
    unittest.main()
