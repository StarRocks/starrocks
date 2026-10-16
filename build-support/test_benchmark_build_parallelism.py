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
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BENCHMARK_SCRIPT = os.path.join(
    REPO_ROOT, "build-support", "benchmark_build_parallelism.sh"
)


class TestBenchmarkBuildParallelism(unittest.TestCase):
    def test_benchmark_no_arguments(self):
        res = subprocess.run(
            ["bash", BENCHMARK_SCRIPT],
            cwd=REPO_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.assertEqual(res.returncode, 0)
        self.assertIn(
            "STARROCKS BUILD PARALLELISM BEFORE / AFTER VALIDATION", res.stdout
        )
        self.assertIn("Throughput Multiplier", res.stdout)

    def test_benchmark_dry_run_check(self):
        res = subprocess.run(
            ["bash", BENCHMARK_SCRIPT, "--dry-run-check"],
            cwd=REPO_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.assertEqual(res.returncode, 0)
        self.assertIn("[PASS] Validation succeeded.", res.stdout)

    def test_benchmark_invalid_option_fails(self):
        res = subprocess.run(
            ["bash", BENCHMARK_SCRIPT, "--invalid-flag"],
            cwd=REPO_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.assertEqual(res.returncode, 1)
        self.assertIn("Usage:", res.stderr)


if __name__ == "__main__":
    unittest.main()
