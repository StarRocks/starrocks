#! /usr/bin/python3
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

import argparse
import math
import os
import shutil
import subprocess
import sys


def parse_gtest_list_tests(output: str) -> list[str]:
    tests: list[str] = []
    suite = None

    for raw in output.splitlines():
        line = raw.split("#", 1)[0].rstrip()
        stripped = line.strip()
        if not stripped:
            continue

        if raw[:1].isspace():
            if suite is not None:
                tests.append(f"{suite}{stripped}")
            continue

        suite = stripped if stripped.endswith(".") else None

    return tests


def run_discover(args: argparse.Namespace) -> int:
    result = subprocess.run(
        [args.binary, f"--gtest_filter={args.gtest_filter}", "--gtest_list_tests"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if result.returncode != 0:
        if result.stdout:
            sys.stdout.write(result.stdout)
        if result.stderr:
            sys.stderr.write(result.stderr)
        return result.returncode

    tests = parse_gtest_list_tests(result.stdout)
    with open(args.output, "w", encoding="utf-8") as fh:
        for test in tests:
            fh.write(test)
            fh.write("\n")
    return 0


def run_plan(args: argparse.Namespace) -> int:
    with open(args.tests_file, "r", encoding="utf-8") as fh:
        tests = [line.strip() for line in fh if line.strip()]

    shutil.rmtree(args.manifest_dir, ignore_errors=True)
    os.makedirs(args.manifest_dir, exist_ok=True)

    total_tests = len(tests)
    if total_tests == 0:
        return 0

    desired_batch_count = min(total_tests, max(args.jobs, args.jobs * args.factor))
    target_tests_per_batch = math.ceil(total_tests / desired_batch_count)

    batches: list[list[str]] = []
    current_batch: list[str] = []
    current_filter_bytes = 0

    for test in tests:
        extra_bytes = len(test) if not current_batch else len(test) + 1
        if current_batch and (
            len(current_batch) >= target_tests_per_batch
            or current_filter_bytes + extra_bytes > args.max_filter_bytes
        ):
            batches.append(current_batch)
            current_batch = []
            current_filter_bytes = 0
            extra_bytes = len(test)

        current_batch.append(test)
        current_filter_bytes += extra_bytes

    if current_batch:
        batches.append(current_batch)

    for idx, batch in enumerate(batches, start=1):
        manifest_path = os.path.join(args.manifest_dir, f"batch_{idx:04d}.txt")
        with open(manifest_path, "w", encoding="utf-8") as fh:
            for test in batch:
                fh.write(test)
                fh.write("\n")

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Helpers for batching starrocks_test gtests.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    discover = subparsers.add_parser("discover", help="List concrete gtests for a filtered binary run.")
    discover.add_argument("--binary", required=True, help="Path to starrocks_test.")
    discover.add_argument("--gtest-filter", required=True, help="Filter passed to --gtest_filter.")
    discover.add_argument("--output", required=True, help="Output file for concrete test names.")
    discover.set_defaults(func=run_discover)

    plan = subparsers.add_parser("plan", help="Split discovered tests into manifest batches.")
    plan.add_argument("--tests-file", required=True, help="Input file containing one concrete test per line.")
    plan.add_argument("--manifest-dir", required=True, help="Output directory for batch manifests.")
    plan.add_argument("--jobs", required=True, type=int, help="Maximum concurrent batch jobs.")
    plan.add_argument("--factor", required=True, type=int, help="Batch count multiplier.")
    plan.add_argument(
        "--max-filter-bytes",
        required=True,
        type=int,
        help="Maximum generated --gtest_filter payload per batch.",
    )
    plan.set_defaults(func=run_plan)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
