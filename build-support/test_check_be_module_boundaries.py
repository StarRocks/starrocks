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

import importlib.util
import json
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parent / "check_be_module_boundaries.py"
SPEC = importlib.util.spec_from_file_location("check_be_module_boundaries", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"failed to load {MODULE_PATH}")
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class CheckBeModuleBoundariesTest(unittest.TestCase):
    def test_load_path_allowlist_ignores_comments_and_blank_lines(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            allowlist_path = Path(tmpdir) / "allowlist.txt"
            allowlist_path.write_text(
                textwrap.dedent(
                    """\
                    # comment

                    src/runtime/exec_env.cpp
                    src/storage/lake/update_manager.cpp
                    """
                )
            )

            self.assertEqual(
                {
                    "src/runtime/exec_env.cpp",
                    "src/storage/lake/update_manager.cpp",
                },
                MODULE.load_path_allowlist(allowlist_path),
            )

    def test_collect_exec_env_include_paths_scans_header_files_in_src_and_test(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)
            (repo / "be" / "test" / "runtime" / "exec_env_helper.h").write_text('#include "runtime/exec_env.h"\n')
            (repo / "be" / "test" / "runtime" / "exec_env_helper.cpp").write_text('#include "runtime/exec_env.h"\n')

            self.assertEqual(
                {
                    "src/column/hash_set.h",
                    "test/runtime/exec_env_helper.h",
                },
                MODULE.collect_exec_env_include_paths(repo),
            )

    def test_collect_exec_env_singleton_paths_scans_be_src_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)
            (repo / "be" / "src" / "runtime" / "runtime.cpp").write_text("void f() { (void)ExecEnv::GetInstance(); }\n")
            (repo / "be" / "test" / "runtime" / "runtime_test.cpp").write_text("void f() { (void)ExecEnv::GetInstance(); }\n")

            self.assertEqual(
                {"src/runtime/runtime.cpp"},
                MODULE.collect_exec_env_singleton_paths(repo),
            )

    def test_diff_allowlist_reports_new_and_stale_paths(self) -> None:
        extra_paths, stale_paths = MODULE.diff_path_allowlist(
            current={"src/runtime/runtime.cpp", "src/column/hash_set.h"},
            allowlist={"src/runtime/runtime.cpp", "src/exec/old.cpp"},
        )

        self.assertEqual({"src/column/hash_set.h"}, extra_paths)
        self.assertEqual({"src/exec/old.cpp"}, stale_paths)

    def test_ci_architecture_filter_covers_checker_code_extensions(self) -> None:
        workflow_path = Path(__file__).resolve().parent.parent / ".github" / "workflows" / "ci-pipeline.yml"
        workflow_lines = workflow_path.read_text().splitlines()

        architecture_patterns = []
        in_architecture_filter = False
        for line in workflow_lines:
            stripped = line.strip()
            if stripped == "architecture:":
                in_architecture_filter = True
                continue
            if not in_architecture_filter:
                continue
            if line.startswith("            ") and not stripped.startswith("- "):
                break
            if stripped.startswith("- 'be/**/*."):
                architecture_patterns.append(stripped.split("'")[1])

        architecture_suffixes = {pattern.removeprefix("be/**/*") for pattern in architecture_patterns}
        self.assertEqual(MODULE.CODE_EXTENSIONS, architecture_suffixes)

    def test_ci_architecture_filter_includes_workflow_file(self) -> None:
        workflow_text = (Path(__file__).resolve().parent.parent / ".github" / "workflows" / "ci-pipeline.yml").read_text()

        self.assertIn("- '.github/workflows/ci-pipeline.yml'", workflow_text)

    def test_ci_baseline_shrink_is_gated_on_base_has_baseline_file(self) -> None:
        workflow_text = (Path(__file__).resolve().parent.parent / ".github" / "workflows" / "ci-pipeline.yml").read_text()

        self.assertIn('name: Determine Baseline Enforcement', workflow_text)
        self.assertIn(
            'if git cat-file -e "${{ steps.merge_base.outputs.base }}:build-support/be_module_boundary_baseline.json" 2>/dev/null; then',
            workflow_text,
        )
        self.assertIn(
            'if [[ "${{ steps.baseline_shrink.outputs.enforce_baseline_shrink }}" == "true" ]]; then',
            workflow_text,
        )
        self.assertNotIn(
            'python3 build-support/check_be_module_boundaries.py --mode changed --base ${{ steps.merge_base.outputs.base }} --enforce-baseline-shrink',
            workflow_text,
        )

    def test_ci_architecture_filter_includes_exec_env_guardrail_files(self) -> None:
        workflow_text = (Path(__file__).resolve().parent.parent / ".github" / "workflows" / "ci-pipeline.yml").read_text()

        self.assertIn("- 'build-support/exec_env_header_include_allowlist.txt'", workflow_text)
        self.assertIn("- 'build-support/exec_env_singleton_allowlist.txt'", workflow_text)

    def test_changed_full_check_paths_include_exec_env_guardrail_files(self) -> None:
        self.assertIn("build-support/exec_env_header_include_allowlist.txt", MODULE.DEFAULT_CHANGED_FULL_CHECK_PATHS)
        self.assertIn("build-support/exec_env_singleton_allowlist.txt", MODULE.DEFAULT_CHANGED_FULL_CHECK_PATHS)

    def test_ci_compute_merge_base_uses_fetched_base_head(self) -> None:
        workflow_text = (Path(__file__).resolve().parent.parent / ".github" / "workflows" / "ci-pipeline.yml").read_text()

        self.assertIn('name: Compute Merge Base', workflow_text)
        self.assertIn('git fetch origin ${{ github.base_ref }}', workflow_text)
        self.assertIn('base="$(git merge-base FETCH_HEAD HEAD)"', workflow_text)
        self.assertIn('[[ -n "${base}" ]]', workflow_text)
        self.assertIn('echo "base=${base}" >> "$GITHUB_OUTPUT"', workflow_text)

    def test_find_baseline_expansions_allows_deletions_only(self) -> None:
        previous = {
            "include_violations": {("base", "be/src/base/orlp/pdqsort.h", "common/compiler_util.h")},
            "target_link_violations": set(),
            "test_link_violations": set(),
        }
        current = {
            "include_violations": set(),
            "target_link_violations": set(),
            "test_link_violations": set(),
        }

        self.assertEqual(
            {
                "include_violations": set(),
                "target_link_violations": set(),
                "test_link_violations": set(),
            },
            MODULE.find_baseline_expansions(previous, current),
        )

    def test_find_baseline_expansions_rejects_added_entries(self) -> None:
        previous = {
            "include_violations": set(),
            "target_link_violations": set(),
            "test_link_violations": set(),
        }
        current = {
            "include_violations": {("base", "be/src/base/orlp/pdqsort.h", "common/compiler_util.h")},
            "target_link_violations": set(),
            "test_link_violations": set(),
        }

        self.assertEqual(
            {
                "include_violations": {("base", "be/src/base/orlp/pdqsort.h", "common/compiler_util.h")},
                "target_link_violations": set(),
                "test_link_violations": set(),
            },
            MODULE.find_baseline_expansions(previous, current),
        )

    def test_find_baseline_expansions_treats_edits_as_additions(self) -> None:
        previous = {
            "include_violations": {("base", "be/src/base/orlp/pdqsort.h", "common/compiler_util.h")},
            "target_link_violations": set(),
            "test_link_violations": set(),
        }
        current = {
            "include_violations": {("base", "be/src/base/orlp/pdqsort.h", "common/new_header.h")},
            "target_link_violations": set(),
            "test_link_violations": set(),
        }

        self.assertEqual(
            {
                "include_violations": {("base", "be/src/base/orlp/pdqsort.h", "common/new_header.h")},
                "target_link_violations": set(),
                "test_link_violations": set(),
            },
            MODULE.find_baseline_expansions(previous, current),
        )

    def test_collects_include_violation_and_allows_exact_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)
            baseline_path = repo / "build-support" / "be_module_boundary_baseline.json"

            manifest = MODULE.load_manifest(repo / "be" / "module_boundary_manifest.json")
            cmake_state = MODULE.parse_cmake_state(repo)
            violations = MODULE.collect_violations(repo, manifest, cmake_state)

            self.assertEqual(1, len(violations.include_violations))
            violation = violations.include_violations[0]
            self.assertEqual("columncore", violation.module)
            self.assertEqual("be/src/column/hash_set.h", violation.path)
            self.assertEqual("runtime/exec_env.h", violation.edge)

            baseline_path.write_text(
                json.dumps(
                    {
                        "include_violations": [
                            {
                                "module": "columncore",
                                "path": "be/src/column/hash_set.h",
                                "edge": "runtime/exec_env.h",
                            }
                        ]
                    },
                    indent=2,
                )
                + "\n"
            )
            baseline = MODULE.load_baseline(baseline_path)
            filtered = MODULE.apply_baseline(violations, baseline)
            self.assertEqual([], filtered.include_violations)

    def test_parses_target_sources_and_test_link_deps(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)

            cmake_state = MODULE.parse_cmake_state(repo)

            self.assertEqual(
                {
                    "be/src/column/column.cpp",
                    "be/src/column/hash_set.cpp",
                },
                set(cmake_state.target_sources["ColumnCore"]),
            )
            self.assertEqual(
                ["ColumnCore", "TypesCore", "Common", "Base", "Gutil", "StarRocksGen"],
                cmake_state.test_target_links["column_test"],
            )

    def test_changed_paths_limit_checked_modules(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)

            manifest = MODULE.load_manifest(repo / "be" / "module_boundary_manifest.json")
            cmake_state = MODULE.parse_cmake_state(repo)
            selected = MODULE.select_modules_for_changed_paths(
                manifest=manifest,
                cmake_state=cmake_state,
                changed_paths={"be/src/column/hash_set.h"},
                repo_root=repo,
            )

            self.assertEqual({"columncore"}, selected)

    def test_changed_paths_include_target_definition_cmakelists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)

            manifest = MODULE.load_manifest(repo / "be" / "module_boundary_manifest.json")
            cmake_state = MODULE.parse_cmake_state(repo)
            selected = MODULE.select_modules_for_changed_paths(
                manifest=manifest,
                cmake_state=cmake_state,
                changed_paths={"be/src/io/CMakeLists.txt", "be/src/column/hash_set.h"},
                repo_root=repo,
            )

            self.assertEqual({"columncore", "iocore"}, selected)

    def test_changed_paths_include_test_target_definition_cmakelists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)

            manifest = MODULE.load_manifest(repo / "be" / "module_boundary_manifest.json")
            cmake_state = MODULE.parse_cmake_state(repo)
            selected = MODULE.select_modules_for_changed_paths(
                manifest=manifest,
                cmake_state=cmake_state,
                changed_paths={"be/test/column/CMakeLists.txt", "be/src/io/io.cpp"},
                repo_root=repo,
            )

            self.assertEqual({"columncore", "iocore"}, selected)

    def test_changed_paths_include_cross_module_test_target_definition_cmakelists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)

            manifest = MODULE.load_manifest(repo / "be" / "module_boundary_manifest.json")
            cmake_state = MODULE.parse_cmake_state(repo)
            selected = MODULE.select_modules_for_changed_paths(
                manifest=manifest,
                cmake_state=cmake_state,
                changed_paths={"be/src/column/hash_set.h", "be/test/runtime/CMakeLists.txt"},
                repo_root=repo,
            )

            self.assertEqual({"columncore", "runtimecore"}, selected)

    def test_collect_owned_files_expands_recursive_manifest_globs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)
            (repo / "be" / "src" / "column" / "header_only_column.h").write_text('#include "runtime/exec_env.h"\n')

            manifest = MODULE.load_manifest(repo / "be" / "module_boundary_manifest.json")
            cmake_state = MODULE.parse_cmake_state(repo)
            columncore = next(module for module in manifest["modules"] if module.id == "columncore")

            owned_files = MODULE.collect_owned_files(columncore, cmake_state, repo)

            self.assertIn("be/src/column/header_only_column.h", owned_files)

    def _write_sample_repo(self, repo: Path) -> None:
        (repo / "build-support").mkdir(parents=True)
        (repo / "be" / "src" / "column").mkdir(parents=True)
        (repo / "be" / "src" / "io").mkdir(parents=True)
        (repo / "be" / "src" / "runtime").mkdir(parents=True)
        (repo / "be" / "src" / "types").mkdir(parents=True)
        (repo / "be" / "test" / "column").mkdir(parents=True)
        (repo / "be" / "test" / "runtime").mkdir(parents=True)
        (repo / "be").mkdir(exist_ok=True)

        (repo / "be" / "module_boundary_manifest.json").write_text(
            json.dumps(
                {
                    "modules": [
                        {
                            "id": "columncore",
                            "doc_label": "ColumnCore",
                            "owned_targets": ["ColumnCore"],
                            "owned_globs": ["be/src/column/**"],
                            "allowed_include_prefixes": [
                                "column/",
                                "types/",
                                "common/",
                                "base/",
                                "gutil/",
                                "gen_cpp/",
                            ],
                            "allowed_target_deps": ["TypesCore", "Common", "Base", "Gutil", "StarRocksGen"],
                            "allowed_test_targets": ["column_test"],
                            "allowed_test_link_deps": [
                                "ColumnCore",
                                "TypesCore",
                                "Common",
                                "Base",
                                "Gutil",
                                "StarRocksGen",
                            ],
                            "remediation": "Move code down or add an interface instead of pulling runtime into ColumnCore.",
                        },
                        {
                            "id": "iocore",
                            "doc_label": "IOCore",
                            "owned_targets": ["IOCore"],
                            "allowed_include_prefixes": [
                                "io/",
                                "common/",
                                "base/",
                                "gutil/",
                            ],
                            "allowed_target_deps": ["Common", "Base", "Gutil"],
                            "allowed_test_targets": [],
                            "allowed_test_link_deps": [],
                            "remediation": "Move code into IOCore or add an interface instead of pulling unrelated dependencies into IO.",
                        },
                        {
                            "id": "runtimecore",
                            "doc_label": "RuntimeCore",
                            "owned_targets": ["RuntimeCore"],
                            "allowed_include_prefixes": [
                                "runtime/",
                                "column/",
                                "common/",
                                "base/",
                                "gutil/",
                            ],
                            "allowed_target_deps": ["ColumnCore", "Common", "Base", "Gutil"],
                            "allowed_test_targets": ["runtime_core_test"],
                            "allowed_test_link_deps": ["RuntimeCore", "ColumnCore", "Common", "Base", "Gutil"],
                            "remediation": "Move code down or add an interface instead of pulling unrelated dependencies into RuntimeCore.",
                        }
                    ]
                },
                indent=2,
            )
            + "\n"
        )
        (repo / "build-support" / "be_module_boundary_baseline.json").write_text("{}\n")
        (repo / "be" / "src" / "column" / "CMakeLists.txt").write_text(
            textwrap.dedent(
                """\
                ADD_BE_LIB(ColumnCore
                    column.cpp
                    hash_set.cpp
                )
                """
            )
        )
        (repo / "be" / "src" / "io" / "CMakeLists.txt").write_text(
            textwrap.dedent(
                """\
                ADD_BE_LIB(IOCore
                    io.cpp
                )
                """
            )
        )
        (repo / "be" / "src" / "runtime" / "CMakeLists.txt").write_text(
            textwrap.dedent(
                """\
                ADD_BE_LIB(RuntimeCore
                    runtime.cpp
                )
                """
            )
        )
        (repo / "be" / "test" / "column" / "CMakeLists.txt").write_text(
            textwrap.dedent(
                """\
                set(COLUMN_TEST_LINK_LIBS
                    ColumnCore
                    TypesCore
                    Common
                    Base
                    Gutil
                    StarRocksGen
                )

                target_link_libraries(column_test ${COLUMN_TEST_LINK_LIBS} gtest_main)
                """
            )
        )
        (repo / "be" / "test" / "runtime" / "CMakeLists.txt").write_text(
            textwrap.dedent(
                """\
                set(RUNTIME_CORE_TEST_LINK_LIBS
                    RuntimeCore
                    ColumnCore
                    Common
                    Base
                    Gutil
                )

                target_link_libraries(runtime_core_test ${RUNTIME_CORE_TEST_LINK_LIBS} gtest_main)
                """
            )
        )
        (repo / "be" / "src" / "column" / "column.cpp").write_text('#include "column/column.h"\n')
        (repo / "be" / "src" / "column" / "hash_set.cpp").write_text('#include "column/hash_set.h"\n')
        (repo / "be" / "src" / "io" / "io.cpp").write_text('#include "io/io.h"\n')
        (repo / "be" / "src" / "runtime" / "runtime.cpp").write_text('#include "runtime/runtime_state.h"\n')
        (repo / "be" / "src" / "column" / "hash_set.h").write_text(
            textwrap.dedent(
                """\
                #pragma once

                #include "runtime/exec_env.h"
                """
            )
        )


if __name__ == "__main__":
    unittest.main()
