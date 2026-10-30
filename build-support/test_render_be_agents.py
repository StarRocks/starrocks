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
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parent / "render_be_agents.py"
SPEC = importlib.util.spec_from_file_location("render_be_agents", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"failed to load {MODULE_PATH}")
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class RenderBeAgentsTest(unittest.TestCase):
    def test_rendered_section_contains_manifest_content(self) -> None:
        rendered = MODULE.render_module_boundaries_section(
            [
                {
                    "id": "columncore",
                    "doc_label": "ColumnCore",
                    "owned_targets": ["ColumnCore"],
                    "allowed_include_prefixes": ["column/", "types/", "common/"],
                    "allowed_test_targets": ["column_test"],
                    "remediation": "Move code down or add an interface.",
                }
            ]
        )

        self.assertIn("## Module Harness", rendered)
        self.assertIn("### ColumnCore (`columncore`)", rendered)
        self.assertIn("`ColumnCore`", rendered)
        self.assertIn("`column_test`", rendered)
        self.assertIn("Move code down or add an interface.", rendered)

    def test_replace_generated_section_updates_existing_markers(self) -> None:
        content = "\n".join(
            [
                "# Header",
                MODULE.BEGIN_MARKER,
                "old",
                MODULE.END_MARKER,
                "",
            ]
        )

        updated = MODULE.replace_generated_section(content, "new")
        self.assertIn("new", updated)
        self.assertNotIn("old", updated)

    def test_check_mode_detects_drift(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            (repo / "be").mkdir()
            manifest_path = repo / "be" / "module_boundary_manifest.json"
            agents_path = repo / "be" / "AGENTS.md"

            manifest_path.write_text(
                json.dumps(
                    {
                        "modules": [
                            {
                                "id": "base",
                                "doc_label": "Base",
                                "owned_targets": ["Base"],
                                "allowed_include_prefixes": ["base/", "gutil/", "gen_cpp/"],
                                "allowed_test_targets": ["base_test"],
                                "remediation": "Keep Base standalone.",
                            }
                        ]
                    },
                    indent=2,
                )
                + "\n"
            )
            agents_path.write_text(
                "\n".join(
                    [
                        "# BE",
                        MODULE.BEGIN_MARKER,
                        "stale",
                        MODULE.END_MARKER,
                        "",
                    ]
                )
            )

            with self.assertRaises(MODULE.GeneratedSectionMismatchError):
                manifest = json.loads(manifest_path.read_text())
                MODULE.check_agents_file(agents_path, manifest_path, manifest["modules"])

    def test_agents_paths_include_nested_agents_files(self) -> None:
        manifest = {
            "modules": [
                {
                    "id": "base",
                    "agents_path": "be/AGENTS.md",
                },
                {
                    "id": "connectorbenchmark",
                    "agents_path": "be/src/connector/benchmark/AGENTS.md",
                },
                {
                    "id": "common",
                },
            ]
        }

        self.assertEqual(
            MODULE._agents_paths(manifest, MODULE.DEFAULT_AGENTS),
            [
                "be/AGENTS.md",
                "be/src/connector/benchmark/AGENTS.md",
            ],
        )
        self.assertEqual(MODULE._agents_paths(manifest, "custom/AGENTS.md"), ["custom/AGENTS.md"])

    def test_modules_for_agents_path_uses_default_path(self) -> None:
        manifest = {
            "modules": [
                {
                    "id": "base",
                },
                {
                    "id": "connectorbenchmark",
                    "agents_path": "be/src/connector/benchmark/AGENTS.md",
                },
            ]
        }

        default_modules = MODULE._modules_for_agents_path(manifest, MODULE.DEFAULT_AGENTS)
        nested_modules = MODULE._modules_for_agents_path(manifest, "be/src/connector/benchmark/AGENTS.md")

        self.assertEqual([module["id"] for module in default_modules], ["base"])
        self.assertEqual([module["id"] for module in nested_modules], ["connectorbenchmark"])


if __name__ == "__main__":
    unittest.main()
