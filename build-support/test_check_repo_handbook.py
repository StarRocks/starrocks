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
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parent / "check_repo_handbook.py"
SPEC = importlib.util.spec_from_file_location("check_repo_handbook", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"failed to load {MODULE_PATH}")
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class CheckRepoHandbookTest(unittest.TestCase):
    def test_valid_repo_handbook_passes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)

            self.assertEqual([], MODULE.collect_errors(repo))

    def test_missing_root_agents_link_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)
            (repo / "AGENTS.md").write_text("# AGENTS.md\n")

            errors = MODULE.collect_errors(repo)

            self.assertTrue(any("AGENTS.md" in error and "handbook/index.md" in error for error in errors))

    def test_unindexed_active_plan_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)
            (repo / "handbook" / "plans" / "index.md").write_text(
                textwrap.dedent(
                    """\
                    # Plans

                    ## Active Plans

                    ## Templates

                    - [Execution Plan Template](templates/execution-plan.md)

                    ## Completed Plans

                    - [Completed Plans README](completed/README.md)
                    """
                )
            )

            errors = MODULE.collect_errors(repo)

            self.assertTrue(any("active plan" in error.lower() and "roadmap.md" in error for error in errors))

    def test_missing_plan_metadata_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)
            (repo / "handbook" / "plans" / "active" / "roadmap.md").write_text(
                textwrap.dedent(
                    """\
                    # Harness Engineering Roadmap

                    - Status: active
                    - Owner: Engineering Productivity

                    ## Summary

                    Phase 2 tracks the handbook rollout.
                    """
                )
            )

            errors = MODULE.collect_errors(repo)

            self.assertTrue(any("roadmap.md" in error and "Last Updated" in error for error in errors))
            self.assertTrue(any("roadmap.md" in error and "Acceptance Criteria" in error for error in errors))

    def test_ci_pipeline_tracks_handbook_inputs(self) -> None:
        workflow_text = (Path(__file__).resolve().parent.parent / ".github" / "workflows" / "ci-pipeline.yml").read_text()

        self.assertIn("name: Repo Handbook", workflow_text)
        self.assertIn("- 'handbook/**'", workflow_text)
        self.assertIn("- 'AGENTS.md'", workflow_text)
        self.assertIn("- 'CLAUDE.md'", workflow_text)
        self.assertIn("- 'be/AGENTS.md'", workflow_text)
        self.assertIn("- 'build-support/check_repo_handbook.py'", workflow_text)
        self.assertIn("- 'build-support/test_check_repo_handbook.py'", workflow_text)

    def _write_sample_repo(self, repo: Path) -> None:
        (repo / "be").mkdir()
        (repo / "handbook" / "architecture").mkdir(parents=True)
        (repo / "handbook" / "plans" / "active").mkdir(parents=True)
        (repo / "handbook" / "plans" / "completed").mkdir(parents=True)
        (repo / "handbook" / "plans" / "templates").mkdir(parents=True)

        (repo / "AGENTS.md").write_text(
            textwrap.dedent(
                """\
                # AGENTS.md - StarRocks

                See [handbook/index.md](./handbook/index.md).
                """
            )
        )
        (repo / "CLAUDE.md").write_text(
            textwrap.dedent(
                """\
                # AGENTS.md - StarRocks

                See [handbook/index.md](./handbook/index.md).
                """
            )
        )
        (repo / "be" / "AGENTS.md").write_text(
            textwrap.dedent(
                """\
                # AGENTS.md - StarRocks Backend

                - [Repo handbook](../handbook/index.md)
                - [BE boundary harness](../handbook/architecture/be-boundary-harness.md)
                """
            )
        )
        (repo / "handbook" / "index.md").write_text(
            textwrap.dedent(
                """\
                # Handbook

                - [Architecture](architecture/index.md)
                - [Plans](plans/index.md)
                """
            )
        )
        (repo / "handbook" / "architecture" / "index.md").write_text(
            textwrap.dedent(
                """\
                # Architecture

                - [Repo Topology](repo-topology.md)
                - [BE Boundary Harness](be-boundary-harness.md)
                """
            )
        )
        (repo / "handbook" / "architecture" / "repo-topology.md").write_text("# Repo Topology\n")
        (repo / "handbook" / "architecture" / "be-boundary-harness.md").write_text("# BE Boundary Harness\n")
        (repo / "handbook" / "plans" / "index.md").write_text(
            textwrap.dedent(
                """\
                # Plans

                ## Active Plans

                - [Harness Engineering Roadmap](active/roadmap.md)

                ## Templates

                - [Execution Plan Template](templates/execution-plan.md)

                ## Completed Plans

                - [Completed Plans README](completed/README.md)
                """
            )
        )
        (repo / "handbook" / "plans" / "active" / "roadmap.md").write_text(
            textwrap.dedent(
                """\
                # Harness Engineering Roadmap

                - Status: active
                - Owner: Engineering Productivity
                - Last Updated: 2026-03-27

                ## Summary

                Phase 2 tracks the handbook rollout.

                ## Acceptance Criteria

                - Agents can find architecture and active plans from the root entrypoints.

                ## Decision Log

                - 2026-03-27: Use handbook/ instead of docs/.
                """
            )
        )
        (repo / "handbook" / "plans" / "completed" / "README.md").write_text("# Completed Plans\n")
        (repo / "handbook" / "plans" / "templates" / "execution-plan.md").write_text("# Execution Plan Template\n")


if __name__ == "__main__":
    unittest.main()
