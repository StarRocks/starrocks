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

    def test_unindexed_domain_page_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)
            (repo / "handbook" / "domains" / "index.md").write_text(
                textwrap.dedent(
                    """\
                    # Handbook Domains

                    ## Domains

                    - [Backend](backend.md)
                    - [Frontend](frontend.md)
                    - [SQL Integration](sql-integration.md)
                    - [Docs and Translation](docs-and-translation.md)
                    - [CI and Tooling](ci-and-tooling.md)
                    """
                )
            )

            errors = MODULE.collect_errors(repo)

            self.assertTrue(
                any(
                    "generated-and-extensions.md" in error and "Unindexed domain page" in error
                    for error in errors
                )
            )

    def test_missing_domain_heading_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)
            (repo / "handbook" / "domains" / "frontend.md").write_text(
                textwrap.dedent(
                    """\
                    # Frontend Domain

                    ## Purpose

                    FE ownership.

                    ## Entrypoints

                    - `fe/`

                    ## Commands

                    - `./build.sh --fe`

                    ## Guardrails

                    - Keep FE SPI boundaries.

                    ## Open Gaps

                    - No FE structural harness yet.
                    """
                )
            )

            errors = MODULE.collect_errors(repo)

            self.assertTrue(
                any(
                    "frontend.md" in error and "## Test and Validation" in error
                    for error in errors
                )
            )

    def test_missing_nested_agents_domain_link_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)
            (repo / "docs" / "AGENTS.md").write_text(
                textwrap.dedent(
                    """\
                    # AGENTS.md - StarRocks Documentation

                    - [Repo handbook](../handbook/index.md)
                    """
                )
            )

            errors = MODULE.collect_errors(repo)

            self.assertTrue(
                any(
                    "docs/AGENTS.md" in error and "handbook/domains/docs-and-translation.md" in error
                    for error in errors
                )
            )

    def test_ci_pipeline_tracks_handbook_inputs(self) -> None:
        workflow_text = (Path(__file__).resolve().parent.parent / ".github" / "workflows" / "ci-pipeline.yml").read_text()

        self.assertIn("name: Repo Handbook", workflow_text)
        self.assertIn("- 'handbook/**'", workflow_text)
        self.assertIn("- 'AGENTS.md'", workflow_text)
        self.assertIn("- 'CLAUDE.md'", workflow_text)
        self.assertIn("- 'be/AGENTS.md'", workflow_text)
        self.assertIn("- 'be/src/common/AGENTS.md'", workflow_text)
        self.assertIn("- 'docs/AGENTS.md'", workflow_text)
        self.assertIn("- 'fe/AGENTS.md'", workflow_text)
        self.assertIn("- 'gensrc/AGENTS.md'", workflow_text)
        self.assertIn("- 'java-extensions/AGENTS.md'", workflow_text)
        self.assertIn("- 'test/AGENTS.md'", workflow_text)
        self.assertIn("- 'build-support/check_repo_handbook.py'", workflow_text)
        self.assertIn("- 'build-support/test_check_repo_handbook.py'", workflow_text)

    def test_ci_pipeline_branch_tracks_handbook_inputs(self) -> None:
        workflow_text = (
            Path(__file__).resolve().parent.parent / ".github" / "workflows" / "ci-pipeline-branch.yml"
        ).read_text()

        self.assertIn("name: Repo Handbook", workflow_text)
        self.assertIn("- 'handbook/**'", workflow_text)
        self.assertIn("- 'AGENTS.md'", workflow_text)
        self.assertIn("- 'CLAUDE.md'", workflow_text)
        self.assertIn("- 'be/AGENTS.md'", workflow_text)
        self.assertIn("- 'be/src/common/AGENTS.md'", workflow_text)
        self.assertIn("- 'docs/AGENTS.md'", workflow_text)
        self.assertIn("- 'fe/AGENTS.md'", workflow_text)
        self.assertIn("- 'gensrc/AGENTS.md'", workflow_text)
        self.assertIn("- 'java-extensions/AGENTS.md'", workflow_text)
        self.assertIn("- 'test/AGENTS.md'", workflow_text)
        self.assertIn("- 'build-support/check_repo_handbook.py'", workflow_text)
        self.assertIn("- 'build-support/test_check_repo_handbook.py'", workflow_text)

    def _write_sample_repo(self, repo: Path) -> None:
        (repo / "be" / "src" / "common").mkdir(parents=True)
        (repo / "docs").mkdir()
        (repo / "fe").mkdir()
        (repo / "gensrc").mkdir()
        (repo / "handbook" / "architecture").mkdir(parents=True)
        (repo / "handbook" / "domains").mkdir(parents=True)
        (repo / "handbook" / "plans" / "active").mkdir(parents=True)
        (repo / "handbook" / "plans" / "completed").mkdir(parents=True)
        (repo / "handbook" / "plans" / "templates").mkdir(parents=True)
        (repo / "handbook" / "policies").mkdir(parents=True)
        (repo / "handbook" / "quality").mkdir(parents=True)
        (repo / "handbook" / "templates").mkdir(parents=True)
        (repo / "java-extensions").mkdir()
        (repo / "test").mkdir()

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
                - [Backend domain map](../handbook/domains/backend.md)
                - [BE boundary harness](../handbook/architecture/be-boundary-harness.md)
                """
            )
        )
        (repo / "be" / "src" / "common" / "AGENTS.md").write_text(
            textwrap.dedent(
                """\
                # AGENTS.md - be/src/common

                - [Repo handbook](../../../handbook/index.md)
                - [Backend domain map](../../../handbook/domains/backend.md)
                - [BE harness](../../AGENTS.md)
                """
            )
        )
        (repo / "docs" / "AGENTS.md").write_text(
            textwrap.dedent(
                """\
                # AGENTS.md - StarRocks Documentation

                - [Repo handbook](../handbook/index.md)
                - [Docs and translation domain](../handbook/domains/docs-and-translation.md)
                """
            )
        )
        (repo / "fe" / "AGENTS.md").write_text(
            textwrap.dedent(
                """\
                # AGENTS.md - StarRocks Frontend

                - [Repo handbook](../handbook/index.md)
                - [Frontend domain map](../handbook/domains/frontend.md)
                """
            )
        )
        (repo / "gensrc" / "AGENTS.md").write_text(
            textwrap.dedent(
                """\
                # AGENTS.md - StarRocks Generated Source Code

                - [Repo handbook](../handbook/index.md)
                - [Generated and extensions domain](../handbook/domains/generated-and-extensions.md)
                """
            )
        )
        (repo / "java-extensions" / "AGENTS.md").write_text(
            textwrap.dedent(
                """\
                # AGENTS.md - StarRocks Java Extensions

                - [Repo handbook](../handbook/index.md)
                - [Generated and extensions domain](../handbook/domains/generated-and-extensions.md)
                """
            )
        )
        (repo / "test" / "AGENTS.md").write_text(
            textwrap.dedent(
                """\
                # AGENTS.md - StarRocks SQL Integration Tests

                - [Repo handbook](../handbook/index.md)
                - [SQL integration domain](../handbook/domains/sql-integration.md)
                """
            )
        )
        (repo / "handbook" / "index.md").write_text(
            textwrap.dedent(
                """\
                # Handbook

                - [Architecture](architecture/index.md)
                - [Domains](domains/index.md)
                - [Policies](policies/index.md)
                - [Quality](quality/index.md)
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
        (repo / "handbook" / "domains" / "index.md").write_text(
            textwrap.dedent(
                """\
                # Handbook Domains

                ## Domains

                - [Backend](backend.md)
                - [Frontend](frontend.md)
                - [SQL Integration](sql-integration.md)
                - [Docs and Translation](docs-and-translation.md)
                - [CI and Tooling](ci-and-tooling.md)
                - [Generated and Extensions](generated-and-extensions.md)
                """
            )
        )
        for path, title in (
            ("backend.md", "Backend Domain"),
            ("frontend.md", "Frontend Domain"),
            ("sql-integration.md", "SQL Integration Domain"),
            ("docs-and-translation.md", "Docs and Translation Domain"),
            ("ci-and-tooling.md", "CI and Tooling Domain"),
            ("generated-and-extensions.md", "Generated and Extensions Domain"),
        ):
            (repo / "handbook" / "domains" / path).write_text(
                textwrap.dedent(
                    f"""\
                    # {title}

                    ## Purpose

                    Describe the domain scope.

                    ## Entrypoints

                    - `sample/path`

                    ## Commands

                    - `sample-command`

                    ## Guardrails

                    - Follow domain rules.

                    ## Test and Validation

                    - Run focused checks.

                    ## Open Gaps

                    - Add more harnessing.
                    """
                )
            )
        (repo / "handbook" / "policies" / "index.md").write_text(
            textwrap.dedent(
                """\
                # Handbook Policies

                ## Policies

                - [Reliability First](reliability-first.md)
                - [Internal vs Public Docs](internal-vs-public-docs.md)
                - [Agent Change Flow](agent-change-flow.md)
                """
            )
        )
        for path, title in (
            ("reliability-first.md", "Reliability First"),
            ("internal-vs-public-docs.md", "Internal vs Public Docs"),
            ("agent-change-flow.md", "Agent Change Flow"),
        ):
            (repo / "handbook" / "policies" / path).write_text(
                textwrap.dedent(
                    f"""\
                    # {title}

                    ## Intent

                    State the policy intent.

                    ## Applies To

                    - Core maintainers

                    ## Enforcement

                    - Mechanical checks

                    ## Exceptions

                    - Document exceptions explicitly.
                    """
                )
            )
        (repo / "handbook" / "quality" / "index.md").write_text(
            textwrap.dedent(
                """\
                # Handbook Quality

                ## Reports

                - [Scorecard](scorecard.md)
                """
            )
        )
        (repo / "handbook" / "quality" / "scorecard.md").write_text(
            textwrap.dedent(
                """\
                # Handbook Quality Scorecard

                ## Rating Scale

                - `seed`
                - `structured`

                ## Domain Scores

                - Backend: structured
                - Frontend: seed
                """
            )
        )
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
        (repo / "handbook" / "templates" / "domain-map.md").write_text("# Domain Map Template\n")
        (repo / "handbook" / "templates" / "policy.md").write_text("# Policy Template\n")


if __name__ == "__main__":
    unittest.main()
