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

import contextlib
import importlib.util
import io
import sys
import tempfile
import textwrap
import unittest
from datetime import date
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parent / "handbook_plan.py"
SPEC = importlib.util.spec_from_file_location("handbook_plan", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"failed to load {MODULE_PATH}")
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class HandbookPlanTest(unittest.TestCase):
    def test_create_local_plan_initializes_local_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                result = MODULE.main(
                    ["create", "--local", "--title", "Scratch Plan", "--owner", "Engineering Productivity"],
                    repo_root=repo,
                )

            self.assertEqual(0, result)
            created_rel_path = stdout.getvalue().strip()
            created_path = repo / created_rel_path
            self.assertTrue(created_path.exists())
            created_text = created_path.read_text()
            self.assertIn("# Scratch Plan", created_text)
            self.assertIn("- Owner: Engineering Productivity", created_text)
            self.assertIn(f"- Last Updated: {date.today().isoformat()}", created_text)
            self.assertNotIn("- Overrides:", created_text)

            local_index = (repo / "handbook" / "plans" / "local" / "index.md").read_text()
            self.assertIn("[Scratch Plan](", local_index)

    def test_create_local_override_copies_tracked_plan_and_sets_override_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                result = MODULE.main(
                    ["create", "--local", "--from", "handbook/plans/active/../active/roadmap.md"],
                    repo_root=repo,
                )

            self.assertEqual(0, result)
            created_path = repo / stdout.getvalue().strip()
            created_text = created_path.read_text()
            self.assertIn("# Harness Engineering Roadmap", created_text)
            self.assertIn("- Owner: Engineering Productivity", created_text)
            self.assertIn("- Overrides: handbook/plans/active/roadmap.md", created_text)
            self.assertIn("## Summary", created_text)
            self.assertIn("## Decision Log", created_text)

    def test_create_local_override_rejects_duplicate_after_path_normalization(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)

            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(
                    0,
                    MODULE.main(
                        ["create", "--local", "--from", "handbook/plans/active/../active/roadmap.md"],
                        repo_root=repo,
                    ),
                )

            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                result = MODULE.main(
                    ["create", "--local", "--from", "handbook/plans/active/roadmap.md"],
                    repo_root=repo,
                )

            self.assertEqual(1, result)
            self.assertIn("a local override already exists for handbook/plans/active/roadmap.md", stderr.getvalue())

    def test_list_resolves_local_overrides_and_additive_local_plans(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)

            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(
                    0,
                    MODULE.main(
                        ["create", "--local", "--from", "handbook/plans/active/../active/roadmap.md"],
                        repo_root=repo,
                    ),
                )
                self.assertEqual(
                    0,
                    MODULE.main(
                        ["create", "--local", "--title", "Scratch Plan", "--owner", "Engineering Productivity"],
                        repo_root=repo,
                    ),
                )

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                result = MODULE.main(["list"], repo_root=repo)

            self.assertEqual(0, result)
            output = stdout.getvalue()
            self.assertIn("Effective active plans:", output)
            self.assertIn("(local override for handbook/plans/active/roadmap.md)", output)
            self.assertIn("- handbook/plans/active/exec-env.md", output)
            self.assertIn("- handbook/plans/local/active/", output)
            self.assertNotIn("\n- handbook/plans/active/roadmap.md\n", output)

    def test_complete_local_plan_accepts_slug_and_removes_index_entry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                result = MODULE.main(
                    ["create", "--local", "--title", "Scratch Plan", "--owner", "Engineering Productivity"],
                    repo_root=repo,
                )

            self.assertEqual(0, result)
            created_rel_path = stdout.getvalue().strip()
            created_path = repo / created_rel_path
            self.assertTrue(created_path.exists())

            complete_stdout = io.StringIO()
            with contextlib.redirect_stdout(complete_stdout):
                result = MODULE.main(["complete", "--local", "--plan", created_path.stem], repo_root=repo)

            self.assertEqual(0, result)
            self.assertEqual(created_rel_path, complete_stdout.getvalue().strip())
            self.assertFalse(created_path.exists())
            local_index = (repo / "handbook" / "plans" / "local" / "index.md").read_text()
            self.assertNotIn("[Scratch Plan](", local_index)

    def test_complete_local_plan_rejects_tracked_path_without_deleting_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)
            tracked_path = repo / "handbook" / "plans" / "active" / "roadmap.md"
            original_text = tracked_path.read_text()

            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(
                    0,
                    MODULE.main(
                        ["create", "--local", "--title", "Scratch Plan", "--owner", "Engineering Productivity"],
                        repo_root=repo,
                    ),
                )

            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                result = MODULE.main(
                    ["complete", "--local", "--plan", "handbook/plans/active/roadmap.md"],
                    repo_root=repo,
                )

            self.assertEqual(1, result)
            self.assertTrue(tracked_path.exists())
            self.assertEqual(original_text, tracked_path.read_text())
            self.assertIn("no local handbook plan matched 'handbook/plans/active/roadmap.md'", stderr.getvalue())

    def test_complete_local_plan_unlinks_symlinked_local_entry_without_touching_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)
            tracked_path = repo / "handbook" / "plans" / "active" / "roadmap.md"
            local_active_dir = repo / "handbook" / "plans" / "local" / "active"
            local_active_dir.mkdir(parents=True)
            symlink_path = local_active_dir / "evil.md"

            try:
                symlink_path.symlink_to(Path("../../active/roadmap.md"))
            except OSError as err:
                self.skipTest(f"symlink creation failed: {err}")

            self.assertTrue(symlink_path.is_symlink())
            self.assertTrue(symlink_path.exists())

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                result = MODULE.main(["complete", "--local", "--plan", "evil"], repo_root=repo)

            self.assertEqual(0, result)
            self.assertEqual("handbook/plans/local/active/evil.md", stdout.getvalue().strip())
            self.assertFalse(symlink_path.exists())
            self.assertFalse(symlink_path.is_symlink())
            self.assertTrue(tracked_path.exists())
            self.assertEqual(
                tracked_path.read_text(),
                textwrap.dedent(
                    """\
                    # Harness Engineering Roadmap

                    - Status: active
                    - Owner: Engineering Productivity
                    - Last Updated: 2026-03-27

                    ## Summary

                    Track the staged conversion toward a harness-engineering workflow.

                    ## Acceptance Criteria

                    - Keep handbook structure validated mechanically.

                    ## Decision Log

                    - 2026-03-27: Use handbook/ for execution plans.
                    """
                ),
            )

    def test_complete_local_plan_rejects_traversal_ref_without_deleting_tracked_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._write_sample_repo(repo)
            tracked_path = repo / "handbook" / "plans" / "active" / "roadmap.md"
            original_text = tracked_path.read_text()
            local_entry = repo / "handbook" / "plans" / "local" / "active" / "evil.md"
            local_entry.parent.mkdir(parents=True)
            local_entry.write_text(
                textwrap.dedent(
                    """\
                    # Evil Local Plan

                    - Status: active
                    - Owner: Security
                    - Last Updated: 2026-04-07

                    ## Summary

                    Keep this local.
                    """
                )
            )

            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                result = MODULE.main(
                    ["complete", "--local", "--plan", "../../active/roadmap.md"],
                    repo_root=repo,
                )

            self.assertEqual(1, result)
            self.assertTrue(tracked_path.exists())
            self.assertEqual(original_text, tracked_path.read_text())
            self.assertTrue(local_entry.exists())
            self.assertIn("no local handbook plan matched '../../active/roadmap.md'", stderr.getvalue())

    def _write_sample_repo(self, repo: Path) -> None:
        (repo / "handbook" / "plans" / "active").mkdir(parents=True)
        (repo / "handbook" / "plans" / "templates").mkdir(parents=True)
        (repo / "handbook" / "plans" / "index.md").write_text(
            textwrap.dedent(
                """\
                # Handbook Plans

                ## Active Plans

                - [Harness Engineering Roadmap](active/roadmap.md)
                - [ExecEnv Decomposition Multi-PR Rollout](active/exec-env.md)

                ## Templates

                - [Execution Plan Template](templates/execution-plan.md)

                ## Completed Plans

                - [Completed Plans README](completed/README.md)
                """
            )
        )
        (repo / "handbook" / "plans" / "templates" / "execution-plan.md").write_text(
            textwrap.dedent(
                """\
                # Execution Plan Template

                - Status: active
                - Owner: <team-or-person>
                - Last Updated: YYYY-MM-DD

                ## Summary

                Describe the goal and scope.

                ## Acceptance Criteria

                - Define the checks that prove the work is complete.

                ## Decision Log

                - YYYY-MM-DD: Record important scope and sequencing decisions here.
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

                Track the staged conversion toward a harness-engineering workflow.

                ## Acceptance Criteria

                - Keep handbook structure validated mechanically.

                ## Decision Log

                - 2026-03-27: Use handbook/ for execution plans.
                """
            )
        )
        (repo / "handbook" / "plans" / "active" / "exec-env.md").write_text(
            textwrap.dedent(
                """\
                # ExecEnv Decomposition Multi-PR Rollout

                - Status: active
                - Owner: Backend Runtime
                - Last Updated: 2026-04-04

                ## Summary

                Decompose ExecEnv across behavior-preserving PRs.

                ## Acceptance Criteria

                - Tighten the exec_env guardrail.

                ## Decision Log

                - 2026-04-04: Use an incremental rollout.
                """
            )
        )


if __name__ == "__main__":
    unittest.main()
