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
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parent / "agent_pool.py"
SPEC = importlib.util.spec_from_file_location("agent_pool", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"failed to load {MODULE_PATH}")
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class AgentPoolTest(unittest.TestCase):
    def _run_cli(
        self,
        repo_root: Path,
        *args: str,
        env_overrides: dict[str, str | None] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env.setdefault("BUILD_TYPE", "Release")
        if env_overrides:
            for key, value in env_overrides.items():
                if value is None:
                    env.pop(key, None)
                else:
                    env[key] = value
        return subprocess.run(
            [sys.executable, str(MODULE_PATH), *args],
            cwd=repo_root,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

    def _create_repo(self, root: Path) -> tuple[Path, str]:
        subprocess.run(["git", "init"], cwd=root, check=True, capture_output=True, text=True)
        subprocess.run(["git", "config", "user.name", "Test User"], cwd=root, check=True)
        subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=root, check=True)
        (root / ".gitignore").write_text(".worktrees/\n.agent-pool/\n")
        (root / "tracked.txt").write_text("v1\n")
        subprocess.run(["git", "add", ".gitignore", "tracked.txt"], cwd=root, check=True)
        subprocess.run(["git", "commit", "-m", "init"], cwd=root, check=True, capture_output=True, text=True)
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        return root, head

    def test_bootstrap_and_acquire_release_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root, head = self._create_repo(Path(tmpdir))

            bootstrap = self._run_cli(repo_root, "bootstrap", "--slots", "2", "--base", head, "--skip-warmup")
            self.assertEqual(0, bootstrap.returncode, bootstrap.stderr)
            self.assertTrue((repo_root / ".worktrees" / "agent-pool" / "slot-01").is_dir())
            self.assertTrue((repo_root / ".agent-pool" / "slot-01" / "be").is_dir())

            acquire = self._run_cli(
                repo_root,
                "acquire",
                "--base",
                head,
                "--json",
                env_overrides={"PYTHON": None, "STARROCKS_THIRDPARTY": None},
            )
            self.assertEqual(0, acquire.returncode, acquire.stderr)
            acquired = json.loads(acquire.stdout)
            self.assertEqual("slot-01", acquired["slot"])
            self.assertEqual(str(repo_root.resolve()), acquired["env"]["CCACHE_BASEDIR"])
            self.assertEqual(str((repo_root / ".agent-pool" / "slot-01" / "be").resolve()), acquired["env"]["CMAKE_BUILD_PREFIX"])
            self.assertEqual("python3", acquired["env"]["PYTHON"])
            self.assertEqual(str((repo_root / "thirdparty").resolve()), acquired["env"]["STARROCKS_THIRDPARTY"])

            release = self._run_cli(repo_root, "release", "--slot", "1")
            self.assertEqual(0, release.returncode, release.stderr)
            self.assertFalse((repo_root / ".agent-pool" / "locks" / "slot-01.lock").exists())

    def test_acquire_auto_selects_next_free_slot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root, head = self._create_repo(Path(tmpdir))

            self.assertEqual(
                0,
                self._run_cli(repo_root, "bootstrap", "--slots", "2", "--base", head, "--skip-warmup").returncode,
            )
            first_result = self._run_cli(repo_root, "acquire", "--base", head, "--json")
            second_result = self._run_cli(repo_root, "acquire", "--base", head, "--json")
            self.assertEqual(0, first_result.returncode, first_result.stderr)
            self.assertEqual(0, second_result.returncode, second_result.stderr)
            first = json.loads(first_result.stdout)
            second = json.loads(second_result.stdout)

            self.assertEqual("slot-01", first["slot"])
            self.assertEqual("slot-02", second["slot"])

    def test_reacquire_resets_dirty_slot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root, head = self._create_repo(Path(tmpdir))

            self.assertEqual(
                0,
                self._run_cli(repo_root, "bootstrap", "--slots", "1", "--base", head, "--skip-warmup").returncode,
            )
            acquire_result = self._run_cli(repo_root, "acquire", "--base", head, "--json")
            self.assertEqual(0, acquire_result.returncode, acquire_result.stderr)
            acquired = json.loads(acquire_result.stdout)
            slot_root = Path(acquired["worktree_path"])
            (slot_root / "scratch.txt").write_text("dirty\n")

            forced_release = self._run_cli(repo_root, "release", "--slot", "1", "--force")
            self.assertEqual(0, forced_release.returncode, forced_release.stderr)

            reacquire_result = self._run_cli(repo_root, "acquire", "--slot", "1", "--base", head, "--json")
            self.assertEqual(0, reacquire_result.returncode, reacquire_result.stderr)
            reacquired = json.loads(reacquire_result.stdout)
            self.assertEqual("slot-01", reacquired["slot"])
            self.assertFalse((slot_root / "scratch.txt").exists())

    def test_recycle_removes_state_without_touching_worktree(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root, head = self._create_repo(Path(tmpdir))

            self.assertEqual(
                0,
                self._run_cli(repo_root, "bootstrap", "--slots", "1", "--base", head, "--skip-warmup").returncode,
            )
            slot_state = repo_root / ".agent-pool" / "slot-01" / "be" / "build_Release"
            slot_state.mkdir(parents=True, exist_ok=True)
            marker = slot_state / "marker.txt"
            marker.write_text("cache\n")

            recycle = self._run_cli(repo_root, "recycle", "--slot", "1")
            self.assertEqual(0, recycle.returncode, recycle.stderr)
            self.assertFalse(marker.exists())
            self.assertTrue((repo_root / ".worktrees" / "agent-pool" / "slot-01").exists())

    def test_run_releases_slot_when_command_launch_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root, head = self._create_repo(Path(tmpdir))

            self.assertEqual(
                0,
                self._run_cli(repo_root, "bootstrap", "--slots", "1", "--base", head, "--skip-warmup").returncode,
            )
            run_result = self._run_cli(repo_root, "run", "--base", head, "--", "__agent_pool_missing_command__")
            self.assertNotEqual(0, run_result.returncode)
            self.assertFalse((repo_root / ".agent-pool" / "locks" / "slot-01.lock").exists())

            reacquire = self._run_cli(repo_root, "acquire", "--base", head, "--json")
            self.assertEqual(0, reacquire.returncode, reacquire.stderr)
            reacquired = json.loads(reacquire.stdout)
            self.assertEqual("slot-01", reacquired["slot"])

    def test_acquire_rejects_slot_outside_configured_pool(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root, head = self._create_repo(Path(tmpdir))

            self.assertEqual(
                0,
                self._run_cli(repo_root, "bootstrap", "--slots", "2", "--base", head, "--skip-warmup").returncode,
            )
            acquire = self._run_cli(repo_root, "acquire", "--slot", "3", "--base", head, "--json")
            self.assertNotEqual(0, acquire.returncode)
            self.assertIn("outside the configured pool", acquire.stderr)
            self.assertFalse((repo_root / ".worktrees" / "agent-pool" / "slot-03").exists())
            self.assertFalse((repo_root / ".agent-pool" / "locks" / "slot-03.lock").exists())


if __name__ == "__main__":
    unittest.main()
