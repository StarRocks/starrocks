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
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).resolve().parent / "check_thirdparty_merge_base.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("check_thirdparty_merge_base", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class CheckThirdpartyMergeBaseTest(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_module()
        self._tmpdir = tempfile.TemporaryDirectory()
        self.repo = Path(self._tmpdir.name)
        self._git("init", "-q")
        self._git("checkout", "-q", "-b", "main")
        self._git("config", "user.email", "ci@example.com")
        self._git("config", "user.name", "ci")
        self._git("config", "commit.gpgsign", "false")
        self._commit("initial", {
            "thirdparty/vars.sh": "v1\n",
            "thirdparty/build-thirdparty-darwin.sh": "d1\n",
            "thirdparty/vars-darwin-aarch64.sh": "a1\n",
            "docker/dockerfiles/dev-env/dev-env.Dockerfile": "FROM base\n",
            "be/src/a.cpp": "a1\n",
        })

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def _git(self, *args: str) -> str:
        return subprocess.run(["git", "-C", str(self.repo), *args], check=True, capture_output=True,
                              text=True).stdout.strip()

    def _commit(self, subject: str, files: dict[str, str]) -> str:
        for name, content in files.items():
            path = self.repo / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
            self._git("add", name)
        self._git("commit", "-q", "-m", subject)
        return self._git("rev-parse", "HEAD")

    def _open_pr(self, name: str, files: dict[str, str]) -> str:
        """Commit a thirdparty change on a new branch `name` forked from the current main tip."""
        self._git("checkout", "-q", "-b", name, "main")
        head = self._commit(name, files)
        self._git("checkout", "-q", "main")
        return head

    def _missing(self, head: str) -> list[str]:
        result = self.module.check(str(self.repo), head, "main")
        return [commit.subject for commit in result.missing]

    def test_pr_based_on_base_tip_passes(self) -> None:
        self._commit("older thirdparty change", {"thirdparty/vars.sh": "v2\n"})
        head = self._open_pr("pr", {"thirdparty/vars.sh": "v3\n"})

        self.assertEqual([], self._missing(head))

    def test_thirdparty_change_merged_after_fork_fails(self) -> None:
        head = self._open_pr("pr", {"thirdparty/vars.sh": "pr\n"})
        missing_sha = self._commit("other thirdparty change", {"thirdparty/patches/x.patch": "x\n"})

        result = self.module.check(str(self.repo), head, "main")

        self.assertEqual([missing_sha], [commit.sha for commit in result.missing])
        self.assertEqual(self._git("rev-parse", "main~1"), result.merge_base)
        self.assertEqual(missing_sha, result.base_tip)

    def test_dev_env_dockerfile_change_counts_as_thirdparty(self) -> None:
        head = self._open_pr("pr", {"thirdparty/vars.sh": "pr\n"})
        self._commit("dev-env change", {"docker/dockerfiles/dev-env/dev-env.Dockerfile": "FROM other\n"})

        self.assertEqual(["dev-env change"], self._missing(head))

    def test_non_thirdparty_and_darwin_only_changes_are_ignored(self) -> None:
        head = self._open_pr("pr", {"thirdparty/vars.sh": "pr\n"})
        self._commit("be change", {"be/src/a.cpp": "a2\n"})
        self._commit("darwin build change", {"thirdparty/build-thirdparty-darwin.sh": "d2\n"})
        self._commit("darwin vars change", {"thirdparty/vars-darwin-aarch64.sh": "a2\n"})

        self.assertEqual([], self._missing(head))

    def test_lists_every_missing_change_newest_first(self) -> None:
        self._commit("change before fork", {"thirdparty/vars.sh": "v2\n"})
        head = self._open_pr("pr", {"thirdparty/vars.sh": "pr\n"})
        self._commit("first missing", {"thirdparty/vars.sh": "v3\n"})
        self._commit("be change", {"be/src/a.cpp": "a2\n"})
        self._commit("second missing", {"thirdparty/vars.sh": "v4\n",
                                        "thirdparty/build-thirdparty-darwin.sh": "d2\n"})

        self.assertEqual(["second missing", "first missing"], self._missing(head))

    def test_merging_base_into_pr_branch_brings_it_up_to_date(self) -> None:
        self._open_pr("pr", {"thirdparty/vars.sh": "pr\n"})
        self._commit("other thirdparty change", {"thirdparty/patches/x.patch": "x\n"})
        self._git("checkout", "-q", "pr")
        self._git("merge", "-q", "--no-edit", "main")
        head = self._git("rev-parse", "HEAD")
        self._git("checkout", "-q", "main")

        self.assertEqual([], self._missing(head))

    def test_main_reports_stale_base_with_annotation(self) -> None:
        head = self._open_pr("pr", {"thirdparty/vars.sh": "pr\n"})
        self._commit("other thirdparty change", {"thirdparty/patches/x.patch": "x\n"})

        out = io.StringIO()
        with mock.patch.dict(os.environ, {"GITHUB_ACTIONS": "true"}), contextlib.redirect_stdout(out):
            code = self.module.main(["--repo", str(self.repo), "--head", head, "--base", "main"])

        self.assertEqual(1, code)
        self.assertIn("::error::This PR changes the thirdparty build", out.getvalue())
        self.assertIn("other thirdparty change", out.getvalue())

    def test_main_passes_up_to_date_pr(self) -> None:
        head = self._open_pr("pr", {"thirdparty/vars.sh": "pr\n"})

        out = io.StringIO()
        with mock.patch.dict(os.environ, {"GITHUB_ACTIONS": ""}), contextlib.redirect_stdout(out):
            code = self.module.main(["--repo", str(self.repo), "--head", head, "--base", "main"])

        self.assertEqual(0, code)
        self.assertIn("OK:", out.getvalue())

    def test_main_rejects_unknown_revision(self) -> None:
        out = io.StringIO()
        with mock.patch.dict(os.environ, {"GITHUB_ACTIONS": ""}), contextlib.redirect_stdout(out):
            code = self.module.main(["--repo", str(self.repo), "--head", "no-such-ref", "--base", "main"])

        self.assertEqual(2, code)
        self.assertIn("ERROR: git rev-parse", out.getvalue())


if __name__ == "__main__":
    unittest.main()
