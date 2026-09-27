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
import json
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).resolve().parent / "check_conflict_markers.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("check_conflict_markers", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


check_conflict_markers = _load_module()


# The java-extensions/pom.xml patch of #79804, which was merged with conflict markers.
CONFLICT_PATCH = textwrap.dedent(
    """\
    @@ -61,8 +61,13 @@
             <luben.zstd.jni.version>1.5.4-2</luben.zstd.jni.version>
             <kryo.version>4.0.2</kryo.version>
             <commons-beanutils.version>1.11.0</commons-beanutils.version>
    +<<<<<<< HEAD
             <lz4-java.version>1.11.1</lz4-java.version>
             <zookeeper.version>3.8.6</zookeeper.version>
    +=======
    +        <lz4-java.version>1.10.1</lz4-java.version>
    +        <zookeeper.version>3.8.7</zookeeper.version>
    +>>>>>>> dae578a ([BugFix][CVE] bump ZooKeeper to 3.8.7 and 3.9.6 (backport #79707) (#79717))
             <httpclient5.version>5.6.4</httpclient5.version>
             <httpcore5.version>5.4.3</httpcore5.version>
             <bouncycastle.version>1.85</bouncycastle.version>"""
)

# The fix of #79826, which only removes the conflict markers.
RESOLVED_PATCH = textwrap.dedent(
    """\
    @@ -61,13 +61,8 @@
             <luben.zstd.jni.version>1.5.4-2</luben.zstd.jni.version>
             <kryo.version>4.0.2</kryo.version>
             <commons-beanutils.version>1.11.0</commons-beanutils.version>
    -<<<<<<< HEAD
             <lz4-java.version>1.11.1</lz4-java.version>
    -        <zookeeper.version>3.8.6</zookeeper.version>
    -=======
    -        <lz4-java.version>1.10.1</lz4-java.version>
             <zookeeper.version>3.8.7</zookeeper.version>
    ->>>>>>> dae578a ([BugFix][CVE] bump ZooKeeper to 3.8.7 and 3.9.6 (backport #79707) (#79717))
             <httpclient5.version>5.6.4</httpclient5.version>
             <httpcore5.version>5.4.3</httpcore5.version>
             <bouncycastle.version>1.85</bouncycastle.version>"""
)


def _entry(filename: str, patch: str | None, changes: int = 1, status: str = "modified") -> dict:
    entry = {"filename": filename, "status": status, "changes": changes}
    if patch is not None:
        entry["patch"] = patch
    return entry


def _lines(markers) -> list[tuple[str, int, str]]:
    return [(marker.path, marker.line, marker.text) for marker in markers]


class MarkerDetectionTest(unittest.TestCase):
    def test_reports_markers_on_added_lines(self) -> None:
        markers = check_conflict_markers.find_markers_in_patch("java-extensions/pom.xml", CONFLICT_PATCH)
        self.assertEqual(
            _lines(markers),
            [
                ("java-extensions/pom.xml", 64, "<<<<<<< HEAD"),
                (
                    "java-extensions/pom.xml",
                    70,
                    ">>>>>>> dae578a ([BugFix][CVE] bump ZooKeeper to 3.8.7 and 3.9.6 (backport #79707) (#79717))",
                ),
            ],
        )

    def test_ignores_markers_on_removed_lines(self) -> None:
        self.assertEqual(check_conflict_markers.find_markers_in_patch("java-extensions/pom.xml", RESOLVED_PATCH), [])

    def test_ignores_markers_on_context_lines(self) -> None:
        patch = "@@ -1,2 +1,3 @@\n <<<<<<< HEAD\n+added\n >>>>>>> branch"
        self.assertEqual(check_conflict_markers.find_markers_in_patch("a.txt", patch), [])

    def test_reports_bare_markers(self) -> None:
        patch = "@@ -0,0 +1,2 @@\n+<<<<<<<\n+>>>>>>>"
        self.assertEqual(
            _lines(check_conflict_markers.find_markers_in_patch("a.txt", patch)),
            [("a.txt", 1, "<<<<<<<"), ("a.txt", 2, ">>>>>>>")],
        )

    def test_ignores_non_marker_lines(self) -> None:
        patch = textwrap.dedent(
            """\
            @@ -0,0 +1,7 @@
            +Title
            +=======
            +<<<<<<<< eight chars
            +>>>>>>>> eight chars
            +<<<<<<<HEAD
            + <<<<<<< indented
            +a <<<<<<< in the middle"""
        )
        self.assertEqual(check_conflict_markers.find_markers_in_patch("docs/a.md", patch), [])


class HunkLineAccountingTest(unittest.TestCase):
    def test_counts_lines_across_hunks(self) -> None:
        patch = textwrap.dedent(
            """\
            @@ -1,3 +1,4 @@
             line 1
            -old line 2
            +new line 2
            +<<<<<<< HEAD
             line 3
            @@ -40 +41,2 @@
             line 41
            +>>>>>>> theirs"""
        )
        self.assertEqual(
            _lines(check_conflict_markers.find_markers_in_patch("a.txt", patch)),
            [("a.txt", 3, "<<<<<<< HEAD"), ("a.txt", 42, ">>>>>>> theirs")],
        )

    def test_skips_no_newline_at_end_of_file(self) -> None:
        patch = textwrap.dedent(
            """\
            @@ -1,2 +1,3 @@
             line 1
            -line 2
            \\ No newline at end of file
            +line 2
            +<<<<<<< HEAD
            \\ No newline at end of file"""
        )
        self.assertEqual(
            _lines(check_conflict_markers.find_markers_in_patch("a.txt", patch)),
            [("a.txt", 3, "<<<<<<< HEAD")],
        )

    def test_counts_empty_context_lines(self) -> None:
        patch = "@@ -1,3 +1,4 @@\n line 1\n\n line 3\n+<<<<<<< HEAD"
        self.assertEqual(
            _lines(check_conflict_markers.find_markers_in_patch("a.txt", patch)),
            [("a.txt", 4, "<<<<<<< HEAD")],
        )

    def test_new_file_hunk(self) -> None:
        patch = "@@ -0,0 +1,3 @@\n+line 1\n+line 2\n+>>>>>>> theirs"
        self.assertEqual(
            _lines(check_conflict_markers.find_markers_in_patch("new.txt", patch)),
            [("new.txt", 3, ">>>>>>> theirs")],
        )


class OmittedPatchTest(unittest.TestCase):
    def test_warns_for_large_diff_without_patch(self) -> None:
        result = check_conflict_markers.check_files([_entry("big.sql", None, changes=50000)])
        self.assertEqual(result.markers, [])
        self.assertEqual(result.unchecked_paths, ["big.sql"])

    def test_skips_binary_rename_and_removed_files(self) -> None:
        result = check_conflict_markers.check_files(
            [
                _entry("image.png", None, changes=0),
                _entry("renamed.txt", None, changes=0, status="renamed"),
                _entry("removed.sql", None, changes=50000, status="removed"),
            ]
        )
        self.assertEqual(result.markers, [])
        self.assertEqual(result.unchecked_paths, [])

    def test_checks_the_remaining_files(self) -> None:
        result = check_conflict_markers.check_files(
            [
                _entry("big.sql", None, changes=50000),
                _entry("java-extensions/pom.xml", CONFLICT_PATCH),
                _entry("java-extensions/pom.xml.bak", RESOLVED_PATCH),
            ]
        )
        self.assertEqual([marker.line for marker in result.markers], [64, 70])
        self.assertEqual(result.unchecked_paths, ["big.sql"])


class MainTest(unittest.TestCase):
    def _run_main(self, argv: list[str]) -> tuple[int, str]:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            code = check_conflict_markers.main(argv)
        return code, stdout.getvalue()

    def _run_main_with_files(self, files) -> tuple[int, str]:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "files.json"
            path.write_text(json.dumps(files))
            return self._run_main(["--files-json", str(path)])

    def test_fails_and_annotates_markers(self) -> None:
        code, output = self._run_main_with_files([_entry("java-extensions/pom.xml", CONFLICT_PATCH)])
        self.assertEqual(code, 1)
        self.assertIn("::error file=java-extensions/pom.xml,line=64::Unresolved conflict marker: <<<<<<< HEAD", output)
        self.assertIn("::error file=java-extensions/pom.xml,line=70::", output)

    def test_passes_without_markers(self) -> None:
        code, output = self._run_main_with_files([_entry("java-extensions/pom.xml", RESOLVED_PATCH)])
        self.assertEqual(code, 0)
        self.assertEqual(output, "")

    def test_omitted_patch_only_warns(self) -> None:
        code, output = self._run_main_with_files([_entry("big.sql", None, changes=50000)])
        self.assertEqual(code, 0)
        self.assertIn("::warning file=big.sql::", output)

    def test_fetches_pr_files_with_gh(self) -> None:
        stdout = "\n".join(
            [
                json.dumps(_entry("java-extensions/pom.xml", CONFLICT_PATCH)),
                json.dumps(_entry("image.png", None, changes=0)),
                "",
            ]
        )
        completed = subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")
        with mock.patch.object(check_conflict_markers.subprocess, "run", return_value=completed) as run:
            code, output = self._run_main(["--repo", "StarRocks/starrocks", "--pr", "79804"])
        self.assertEqual(code, 1)
        self.assertIn("line=64", output)
        command = run.call_args.args[0]
        self.assertEqual(command[:3], ["gh", "api", "--paginate"])
        self.assertIn("repos/StarRocks/starrocks/pulls/79804/files?per_page=100", command)

    def test_fails_when_gh_fails(self) -> None:
        completed = subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="HTTP 404: Not Found")
        with mock.patch.object(check_conflict_markers.subprocess, "run", return_value=completed):
            code, output = self._run_main(["--pr", "1"])
        self.assertEqual(code, 2)
        self.assertIn("::error::Failed to check conflict markers:", output)
        self.assertIn("HTTP 404: Not Found", output)

    def test_fails_when_gh_is_missing(self) -> None:
        with mock.patch.object(check_conflict_markers.subprocess, "run", side_effect=FileNotFoundError("gh")):
            code, output = self._run_main(["--pr", "1"])
        self.assertEqual(code, 2)
        self.assertIn("failed to run gh", output)

    def test_fails_on_invalid_gh_output(self) -> None:
        completed = subprocess.CompletedProcess(args=[], returncode=0, stdout="not json\n", stderr="")
        with mock.patch.object(check_conflict_markers.subprocess, "run", return_value=completed):
            code, output = self._run_main(["--pr", "1"])
        self.assertEqual(code, 2)
        self.assertIn("invalid gh api output line", output)

    def test_fails_on_malformed_file_entry(self) -> None:
        code, output = self._run_main_with_files([{"patch": "@@ -0,0 +1 @@\n+a"}])
        self.assertEqual(code, 2)
        self.assertIn("::error::Failed to check conflict markers:", output)

    def test_fails_on_unreadable_files_json(self) -> None:
        code, output = self._run_main(["--files-json", "/nonexistent/files.json"])
        self.assertEqual(code, 2)
        self.assertIn("failed to read", output)

    def test_rejects_non_array_files_json(self) -> None:
        code, output = self._run_main_with_files({"filename": "a.txt"})
        self.assertEqual(code, 2)
        self.assertIn("must contain a JSON array", output)


if __name__ == "__main__":
    unittest.main()
