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

"""Reject unresolved git conflict markers added by a pull request.

Mergify commits unresolved conflict markers into backport PRs when a cherry-pick
fails, and the build/UT jobs do not parse files outside their modules, so such a
PR can pass CI. This checker scans the added lines of the PR diff, fetched from
the GitHub API, so it never needs to check out or run the PR code (it is invoked
from a `pull_request_target` workflow).

Only `<<<<<<< ` and `>>>>>>> ` lines are reported: a bare `=======` line is also
a valid setext heading underline in Markdown/reStructuredText.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


MARKER_RE = re.compile(r"^(<<<<<<<|>>>>>>>)( |$)")
HUNK_RE = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@")


@dataclass(frozen=True)
class Marker:
    path: str
    line: int
    text: str


@dataclass(frozen=True)
class CheckResult:
    markers: list[Marker]
    unchecked_paths: list[str]


class FetchError(Exception):
    pass


def find_markers_in_patch(path: str, patch: str) -> list[Marker]:
    """Return the conflict markers on the added lines of a unified diff patch.

    Line numbers refer to the new version of the file.
    """
    markers = []
    line = None
    for raw_line in patch.splitlines():
        hunk = HUNK_RE.match(raw_line)
        if hunk:
            line = int(hunk.group(1))
            continue
        if line is None:
            # Not inside a hunk yet, e.g. diff headers.
            continue
        if raw_line.startswith("-") or raw_line.startswith("\\"):
            # Removed lines and "\ No newline at end of file" do not exist in the new file.
            continue
        if raw_line.startswith("+"):
            content = raw_line[1:]
            if MARKER_RE.match(content):
                markers.append(Marker(path=path, line=line, text=content))
        line += 1
    return markers


def check_files(files: Iterable[dict]) -> CheckResult:
    """Check the file entries returned by the GitHub "list pull request files" API."""
    markers = []
    unchecked_paths = []
    for entry in files:
        path = entry["filename"]
        patch = entry.get("patch")
        if patch is None:
            # GitHub omits the patch of binary files and of diffs that are too large.
            # Only the latter can hide conflict markers.
            if entry.get("changes", 0) > 0 and entry.get("status") != "removed":
                unchecked_paths.append(path)
            continue
        markers.extend(find_markers_in_patch(path, patch))
    return CheckResult(markers=markers, unchecked_paths=unchecked_paths)


def fetch_pr_files(repo: str, pr_number: int) -> list[dict]:
    command = [
        "gh",
        "api",
        "--paginate",
        f"repos/{repo}/pulls/{pr_number}/files?per_page=100",
        "--jq",
        ".[] | {filename, status, changes, patch} | @json",
    ]
    try:
        result = subprocess.run(command, check=False, capture_output=True, text=True)
    except OSError as error:
        raise FetchError(f"failed to run gh: {error}") from error
    if result.returncode != 0:
        raise FetchError(f"`{' '.join(command)}` exited with {result.returncode}: {result.stderr.strip()}")

    files = []
    for output_line in result.stdout.splitlines():
        if not output_line.strip():
            continue
        try:
            files.append(json.loads(output_line))
        except json.JSONDecodeError as error:
            raise FetchError(f"invalid gh api output line {output_line!r}: {error}") from error
    return files


def load_files_json(path: Path) -> list[dict]:
    try:
        files = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise FetchError(f"failed to read {path}: {error}") from error
    if not isinstance(files, list):
        raise FetchError(f"{path} must contain a JSON array of pull request file entries")
    return files


def _print_result(result: CheckResult) -> None:
    for path in result.unchecked_paths:
        print(f"::warning file={path}::Diff is too large to check for conflict markers, please check it manually")
    for marker in result.markers:
        print(f"::error file={marker.path},line={marker.line}::Unresolved conflict marker: {marker.text}")
    if result.markers:
        print(
            f"Found {len(result.markers)} unresolved conflict marker(s). "
            "Resolve the conflicts and push the fix before merging."
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Reject unresolved git conflict markers added by a pull request.")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--pr", type=int, help="Pull request number, fetched with `gh api` (needs GH_TOKEN)")
    source.add_argument(
        "--files-json",
        type=Path,
        help='Local JSON array in the format of the GitHub "list pull request files" API',
    )
    parser.add_argument("--repo", default="StarRocks/starrocks", help="GitHub repository, used with --pr")
    args = parser.parse_args(argv)

    try:
        files = fetch_pr_files(args.repo, args.pr) if args.pr is not None else load_files_json(args.files_json)
        result = check_files(files)
    except (FetchError, KeyError, TypeError) as error:
        print(f"::error::Failed to check conflict markers: {error}")
        return 2

    _print_result(result)
    return 1 if result.markers else 0


if __name__ == "__main__":
    sys.exit(main())
