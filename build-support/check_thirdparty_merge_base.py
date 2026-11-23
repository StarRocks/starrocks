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

"""Check that a PR changing the thirdparty build is based on the latest thirdparty change.

PR CI builds a dev-env image for a PR that changes the Linux thirdparty build, and the
merge pipeline (.github/workflows/ci-merged.yml) promotes that image to the base branch's
CI image once the PR is merged. Such PRs must therefore land in order: when one is merged,
another one that was not rebased onto it would later promote an image that lacks its
change.

This checker passes only when the merge base of the PR head and the base branch already
contains every thirdparty change of the base branch, that is, when no commit between the
merge base and the base branch tip touches the thirdparty paths below.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass


# Keep aligned with the `linux-thirdparty-filter` path filters in
# .github/workflows/ci-pipeline.yml, ci-pipeline-branch.yml and ci-merged.yml: these are the
# changes that make PR CI build a thirdparty image and the merge pipeline promote it.
THIRDPARTY_PATHSPECS = (
    ":(top)thirdparty",
    ":(top,exclude)thirdparty/build-thirdparty-darwin.sh",
    ":(top,exclude)thirdparty/vars-darwin-aarch64.sh",
    ":(top)docker/dockerfiles/dev-env/dev-env.Dockerfile",
)


class CheckError(Exception):
    pass


@dataclass(frozen=True)
class Commit:
    sha: str
    subject: str


@dataclass(frozen=True)
class Result:
    merge_base: str
    base_tip: str
    missing: tuple[Commit, ...]


def run_git(repo: str, *args: str) -> str:
    proc = subprocess.run(["git", "-C", repo, *args], capture_output=True, text=True)
    if proc.returncode != 0:
        raise CheckError("git {} failed: {}".format(" ".join(args), proc.stderr.strip()))
    return proc.stdout


def resolve_commit(repo: str, rev: str) -> str:
    return run_git(repo, "rev-parse", "--verify", rev + "^{commit}").strip()


def check(repo: str, head: str, base: str) -> Result:
    head_sha = resolve_commit(repo, head)
    base_sha = resolve_commit(repo, base)
    merge_base = run_git(repo, "merge-base", head_sha, base_sha).strip()
    if not merge_base:
        raise CheckError("{} and {} have no merge base".format(head, base))
    log = run_git(repo, "log", "--format=%H%x09%s", "{}..{}".format(merge_base, base_sha), "--",
                  *THIRDPARTY_PATHSPECS)
    missing = []
    for line in log.splitlines():
        sha, _, subject = line.partition("\t")
        missing.append(Commit(sha, subject))
    return Result(merge_base, base_sha, tuple(missing))


def report(result: Result, head: str, base: str, annotate: bool) -> int:
    print("PR head:    {}".format(head))
    print("Base:       {} ({})".format(base, result.base_tip))
    print("Merge base: {}".format(result.merge_base))
    if not result.missing:
        print("OK: the merge base contains every thirdparty change of {}.".format(base))
        return 0

    summary = ("This PR changes the thirdparty build, but its merge base is missing {} thirdparty "
               "change(s) already merged into {}. Rebase this PR onto (or merge) the latest {} and "
               "push again.").format(len(result.missing), base, base)
    print("{}{}".format("::error::" if annotate else "ERROR: ", summary))
    print("Thirdparty changes missing from this PR's merge base:")
    for commit in result.missing:
        print("  {} {}".format(commit.sha[:12], commit.subject))
    print("PR CI builds this PR's thirdparty image and the merge pipeline promotes it to the CI image "
          "of {}, so an image built without these changes would drop them.".format(base))
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fail when a PR that changes the thirdparty build is not based on every thirdparty "
        "change of its base branch.")
    parser.add_argument("--head", required=True, help="PR head commit (not the CI test-merge commit)")
    parser.add_argument("--base", required=True, help="base branch ref, e.g. origin/main")
    parser.add_argument("--repo", default=".", help="git repository path (default: current directory)")
    args = parser.parse_args(argv)

    annotate = os.environ.get("GITHUB_ACTIONS") == "true"
    try:
        result = check(args.repo, args.head, args.base)
    except CheckError as error:
        print("{}{}".format("::error::" if annotate else "ERROR: ", error))
        return 2
    return report(result, args.head, args.base, annotate)


if __name__ == "__main__":
    sys.exit(main())
