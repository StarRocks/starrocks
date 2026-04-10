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

from __future__ import print_function

import argparse
import os
import sys
from fnmatch import fnmatch


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Filter changed files to C++ sources within target dirs.")
    parser.add_argument(
        "--repo_root",
        required=True,
        help="Absolute path to the repository root.")
    parser.add_argument(
        "--exclude_globs",
        help="Filename containing globs for files that should be excluded.")
    parser.add_argument(
        "--source_dirs",
        required=True,
        help="Comma-separated root directories of the source code.")
    parser.add_argument(
        "--null",
        action="store_true",
        help="Use NUL as output delimiter for paths.")
    return parser.parse_args()


def _load_excludes(exclude_file):
    if not exclude_file or not os.path.exists(exclude_file):
        return []
    with open(exclude_file) as f:
        return [line.strip() for line in f if line.strip()]


def main():
    args = _parse_args()
    repo_root = os.path.abspath(args.repo_root)
    source_dirs = [os.path.abspath(p) for p in args.source_dirs.split(",") if p]
    exclude_globs = _load_excludes(args.exclude_globs)

    extensions = {".h", ".cc", ".cpp", ".tpp", ".hh", ".hpp"}
    changed = [line.strip() for line in sys.stdin.read().splitlines() if line.strip()]
    delimiter = "\0" if args.null else "\n"

    for relpath in changed:
        path = os.path.abspath(os.path.join(repo_root, relpath))
        if os.path.splitext(path)[1] not in extensions:
            continue
        if not any(path == d or path.startswith(d + os.sep) for d in source_dirs):
            continue
        if any(fnmatch(path, glob) for glob in exclude_globs):
            continue
        if os.path.exists(path):
            sys.stdout.write(path + delimiter)


if __name__ == "__main__":
    main()
