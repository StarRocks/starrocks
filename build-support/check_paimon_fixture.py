#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Check Paimon fixture contents, history, size, and explicit case dependencies."""

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "test/lib"))
from paimon_fixture import load_manifest, validate_transition

DATA = "test/sql/test_paimon_catalog/data"


def git(*args):
    return subprocess.check_output(["git", *args], cwd=REPO, text=True).strip()


def check(base=None):
    root = REPO / DATA
    manifest = load_manifest(root)
    changed_tables = set(manifest["tables"])
    if base:
        base = git("merge-base", base, "HEAD")
        files = git("ls-tree", "-r", "--name-only", base, "--", DATA).splitlines()
        if DATA + "/MANIFEST.json" in files:
            old = json.loads(git("show", base + ":" + DATA + "/MANIFEST.json"))
            validate_transition(old, manifest)
            changed_tables = set(old["tables"]) ^ set(manifest["tables"])
        # Count new blob contents, not renames or deletions. Include uncommitted files for local review.
        old_blobs = {line.split()[2] for line in git("ls-tree", "-r", base, "--", DATA).splitlines()}
        added = {}
        for path in root.rglob("*"):
            if path.is_file():
                blob = git("hash-object", str(path))
                if blob not in old_blobs:
                    added[blob] = path.stat().st_size
        if sum(added.values()) > manifest["budget"]["per_change_bytes"]:
            raise ValueError("new fixture blobs exceed per_change_bytes")

    used = set()
    suite = REPO / "test/sql/test_paimon_catalog"
    for directory in ("T", "R"):
        for path in sorted((suite / directory).iterdir()):
            if not path.is_file():
                continue
            content = path.read_text()
            if re.search(r"oss://[a-zA-Z0-9]", content):
                raise ValueError("%s: hard-coded OSS bucket" % path.relative_to(REPO))
            selections = re.findall(r'function: paimon_stage\("[^"\n]+", "([^"\n]+)"\)', content)
            if "paimon_stage(" in content and not selections:
                raise ValueError("%s: use literal comma-separated table names in paimon_stage" % path)
            for selection in selections:
                names = set(selection.split(","))
                if names - set(manifest["tables"]):
                    raise ValueError("%s: unknown fixture tables %s" % (path, names - set(manifest["tables"])))
                used.update(names)
                if directory == "T" and names & changed_tables:
                    print("Fixture changes affect: %s" % path.relative_to(REPO))
    unused = set(manifest["tables"]) - used
    if unused:
        print("Unused fixtures (consider removing): " + ", ".join(sorted(unused)))
    print("Paimon fixtures: %d tables, %d bytes; checks passed" %
          (len(manifest["tables"]), sum(entry["bytes"] for entry in manifest["tables"].values())))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", help="compare immutable fixtures and new blob budget against this merge base")
    args = parser.parse_args()
    try:
        check(args.base)
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError) as error:
        sys.exit("Paimon fixture check failed: %s" % error)
