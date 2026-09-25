#!/usr/bin/env python3
"""
Check that SQL-Tester case files are shaped so their cases actually run.

A case can sit in the tree looking perfectly normal and never execute. Three ways that
happens, one rule each:

  1. The case is written in a T file and its name appears in no R file. Validate mode --
     the mode CI runs -- collects cases from R, not from T (choose_cases.get_cases uses
     `self.t if record_mode else self.r`), so a case that was never recorded does not
     exist as far as the runner is concerned.

  2. The case name does not match nose's testMatch. Cases become test methods named after
     the case itself (test_sql_cases.name_func returns the name with nothing prepended),
     and nose collects a method only if its name matches `(?:^|[\b_./-])[Tt]est`. A case
     called `sec_to_time` is parsed, expanded, and never called.

  3. A T file and its R file do not share an extension. Nothing is wrong at validate time,
     because collection walks every file under /R and reads the names out of them. But
     recording derives the R path from the T path (save_r_into_file swaps /T/ for /R/), so
     re-recording writes a second R file beside the first, and the case name then lives in
     two of them.

None of the three fails anywhere today: the case is simply absent from the run, or becomes
absent later. That is what this is for.

Scope is the files a change touches, not the whole tree, so touching a file means bringing
that file up to standard. Run with --all to see everything.

    python3 test/check_case_files.py --base origin/main
    python3 test/check_case_files.py --all
    python3 test/check_case_files.py --files test/sql/test_array/T/test_array
"""

import argparse
import os
import re
import subprocess
import sys

CASE_DIR = os.path.join("test", "sql")
NAME_RE = re.compile(r"^-- name:\s*(\S+)", re.M)

# nose's own default, including the os.sep it interpolates. Note [\b...] is a backspace
# character, not a word boundary -- that is nose's, not a transcription slip.
NOSE_TEST_MATCH = re.compile(r"(?:^|[\b_\.%s-])[Tt]est" % re.escape(os.sep))


def case_names(path):
    try:
        with open(path, encoding="utf-8", errors="ignore") as handle:
            return NAME_RE.findall(handle.read())
    except OSError:
        return []


def walk_case_files():
    t_files, r_files = [], []
    for dir_path, _, file_names in os.walk(CASE_DIR):
        for name in file_names:
            if name.startswith("."):
                continue
            path = os.path.join(dir_path, name)
            if os.sep + "T" + os.sep in path:
                t_files.append(path)
            elif os.sep + "R" + os.sep in path:
                r_files.append(path)
    return sorted(t_files), sorted(r_files)


def changed_files(base):
    merge_base = subprocess.run(["git", "merge-base", base, "HEAD"],
                                capture_output=True, text=True).stdout.strip() or base
    out = subprocess.run(["git", "diff", "--name-only", "--diff-filter=d", merge_base, "HEAD"],
                         capture_output=True, text=True).stdout.split()
    return [f for f in out if f.startswith(CASE_DIR + os.sep) and os.path.exists(f)]


def counterpart_dir(path):
    """The /R directory matching a /T one, or the other way round."""
    for this, other in ((os.sep + "T" + os.sep, os.sep + "R" + os.sep),
                        (os.sep + "R" + os.sep, os.sep + "T" + os.sep)):
        if this in path:
            return os.path.dirname(path.replace(this, other, 1))
    return None


def check(selected, t_files, r_files):
    recorded = set()
    for path in r_files:
        recorded.update(case_names(path))

    problems, seen_pairs = [], set()
    for path in sorted(selected):
        is_t = os.sep + "T" + os.sep in path
        is_r = os.sep + "R" + os.sep in path
        if not (is_t or is_r):
            continue

        # 1. a case written in T that no R file records
        if is_t:
            for name in case_names(path):
                if name not in recorded:
                    problems.append((
                        path, name,
                        "written in T but recorded in no R file, so validate mode never "
                        "collects it -- record the case, or remove it"))

        # 2. a case name nose will not collect
        if is_r:
            for name in case_names(path):
                if not NOSE_TEST_MATCH.search(name):
                    problems.append((
                        path, name,
                        "name does not match nose's testMatch, so the case is expanded and "
                        "never called -- rename it to start with test, in T and R alike"))

        # 3. a T/R pair that does not agree on its extension. Reported once per pair, no
        # matter which side of it the change touched.
        other_dir = counterpart_dir(path)
        if other_dir and os.path.isdir(other_dir):
            stem, ext = os.path.splitext(os.path.basename(path))
            for sibling in sorted(os.listdir(other_dir)):
                s_stem, s_ext = os.path.splitext(sibling)
                if s_stem != stem or s_ext == ext:
                    continue
                other = os.path.join(other_dir, sibling)
                pair = tuple(sorted((path, other)))
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)
                problems.append((
                    pair[0], pair[1],
                    "T and R do not share an extension; recording derives the R name from "
                    "the T one, so re-recording would leave two R files"))
    return problems


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--base", help="check the case files changed since this ref")
    group.add_argument("--files", nargs="+", help="check these case files")
    group.add_argument("--all", action="store_true", help="check every case file")
    args = parser.parse_args()

    if not os.path.isdir(CASE_DIR):
        print("run this from the repository root: %s not found" % CASE_DIR, file=sys.stderr)
        return 2

    t_files, r_files = walk_case_files()
    if args.all:
        selected = t_files + r_files
    elif args.files:
        selected = args.files
    else:
        selected = changed_files(args.base or "origin/main")

    if not selected:
        print("no case files to check")
        return 0

    problems = check(selected, t_files, r_files)
    if not problems:
        print("checked %d case file(s): ok" % len(selected))
        return 0

    scope = "in the tree" if args.all else "changed here"
    print("%d problem(s) across %d case file(s) %s.\n"
          "These are cases that do not run. Bringing a file up to standard is part of "
          "touching it.\n" % (len(problems), len(selected), scope), file=sys.stderr)
    for path, subject, why in problems:
        print("  %s\n    %s\n    %s\n" % (path, subject, why), file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
