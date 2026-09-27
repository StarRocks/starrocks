#!/usr/bin/env python3
"""
Check that SQL-Tester case files are shaped so their cases actually run.

A case can sit in the tree looking perfectly normal and never execute. Four ways that
happens, one rule each:

  1. The case is written in a T file and its name appears in no R file. Validate mode --
     the mode CI runs -- collects cases from R, not from T (choose_cases.get_cases uses
     `self.t if record_mode else self.r`), so a case that was never recorded does not
     exist as far as the runner is concerned.

  2. nose will not collect the case name. Cases become test methods named after the case
     itself (test_sql_cases.name_func returns the name with nothing prepended), and nose
     applies two gates to a method name (selector.py, wantMethod): a leading underscore is
     refused outright as a 'private' method, and only then is it matched against
     `(?:^|[\b_./-])[Tt]est`. A case called `sec_to_time` fails the second gate; one
     called `__test_continuous_insert` fails the first. Either way it is parsed, expanded,
     and never called.

  3. A T file and its R file do not share an extension. Nothing is wrong at validate time,
     because collection walks every file under /R and reads the names out of them. But
     recording derives the R path from the T path (save_r_into_file swaps /T/ for /R/), so
     re-recording writes a second R file beside the first, and the case name then lives in
     two of them.

  4. A line meant as a case marker is not spelled `-- name: ` exactly. choose_cases tests
     that prefix literally, so `--name: foo` is an ordinary comment: the statements under
     it join the case above instead of starting their own, and if there is no case above,
     the whole run of statements is dropped -- the empty name is filtered out at the end
     of get_cases.

None of the four fails anywhere today: the case is simply absent from the run, or becomes
absent later. That is what this is for.

Scope is the files a change touches, not the whole tree, so touching a file means bringing
that file up to standard. Run with --all to see everything.

    python3 build-support/check_sql_tester_case_files.py --base origin/main
    python3 build-support/check_sql_tester_case_files.py --all
    python3 build-support/check_sql_tester_case_files.py --files test/sql/test_array/T/test_array
"""

import argparse
import os
import re
import subprocess
import sys

CASE_DIR = os.path.join("test", "sql")

# The runner's own name grammar, and it has to be exactly this or the guard can approve a
# case the runner skips. choose_cases.read_t_r_file takes two steps: the line must start
# with NAME_FLAG, and then the name is pulled out with
#
#     re.compile("name: ([a-zA-Z0-9_-]+)").findall(line)[0]
#
# -- a search for the first run of that restricted set, not the rest of the line. So
# `-- name: plain.test` declares the case `plain`, which nose will not collect; reading it
# as `plain.test` would find `.test` and wave it through. The same truncation applies to
# `-- name: test_x;` and `-- name: test_x_${uuid0}`, which are the case `test_x` and
# `test_x_`.
NAME_TAIL_RE = re.compile(r"name: ([a-zA-Z0-9_-]+)")

# nose's own default, including the os.sep it interpolates. Note [\b...] is a backspace
# character, not a word boundary -- that is nose's, not a transcription slip.
NOSE_TEST_MATCH = re.compile(r"(?:^|[\b_\.%s-])[Tt]est" % re.escape(os.sep))

# The exact prefix lib/__init__.py uses. A line that only looks like it, such as
# `--name: x`, is read as an ordinary comment by choose_cases.
NAME_FLAG = "-- name: "

# parameterized.to_safe_name, which name_func puts between the case name and the method
# name. Every run of non-word characters collapses to one underscore.
TO_SAFE_NAME_RE = re.compile(r"[^a-zA-Z0-9_]+")
# ... which is what this catches: a comment line whose author meant it as a case marker.
# The tail has to look like a case declaration and nothing else -- a bare name, in the
# character set choose_cases accepts, plus any @tags. Without that, commented-out schemas
# match too: test_iceberg_variant_query_1 has `--    name: STRING,` inside one.
NEAR_NAME_RE = re.compile(r"^--\s*name\s*:\s*[A-Za-z0-9_-]+(?:\s+@[A-Za-z0-9_-]+)*\s*$")


def method_name(case_name):
    """The method name a case gets, which is not the case name.

    test_sql_cases.name_func returns parameterized.to_safe_name(case.name), and that is

        re.sub("[^a-zA-Z0-9_]+", "_", s)

    So the hyphen the runner's grammar allows does not survive: `-test_x` becomes
    `_test_x`. Checking the case name instead of this would wave that through -- the raw
    name has no leading underscore and `-test` satisfies testMatch -- while nose refuses
    the method outright.
    """
    return TO_SAFE_NAME_RE.sub("_", case_name)


def nose_collects(name):
    """Whether nose would collect the method this case name produces.

    Two gates, in nose's own order (selector.py, wantMethod): a leading underscore is
    refused outright as a 'private' method, before testMatch is consulted at all. Both
    apply to the sanitised method name, not to the case name.
    """
    sanitised = method_name(name)
    if sanitised.startswith("_"):
        return False
    return bool(NOSE_TEST_MATCH.search(sanitised))


def read_lines(path):
    try:
        with open(path, encoding="utf-8", errors="ignore") as handle:
            return handle.read().split("\n")
    except OSError:
        return []


def case_names(path):
    """The case names the runner would take from this file, parsed the way it parses them."""
    names = []
    for line in read_lines(path):
        if not line.startswith(NAME_FLAG):
            continue
        found = NAME_TAIL_RE.findall(line)
        if found:
            names.append(found[0])
    return names


def unparseable_name_lines(path):
    """NAME_FLAG lines the runner cannot take a name from at all.

    read_t_r_file indexes findall(...)[0] with no guard, so such a line does not skip the
    case -- it raises IndexError and takes the whole run down.
    """
    return [(lineno, line)
            for lineno, line in enumerate(read_lines(path), 1)
            if line.startswith(NAME_FLAG) and not NAME_TAIL_RE.search(line)]


def near_name_lines(path):
    """Lines that were meant as case markers but are not spelled `-- name: `."""
    lines = read_lines(path)
    out = []
    for lineno, text in enumerate(lines, 1):
        if text.startswith(NAME_FLAG):
            continue
        if NEAR_NAME_RE.match(text):
            out.append((lineno, text))
    return out


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


def paired_t_file(path):
    """The T file an R file is recorded from, by exact name, or None."""
    marker = os.sep + "R" + os.sep
    if marker not in path:
        return None
    return path.replace(marker, os.sep + "T" + os.sep, 1)


def expand_selection(paths):
    """The paths to check, given the paths a change touched.

    Rule 1 is a relation between two files: a T case has to be recorded in some R file.
    Either side can break it. Dropping a case from an R file orphans the T case without
    the T file being touched at all, and deleting an R file leaves no readable path
    behind for the check to start from. Selecting only what the change names would let
    both through -- the whole-tree run catches them, the per-PR run does not.

    So a touched R path also brings in its T file. Not the other way round: rule 2 lives
    on the R side and a change to T cannot orphan anything there, so pulling the R file
    in would only surface pre-existing problems the change did not cause.
    """
    selected, seen = [], set()
    for path in paths:
        for candidate in (path, paired_t_file(path)):
            if candidate and candidate not in seen and os.path.exists(candidate):
                seen.add(candidate)
                selected.append(candidate)
    return selected


class GitUnavailable(Exception):
    """git could not tell us what changed, which is not the same as nothing changing."""


def git(*args):
    """Run git, and refuse to carry on if it failed.

    A failed `git diff` returns empty stdout, which is indistinguishable from a change
    that touched no case file. Left unchecked, an unusable --base -- a ref the runner
    never fetched, a shallow clone, a base branch that is not origin/main -- makes this
    guard report `no case files to check` and exit 0, green and having checked nothing.
    """
    result = subprocess.run(["git"] + list(args), capture_output=True, text=True)
    if result.returncode != 0:
        raise GitUnavailable("git %s: %s" % (
            " ".join(args), result.stderr.strip() or "exit %d" % result.returncode))
    return result.stdout


def changed_files(base):
    merge_base = git("merge-base", base, "HEAD").strip()
    # Deletions are kept here, unlike the paths handed to the check: a deleted R file
    # cannot be read, but the T file it leaves stranded can. expand_selection drops the
    # unreadable path and keeps the counterpart.
    out = git("diff", "--name-only", merge_base, "HEAD").split()
    return expand_selection([f for f in out if f.startswith(CASE_DIR + os.sep)])


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
                if not nose_collects(name):
                    why = ("name does not match nose's testMatch, so the case is expanded "
                           "and never called -- rename it to start with test, in T and R "
                           "alike")
                    if method_name(name).startswith("_"):
                        why = ("becomes the method %r, which starts with an underscore; "
                               "nose refuses that as a private method before testMatch is "
                               "even consulted, so the case is expanded and never called "
                               "-- rename it in T and R alike" % method_name(name))
                    problems.append((path, name, why))

        # A marker the parser cannot take a name from at all. Not one of the four -- this
        # one is loud, it raises -- but it is found by the same reading, so report it here
        # rather than let the run discover it.
        for lineno, text in unparseable_name_lines(path):
            problems.append((
                "%s:%d" % (path, lineno), text.strip(),
                "starts with %r but has no name the runner can parse after it; "
                "read_t_r_file indexes findall(...)[0] unguarded, so this raises "
                "IndexError and takes the whole run down" % NAME_FLAG))

        # 4. a line meant as a case marker that the parser reads as a comment
        for lineno, text in near_name_lines(path):
            problems.append((
                "%s:%d" % (path, lineno), text.strip(),
                "looks like a case marker but is not %r exactly, so choose_cases reads it "
                "as a comment -- the statements under it join the case above, or are "
                "dropped when there is no case above" % NAME_FLAG))

        # 3. a T/R pair that does not agree on its extension. Reported once per pair, no
        # matter which side of it the change touched.
        #
        # Recording swaps /T/ for /R/ and keeps the file name, so the pairing is by exact
        # name. A cross pair only matters when one of its two files has no exact
        # counterpart of its own: T/file_a, R/file_a, T/file_a.sql and R/file_a.sql are
        # two correct pairs, and matching on the stem alone would report the cross
        # product of them as two errors.
        other_dir = counterpart_dir(path)
        if other_dir and os.path.isdir(other_dir):
            stem, ext = os.path.splitext(os.path.basename(path))
            this_is_paired = os.path.exists(os.path.join(other_dir, os.path.basename(path)))
            for sibling in sorted(os.listdir(other_dir)):
                s_stem, s_ext = os.path.splitext(sibling)
                if s_stem != stem or s_ext == ext:
                    continue
                other = os.path.join(other_dir, sibling)
                other_is_paired = os.path.exists(
                    os.path.join(os.path.dirname(path), sibling))
                if this_is_paired and other_is_paired:
                    continue
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
        selected = expand_selection(args.files)
    else:
        try:
            selected = changed_files(args.base or "origin/main")
        except GitUnavailable as exc:
            print("cannot work out what changed, so nothing was checked: %s" % exc,
                  file=sys.stderr)
            return 2

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
