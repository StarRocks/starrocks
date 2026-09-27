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
import textwrap
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parent / "check_sql_tester_case_files.py"
SPEC = importlib.util.spec_from_file_location("check_sql_tester_case_files", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"failed to load {MODULE_PATH}")
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


@contextlib.contextmanager
def case_tree(files):
    """A throwaway repo root holding the given case files, and chdir into it.

    `files` maps a path under test/sql to its content, e.g.
    {"test_array/T/test_array": "-- name: test_a\\nselect 1;\\n"}.
    """
    original = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        for rel, content in files.items():
            path = Path(tmpdir) / MODULE.CASE_DIR / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(textwrap.dedent(content), encoding="utf-8")
        os.chdir(tmpdir)
        try:
            yield Path(tmpdir)
        finally:
            os.chdir(original)


def run_check(selected=None):
    """Run check() over the whole tree, or over the named files, and return the problems."""
    t_files, r_files = MODULE.walk_case_files()
    return MODULE.check(selected if selected is not None else t_files + r_files,
                        t_files, r_files)


def whys(problems):
    return " | ".join(why for _, _, why in problems)


class Rule1Test(unittest.TestCase):
    """A case written in T that no R file records."""

    def test_flags_a_case_that_is_only_in_t(self):
        with case_tree({
            "suite/T/file_a": "-- name: test_recorded\nselect 1;\n"
                              "-- name: test_never_recorded\nselect 2;\n",
            "suite/R/file_a": "-- name: test_recorded\nselect 1;\n-- result:\n1\n-- !result\n",
        }):
            problems = run_check()
        self.assertEqual([p[1] for p in problems], ["test_never_recorded"])
        self.assertIn("recorded in no R file", whys(problems))

    def test_accepts_a_case_recorded_in_any_r_file(self):
        """R files are pooled -- collection walks every one of them, not the matching name."""
        with case_tree({
            "suite/T/file_a": "-- name: test_a\nselect 1;\n",
            "suite/R/file_b": "-- name: test_a\nselect 1;\n-- result:\n1\n-- !result\n",
        }):
            self.assertEqual(run_check(), [])

    def test_does_not_flag_a_case_that_is_only_in_r(self):
        """The reverse is legal: R is what the runner reads."""
        with case_tree({
            "suite/T/file_a": "-- name: test_a\nselect 1;\n",
            "suite/R/file_a": "-- name: test_a\nselect 1;\n-- result:\n1\n-- !result\n"
                              "-- name: test_extra\nselect 2;\n-- result:\n2\n-- !result\n",
        }):
            self.assertEqual(run_check(), [])


class SelectionTest(unittest.TestCase):
    """Rule 1 is a cross-file relation, so a change on either side has to reach it.

    A T case is checked against the names recorded anywhere under /R. Editing an R file
    to drop a case breaks that relation without touching the T file at all, and deleting
    an R file does not even leave a readable path behind. Selecting only what the change
    names would let both through.
    """

    def test_editing_r_reaches_the_t_case_it_orphans(self):
        with case_tree({
            "suite/T/file_a": "-- name: test_a\nselect 1;\n-- name: test_b\nselect 2;\n",
            "suite/R/file_a": "-- name: test_a\nselect 1;\n-- result:\n1\n-- !result\n",
        }):
            target = str(Path(MODULE.CASE_DIR) / "suite" / "R" / "file_a")
            selected = MODULE.expand_selection([target])
            t_files, r_files = MODULE.walk_case_files()
            problems = MODULE.check(selected, t_files, r_files)
        self.assertEqual([p[1] for p in problems], ["test_b"], whys(problems))

    def test_a_deleted_r_file_still_reaches_its_t_file(self):
        """changed_files cannot hand over a path that no longer exists, but the T file
        whose pairing it broke is still there."""
        with case_tree({
            "suite/T/file_a": "-- name: test_a\nselect 1;\n",
        }):
            gone = str(Path(MODULE.CASE_DIR) / "suite" / "R" / "file_a")
            self.assertFalse(os.path.exists(gone), "precondition: the R file is gone")
            selected = MODULE.expand_selection([gone])
            t_files, r_files = MODULE.walk_case_files()
            problems = MODULE.check(selected, t_files, r_files)
        self.assertEqual([p[1] for p in problems], ["test_a"], whys(problems))

    def test_expansion_keeps_the_path_it_was_given(self):
        with case_tree({
            "suite/T/file_a": "-- name: test_a\nselect 1;\n",
            "suite/R/file_a": "-- name: test_a\nselect 1;\n-- result:\n1\n-- !result\n",
        }):
            r = str(Path(MODULE.CASE_DIR) / "suite" / "R" / "file_a")
            t = str(Path(MODULE.CASE_DIR) / "suite" / "T" / "file_a")
            self.assertEqual(sorted(MODULE.expand_selection([r])), sorted([r, t]))

    def test_expansion_does_not_widen_a_t_only_change(self):
        """Touching T does not pull its R in: rule 2 lives on the R side and a T change
        cannot orphan anything there."""
        with case_tree({
            "suite/T/file_a": "-- name: test_a\nselect 1;\n",
            "suite/R/file_a": "-- name: test_a\nselect 1;\n-- result:\n1\n-- !result\n",
        }):
            t = str(Path(MODULE.CASE_DIR) / "suite" / "T" / "file_a")
            self.assertEqual(MODULE.expand_selection([t]), [t])


class Rule2Test(unittest.TestCase):
    """A case name nose will not collect."""

    def test_flags_a_name_without_test_in_it(self):
        with case_tree({
            "suite/T/file_a": "-- name: sec_to_time\nselect 1;\n",
            "suite/R/file_a": "-- name: sec_to_time\nselect 1;\n-- result:\n1\n-- !result\n",
        }):
            problems = run_check()
        self.assertEqual([p[1] for p in problems], ["sec_to_time"])
        self.assertIn("testMatch", whys(problems))

    def test_flags_a_leading_underscore(self):
        """nose refuses a leading underscore before testMatch is consulted at all.

        `__test_continuous_insert` does match testMatch -- the `_` at index 1 satisfies
        the character class -- so a checker that only ran the regex would pass it. It
        still never runs: selector.wantMethod returns False on the underscore first.
        """
        self.assertTrue(MODULE.NOSE_TEST_MATCH.search("__test_continuous_insert"),
                        "precondition: testMatch alone accepts this name")
        with case_tree({
            "suite/T/file_a": "-- name: __test_continuous_insert\nselect 1;\n",
            "suite/R/file_a": "-- name: __test_continuous_insert\nselect 1;\n"
                              "-- result:\n1\n-- !result\n",
        }):
            problems = run_check()
        self.assertEqual([p[1] for p in problems], ["__test_continuous_insert"])
        self.assertIn("underscore", whys(problems))

    def test_applies_to_safe_name_before_the_gates(self):
        """The method name is to_safe_name(case name), not the case name.

        test_sql_cases.name_func returns parameterized.to_safe_name(...), which replaces
        every run of non-word characters with a single underscore. A leading hyphen is
        legal in the runner's grammar -- `-` is in [a-zA-Z0-9_-] -- and it satisfies
        testMatch through the `-` in nose's own character class. But to_safe_name turns
        it into a leading underscore, and nose refuses that before testMatch is consulted.

        So `-- name: -test_x` is a case the runner parses, the guard would wave through,
        and nose silently skips.
        """
        for name in ("-test_x", "--test_x", "-x_test"):
            with self.subTest(name=name):
                self.assertTrue(MODULE.NOSE_TEST_MATCH.search(name),
                                "precondition: the raw name satisfies testMatch")
                self.assertFalse(name.startswith("_"),
                                 "precondition: the raw name has no leading underscore")
                self.assertFalse(MODULE.nose_collects(name),
                                 "but to_safe_name gives it one, so nose refuses it")

    def test_sanitisation_does_not_reject_what_it_should_keep(self):
        """A hyphen in the middle is sanitised to an underscore and stays collectable."""
        for name in ("test-x", "a-test", "test-x-y"):
            with self.subTest(name=name):
                self.assertTrue(MODULE.nose_collects(name))

    def test_flags_a_leading_hyphen_end_to_end(self):
        with case_tree({
            "suite/T/file_a": "-- name: -test_x\nselect 1;\n",
            "suite/R/file_a": "-- name: -test_x\nselect 1;\n-- result:\n1\n-- !result\n",
        }):
            problems = run_check()
        self.assertEqual([p[1] for p in problems], ["-test_x"])
        self.assertIn("underscore", whys(problems))

    def test_accepts_the_names_nose_does_collect(self):
        for name in ("test_a", "testArrayNE", "a_test_b", "Test_a"):
            with self.subTest(name=name):
                self.assertTrue(MODULE.nose_collects(name))

    def test_rejects_the_names_nose_does_not(self):
        for name in ("sec_to_time", "_test_a", "__test_a", "add_GIN_index"):
            with self.subTest(name=name):
                self.assertFalse(MODULE.nose_collects(name))


class NameGrammarTest(unittest.TestCase):
    """The name has to be parsed the way the runner parses it, not more loosely.

    A guard that reads more of the line than the runner does can approve a case the
    runner skips, which is the one thing it must never do.
    """

    def test_name_stops_where_the_runner_stops(self):
        with case_tree({"suite/T/file_a": "-- name: plain.test\nselect 1;\n"}) as root:
            names = MODULE.case_names(str(root / MODULE.CASE_DIR / "suite/T/file_a"))
        self.assertEqual(names, ["plain"])

    def test_a_name_the_runner_truncates_into_an_uncollectable_one_is_flagged(self):
        """`-- name: plain.test` is the case `plain`. Reading the whole tail would find
        `.test`, satisfy testMatch and wave it through -- while nose, given `plain`,
        collects nothing."""
        self.assertTrue(MODULE.nose_collects("plain.test"),
                        "precondition: the loose reading looks fine")
        self.assertFalse(MODULE.nose_collects("plain"),
                         "precondition: the runner's reading does not")
        with case_tree({
            "suite/T/file_a": "-- name: plain.test\nselect 1;\n",
            "suite/R/file_a": "-- name: plain.test\nselect 1;\n-- result:\n1\n-- !result\n",
        }):
            problems = run_check()
        self.assertEqual([p[1] for p in problems], ["plain"])
        self.assertIn("testMatch", whys(problems))

    def test_truncations_that_stay_collectable_are_left_alone(self):
        """Several real cases end in `;` or `${uuid0}`; the runner truncates there and the
        remaining name is still fine, so there is nothing to report."""
        for line, expected in (("-- name: test_x;", "test_x"),
                               ("-- name: test_x_${uuid0}", "test_x_"),
                               ("-- name: test_x:sub", "test_x")):
            with self.subTest(line=line):
                with case_tree({
                    "suite/T/file_a": line + "\nselect 1;\n",
                    "suite/R/file_a": line + "\nselect 1;\n-- result:\n1\n-- !result\n",
                }) as root:
                    names = MODULE.case_names(str(root / MODULE.CASE_DIR / "suite/T/file_a"))
                    problems = run_check()
                self.assertEqual(names, [expected])
                self.assertEqual(problems, [], whys(problems))

    def test_a_marker_with_no_parseable_name_is_flagged_as_fatal(self):
        """read_t_r_file does findall(...)[0] with no guard, so this raises rather than
        skipping the case."""
        with case_tree({"suite/T/file_a": "-- name: .oops\nselect 1;\n"}):
            problems = run_check()
        self.assertEqual(len(problems), 1, whys(problems))
        self.assertIn("IndexError", whys(problems))


class Rule3Test(unittest.TestCase):
    """A T/R pair that does not agree on its extension."""

    def test_flags_a_pair_whose_extensions_differ(self):
        with case_tree({
            "suite/T/file_a.sql": "-- name: test_a\nselect 1;\n",
            "suite/R/file_a": "-- name: test_a\nselect 1;\n-- result:\n1\n-- !result\n",
        }):
            problems = run_check()
        self.assertEqual(len(problems), 1)
        self.assertIn("share an extension", whys(problems))

    def test_reports_the_pair_once_not_once_per_side(self):
        with case_tree({
            "suite/T/file_a.sql": "-- name: test_a\nselect 1;\n",
            "suite/R/file_a": "-- name: test_a\nselect 1;\n-- result:\n1\n-- !result\n",
        }):
            t_files, r_files = MODULE.walk_case_files()
            problems = MODULE.check(t_files + r_files, t_files, r_files)
        self.assertEqual(len(problems), 1)

    def test_two_complete_pairs_are_not_a_mismatch(self):
        """Both extensions present on both sides is two correct pairs, not a problem.

        Recording swaps /T/ for /R/ and keeps the file name, so T/file_a records into
        R/file_a and T/file_a.sql into R/file_a.sql. Neither can overwrite the other.
        Matching on the stem alone reports the cross product -- T/file_a against
        R/file_a.sql, and T/file_a.sql against R/file_a -- two errors for two correct
        pairs.
        """
        with case_tree({
            "suite/T/file_a": "-- name: test_a\nselect 1;\n",
            "suite/R/file_a": "-- name: test_a\nselect 1;\n-- result:\n1\n-- !result\n",
            "suite/T/file_a.sql": "-- name: test_b\nselect 2;\n",
            "suite/R/file_a.sql": "-- name: test_b\nselect 2;\n-- result:\n2\n-- !result\n",
        }):
            self.assertEqual(run_check(), [], whys(run_check()))

    def test_still_flags_the_stale_side_when_only_one_pair_is_complete(self):
        """T/file_a.sql has no R/file_a.sql, so re-recording it leaves a second R file."""
        with case_tree({
            "suite/T/file_a": "-- name: test_a\nselect 1;\n",
            "suite/R/file_a": "-- name: test_a\nselect 1;\n-- result:\n1\n-- !result\n",
            "suite/T/file_a.sql": "-- name: test_a\nselect 1;\n",
        }):
            problems = run_check()
        self.assertEqual(len(problems), 1, whys(problems))
        self.assertIn("share an extension", whys(problems))

    def test_accepts_a_pair_that_agrees(self):
        with case_tree({
            "suite/T/file_a.sql": "-- name: test_a\nselect 1;\n",
            "suite/R/file_a.sql": "-- name: test_a\nselect 1;\n-- result:\n1\n-- !result\n",
        }):
            self.assertEqual(run_check(), [])


class Rule4Test(unittest.TestCase):
    """A line meant as a case marker that the parser reads as a comment."""

    def test_flags_a_marker_missing_its_space(self):
        with case_tree({
            "suite/T/file_a": "-- name: test_a\nselect 1;\n"
                              "--name: test_b\nselect 2;\n",
            "suite/R/file_a": "-- name: test_a\nselect 1;\n-- result:\n1\n-- !result\n",
        }):
            problems = run_check()
        self.assertEqual(len(problems), 1)
        self.assertIn("opens no case here", whys(problems))
        self.assertTrue(problems[0][0].endswith(":3"), problems[0][0])

    def test_flags_a_marker_on_the_first_line(self):
        """The worst shape: nothing above it, so the whole file is discarded."""
        with case_tree({
            "suite/T/file_a": "--name: test_only @system\nselect 1;\n",
        }):
            problems = run_check()
        self.assertEqual([p[0].endswith(":1") for p in problems], [True])

    def test_flags_extra_space_variants(self):
        for text in ("--name: test_b", "--  name: test_b", "-- name : test_b"):
            with self.subTest(text=text):
                with case_tree({"suite/T/file_a": "-- name: test_a\nselect 1;\n"
                                                  + text + "\nselect 2;\n",
                                "suite/R/file_a": "-- name: test_a\nselect 1;\n"
                                                  "-- result:\n1\n-- !result\n"}):
                    problems = run_check()
                self.assertEqual(len(problems), 1, whys(problems))

    def test_flags_an_indented_marker(self):
        """Indentation is enough to lose the case, and is the shape that hides best.

        choose_cases rstrips the newline and nothing else, so `  -- name: test_b` matches
        neither startswith("--") nor startswith(NAME_FLAG). It is not skipped as a comment
        the way `--name: test_b` is; it reaches the statement branch and goes to the server.
        No case opens either way.
        """
        for lead in ("  ", "\t", "    "):
            with self.subTest(lead=repr(lead)):
                with case_tree({
                    "suite/T/file_a": "-- name: test_a\nselect 1;\n"
                                      + lead + "-- name: test_b\nselect 2;\n",
                    "suite/R/file_a": "-- name: test_a\nselect 1;\n"
                                      "-- result:\n1\n-- !result\n",
                }):
                    problems = run_check()
                self.assertEqual(len(problems), 1, whys(problems))
                self.assertTrue(problems[0][0].endswith(":3"), problems[0][0])

    def test_flags_an_indented_marker_carrying_tags(self):
        with case_tree({
            "suite/T/file_a": "-- name: test_a\nselect 1;\n"
                              "   -- name: test_b @system @slow\nselect 2;\n",
            "suite/R/file_a": "-- name: test_a\nselect 1;\n-- result:\n1\n-- !result\n",
        }):
            problems = run_check()
        self.assertEqual(len(problems), 1, whys(problems))

    def test_does_not_flag_an_indented_commented_out_schema(self):
        """The tail still has to be a case declaration, indented or not."""
        with case_tree({
            "suite/T/file_a": "-- name: test_a\n"
                              "    -- create table t (\n"
                              "    --     name: STRING,\n"
                              "    -- )\n"
                              "select 1;\n",
            "suite/R/file_a": "-- name: test_a\nselect 1;\n-- result:\n1\n-- !result\n",
        }):
            self.assertEqual(run_check(), [])

    def test_accepts_the_exact_spelling(self):
        with case_tree({
            "suite/T/file_a": "-- name: test_a\nselect 1;\n",
            "suite/R/file_a": "-- name: test_a\nselect 1;\n-- result:\n1\n-- !result\n",
        }):
            self.assertEqual(run_check(), [])

    def test_does_not_flag_a_commented_out_schema(self):
        """test_iceberg_variant_query_1 has `--    name: STRING,` inside a commented DDL.

        It is a comment about a column called name, not a case marker, so the tail has to
        look like a case declaration and nothing else before this fires.
        """
        with case_tree({
            "suite/T/file_a": "-- name: test_a\n"
                              "-- create table t (\n"
                              "--             name: STRING,\n"
                              "--             age: INT\n"
                              "-- )\n"
                              "select 1;\n",
            "suite/R/file_a": "-- name: test_a\nselect 1;\n-- result:\n1\n-- !result\n",
        }):
            self.assertEqual(run_check(), [])

    def test_flags_a_marker_carrying_tags(self):
        with case_tree({
            "suite/T/file_a": "-- name: test_a\nselect 1;\n"
                              "--name: test_b @sequential @native\nselect 2;\n",
            "suite/R/file_a": "-- name: test_a\nselect 1;\n-- result:\n1\n-- !result\n",
        }):
            problems = run_check()
        self.assertEqual(len(problems), 1, whys(problems))

    def test_does_not_flag_an_ordinary_comment(self):
        with case_tree({
            "suite/T/file_a": "-- name: test_a\n-- the name of the game\nselect 1;\n",
            "suite/R/file_a": "-- name: test_a\nselect 1;\n-- result:\n1\n-- !result\n",
        }):
            self.assertEqual(run_check(), [])


class HelperTest(unittest.TestCase):

    def test_case_names_reads_only_exact_markers(self):
        with case_tree({"suite/T/file_a": "-- name: test_a\n--name: test_b\nselect 1;\n"}) as root:
            names = MODULE.case_names(str(root / MODULE.CASE_DIR / "suite/T/file_a"))
        self.assertEqual(names, ["test_a"])

    def test_case_names_on_a_missing_file_is_empty_not_an_error(self):
        self.assertEqual(MODULE.case_names("does/not/exist"), [])

    def test_near_name_lines_on_a_missing_file_is_empty_not_an_error(self):
        self.assertEqual(MODULE.near_name_lines("does/not/exist"), [])

    def test_walk_case_files_ignores_dotfiles(self):
        with case_tree({"suite/T/file_a": "-- name: test_a\n",
                        "suite/T/.hidden": "-- name: test_hidden\n"}):
            t_files, _ = MODULE.walk_case_files()
        self.assertEqual([os.path.basename(p) for p in t_files], ["file_a"])

    def test_files_outside_t_and_r_are_skipped(self):
        with case_tree({"suite/T/file_a": "-- name: test_a\n",
                        "suite/R/file_a": "-- name: test_a\n"}) as root:
            stray = root / MODULE.CASE_DIR / "suite" / "data.csv"
            stray.write_text("1,2,3\n", encoding="utf-8")
            t_files, r_files = MODULE.walk_case_files()
            problems = MODULE.check([str(stray)], t_files, r_files)
        self.assertEqual(problems, [])


class MainTest(unittest.TestCase):
    """Exit codes and the failure paths."""

    def run_main(self, argv):
        out, err = io.StringIO(), io.StringIO()
        old = sys.argv
        sys.argv = ["check_sql_tester_case_files.py"] + argv
        try:
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = MODULE.main()
        finally:
            sys.argv = old
        return code, out.getvalue(), err.getvalue()

    def test_all_on_a_clean_tree_exits_zero(self):
        with case_tree({
            "suite/T/file_a": "-- name: test_a\nselect 1;\n",
            "suite/R/file_a": "-- name: test_a\nselect 1;\n-- result:\n1\n-- !result\n",
        }):
            code, out, _ = self.run_main(["--all"])
        self.assertEqual(code, 0)
        self.assertIn("ok", out)

    def test_all_on_a_dirty_tree_exits_one_and_names_the_case(self):
        with case_tree({"suite/T/file_a": "-- name: test_a\nselect 1;\n"}):
            code, _, err = self.run_main(["--all"])
        self.assertEqual(code, 1)
        self.assertIn("test_a", err)

    def test_files_checks_only_what_it_is_given(self):
        with case_tree({
            "suite/T/file_a": "-- name: test_unrecorded_a\nselect 1;\n",
            "suite/T/file_b": "-- name: test_unrecorded_b\nselect 1;\n",
        }) as root:
            target = str(Path(MODULE.CASE_DIR) / "suite" / "T" / "file_b")
            code, _, err = self.run_main(["--files", target])
        self.assertEqual(code, 1)
        self.assertIn("test_unrecorded_b", err)
        self.assertNotIn("test_unrecorded_a", err)

    def test_nothing_selected_exits_zero(self):
        """An empty selection is not a failure -- but it is also not a check.

        This is the shape that let the checker's own workflow go green without running a
        single rule: a PR touching only the checker changes no file under test/sql, so
        changed_files() legitimately comes back empty. Pinned deliberately rather than
        left to chance. The tree here is dirty -- rule 1 would fire on it -- and the run
        still exits 0, which is the point.

        Built on a real repository, because an empty diff and a failed one are no longer
        the same thing: see test_an_unusable_base_is_fatal_not_an_empty_selection.
        """
        original = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            case = Path(tmpdir) / MODULE.CASE_DIR / "suite" / "T" / "file_a"
            case.parent.mkdir(parents=True, exist_ok=True)
            case.write_text("-- name: test_a\nselect 1;\n", encoding="utf-8")
            git = lambda *a: subprocess.run(["git", "-c", "user.email=a@b",
                                             "-c", "user.name=c"] + list(a),
                                            check=True, capture_output=True, cwd=tmpdir)
            git("init", "-q", ".")
            git("add", "-A")
            git("commit", "-qm", "base")
            git("branch", "-f", "base_ref")
            # the change touches something that is not a case file
            (Path(tmpdir) / "unrelated.py").write_text("x = 1\n", encoding="utf-8")
            git("add", "-A")
            git("commit", "-qm", "checker only")
            os.chdir(tmpdir)
            try:
                code, out, err = self.run_main(["--base", "base_ref"])
            finally:
                os.chdir(original)
        self.assertEqual(code, 0, err)
        self.assertIn("no case files to check", out)

    def test_an_unusable_base_is_fatal_not_an_empty_selection(self):
        """A guard that cannot work out what changed must say so, not pass.

        An empty selection and a failed `git diff` both leave the same empty list. If
        that is reported as `no case files to check` the job goes green having checked
        nothing, and looks exactly like a PR that touched no case file.
        """
        original = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            case = Path(tmpdir) / MODULE.CASE_DIR / "suite" / "T" / "file_a"
            case.parent.mkdir(parents=True, exist_ok=True)
            case.write_text("-- name: test_a\nselect 1;\n", encoding="utf-8")
            subprocess.run(["git", "init", "-q", tmpdir], check=True)
            os.chdir(tmpdir)
            try:
                subprocess.run(["git", "add", "-A"], check=True, capture_output=True)
                subprocess.run(["git", "-c", "user.email=a@b", "-c", "user.name=c",
                                "commit", "-qm", "init"], check=True, capture_output=True)
                code, out, err = self.run_main(["--base", "origin/does-not-exist"])
            finally:
                os.chdir(original)
        self.assertNotEqual(code, 0, "a guard that cannot read the diff must not pass")
        self.assertNotIn("no case files to check", out)
        self.assertIn("git", err.lower())

    def test_a_renamed_r_file_still_reaches_its_t_file(self):
        """A rename is a deletion on the source path, and rule 1 needs that path.

        `git mv R/file_a R/file_b` plus a new case name in the moved file leaves test_a
        recorded nowhere. Rename detection reports only R/file_b, so R/file_a -- the only
        route expand_selection has to T/file_a -- never reaches the check and the whole
        thing goes green. The asserted precondition below is the collapsing itself.
        """
        original = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / MODULE.CASE_DIR / "suite"
            (root / "T").mkdir(parents=True, exist_ok=True)
            (root / "R").mkdir(parents=True, exist_ok=True)
            (root / "T" / "file_a").write_text(
                "-- name: test_a\nselect 1;\n", encoding="utf-8")
            (root / "R" / "file_a").write_text(
                "-- name: test_a\nselect 1;\n-- result:\n1\n-- !result\n", encoding="utf-8")
            subprocess.run(["git", "init", "-q", tmpdir], check=True)
            os.chdir(tmpdir)
            try:
                def run(*args):
                    return subprocess.run(
                        ["git", "-c", "user.email=a@b", "-c", "user.name=c"] + list(args),
                        check=True, capture_output=True, text=True)

                run("add", "-A")
                run("commit", "-qm", "init")
                run("mv", str(Path(MODULE.CASE_DIR) / "suite" / "R" / "file_a"),
                    str(Path(MODULE.CASE_DIR) / "suite" / "R" / "file_b"))
                (root / "R" / "file_b").write_text(
                    "-- name: test_b\nselect 1;\n-- result:\n1\n-- !result\n",
                    encoding="utf-8")
                run("add", "-A")
                run("commit", "-qm", "rename and rename the case")

                collapsed = run("diff", "--name-only", "HEAD~1", "HEAD").stdout.split()
                self.assertNotIn(
                    str(Path(MODULE.CASE_DIR) / "suite" / "R" / "file_a"), collapsed,
                    "precondition: rename detection hides the source path")

                code, _, err = self.run_main(["--base", "HEAD~1"])
            finally:
                os.chdir(original)
        self.assertEqual(code, 1, "the orphaned T case has to be reported")
        self.assertIn("test_a", err)

    def test_run_from_the_wrong_directory_exits_two(self):
        original = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            try:
                code, _, err = self.run_main(["--all"])
            finally:
                os.chdir(original)
        self.assertEqual(code, 2)
        self.assertIn("repository root", err)

    def test_unreadable_case_file_is_not_fatal(self):
        with case_tree({"suite/T/file_a": "-- name: test_a\nselect 1;\n",
                        "suite/R/file_a": "-- name: test_a\nselect 1;\n"
                                          "-- result:\n1\n-- !result\n"}) as root:
            unreadable = root / MODULE.CASE_DIR / "suite" / "T" / "file_b"
            unreadable.write_bytes(b"\xff\xfe-- name: test_b\n")
            code, _, _ = self.run_main(["--all"])
        self.assertIn(code, (0, 1))


if __name__ == "__main__":
    unittest.main()
