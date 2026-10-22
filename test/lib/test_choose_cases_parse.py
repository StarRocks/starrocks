# !/usr/bin/env python
# -*- coding: utf-8 -*-
###########################################################################
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
###########################################################################
"""
Tests for how read_t_r_file closes a `-- result:` block.

The block opens on the lstripped line, so any indentation starts one. It used to close
only on the line after dedenting by the statement's own indentation, so a terminator
indented MORE than its statement never closed the block: the reader took the rest of the
file as recorded output and swallowed every later `-- name:` with it. These pin both
halves at the same standard.

read_t_r_file needs no cluster, but choose_cases imports cup, nose, timeout_decorator and
lib.sr_sql_lib, and the last pulls in the database drivers. They are stubbed for the
duration of one import and the process is put back as it was found -- the same reason
test_result_format.py imports result_format by path, but this one has a package to go
through, so the stubbing has to be undone rather than just not repeated.
"""

import importlib.util
import os
import sys
import tempfile
import types
import unittest

TEST_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


_MISSING = object()

# Everything the stubbing touches, so the teardown can put each back exactly as it was --
# including not being there at all.
_TOUCHED = ("cup", "cup.log", "timeout_decorator", "nose", "nose.tools",
            "lib", "lib.skip", "lib.sr_sql_lib")


def _build_stubs():
    """The smallest stand-ins read_t_r_file's import chain will accept."""
    cup = types.ModuleType("cup")
    cup.log = types.ModuleType("cup.log")
    for name in ("info", "debug", "warn", "error", "critical"):
        setattr(cup.log, name, lambda *args, **kwargs: None)
    cup.log.init_comlog = lambda *args, **kwargs: None

    timeout_decorator = types.ModuleType("timeout_decorator")
    timeout_decorator.timeout = lambda *args, **kwargs: (lambda fn: fn)
    timeout_decorator.TimeoutError = TimeoutError

    checker = unittest.TestCase("__init__")
    tools = types.ModuleType("nose.tools")
    tools.ok_ = lambda expr, msg=None: checker.assertTrue(expr, msg)
    tools.eq_ = lambda a, b, msg=None: checker.assertEqual(a, b, msg)
    tools.assert_true = lambda a, msg=None: checker.assertTrue(a, msg)
    tools.assert_in = lambda a, b, msg=None: checker.assertIn(a, b, msg)
    tools.assert_less = lambda a, b, msg=None: checker.assertLess(a, b, msg)
    tools.assert_greater = lambda a, b, msg=None: checker.assertGreater(a, b, msg)
    tools.assert_regex = lambda a, b, msg=None: checker.assertRegex(a, b, msg)
    nose = types.ModuleType("nose")
    nose.tools = tools

    sr_sql_lib = types.ModuleType("lib.sr_sql_lib")
    sr_sql_lib.root_path = TEST_ROOT
    sr_sql_lib.StarrocksSQLApiLib = type("StarrocksSQLApiLib", (object,), {})

    return {"cup": cup, "cup.log": cup.log, "timeout_decorator": timeout_decorator,
            "nose": nose, "nose.tools": tools, "lib.sr_sql_lib": sr_sql_lib}


def _load_parser():
    """choose_cases, loaded under a private name with its dependencies stubbed.

    The stubs are installed unconditionally -- binding whatever is already in sys.modules
    would hand choose_cases the real lib.sr_sql_lib and with it the drivers -- and the
    whole lot is reverted in the finally. Without that, this module would leave behind
    either a crippled lib.sr_sql_lib in sys.modules or, if the real one was already
    loaded, a lib.sr_sql_lib *attribute* pointing at the stub: `from lib import
    sr_sql_lib` reads the package attribute before sys.modules, so any later test in the
    process would import the stub and not notice.

    The module is loaded by path as _choose_cases_under_test rather than as
    lib.choose_cases, so the real one can still be imported afterwards. Its globals keep
    the references it bound during exec, so reverting sys.modules does not disturb it.
    """
    saved_modules = {name: sys.modules.get(name, _MISSING) for name in _TOUCHED}
    saved_path = list(sys.path)
    package = None
    saved_attr = _MISSING
    try:
        sys.modules.update(_build_stubs())
        sys.path.insert(0, TEST_ROOT)
        import lib as package

        saved_attr = getattr(package, "sr_sql_lib", _MISSING)
        package.sr_sql_lib = sys.modules["lib.sr_sql_lib"]

        spec = importlib.util.spec_from_file_location(
            "_choose_cases_under_test", os.path.join(TEST_ROOT, "lib", "choose_cases.py"))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        if package is not None:
            if saved_attr is _MISSING:
                if hasattr(package, "sr_sql_lib"):
                    delattr(package, "sr_sql_lib")
            else:
                package.sr_sql_lib = saved_attr
        for name, module in saved_modules.items():
            if module is _MISSING:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module
        sys.path[:] = saved_path


choose_cases = _load_parser()


def parse(content):
    """The (name, statements, results) read_t_r_file takes from this file content.

    `attr` is read from the environment and split unconditionally, so it has to be set
    even when it selects nothing -- and put back, so importing this module cannot change
    how the rest of the process reads it.
    """
    saved_attr = os.environ.get("attr", _MISSING)
    os.environ["attr"] = ""
    directory = tempfile.mkdtemp(dir=TEST_ROOT)
    path = os.path.join(directory, "case")
    with open(path, "w") as handle:
        handle.write(content)
    try:
        case = object.__new__(choose_cases.ChooseCase)
        case.case_list = []
        case.t, case.r = [], []
        case.read_t_r_file(path, None, True)
        return [(each.name, each.sql, each.result) for each in case.case_list]
    finally:
        if saved_attr is _MISSING:
            os.environ.pop("attr", None)
        else:
            os.environ["attr"] = saved_attr
        os.remove(path)
        os.rmdir(directory)


class ResultTerminatorTest(unittest.TestCase):
    def test_terminator_indented_more_than_its_statement(self):
        """The case after it is a case, not the first case's recorded output.

        `select 1;` sits at column 0 and its result block at column 4. Closing on the
        dedented line could not see `-- !result` there, so the block ran to the end of
        the file and `test_second` -- its declaration, its statement and its result --
        all became part of test_first's expectation.
        """
        parsed = parse(
            "-- name: test_first\n"
            "select 1;\n"
            "    -- result:\n"
            "    1\n"
            "    -- !result\n"
            "-- name: test_second\n"
            "select 2;\n"
            "-- result:\n"
            "2\n"
            "-- !result\n"
        )
        self.assertEqual([name for name, _, _ in parsed], ["test_first", "test_second"])
        self.assertEqual(parsed[0][1], ["select 1;"])
        self.assertNotIn("test_second", parsed[0][2][0])

    def test_terminator_indented_less_than_its_statement(self):
        """Already closed before the change: dedenting a shorter prefix is a no-op."""
        parsed = parse(
            "-- name: test_only\n"
            "    select 1;\n"
            "-- result:\n"
            "1\n"
            "-- !result\n"
        )
        self.assertEqual([name for name, _, _ in parsed], ["test_only"])

    def test_aligned_terminator_is_unchanged(self):
        """The shape every recorded file already has."""
        parsed = parse(
            "-- name: test_a\n"
            "select 1;\n"
            "-- result:\n"
            "1\n"
            "-- !result\n"
            "-- name: test_b\n"
            "select 2;\n"
            "-- result:\n"
            "2\n"
            "-- !result\n"
        )
        self.assertEqual([name for name, _, _ in parsed], ["test_a", "test_b"])
        self.assertEqual(parsed[0][2], ["1"])
        self.assertEqual(parsed[1][2], ["2"])

    def test_comment_led_statement_in_a_block(self):
        """SET_VAR skips comments, so the reader enters on the unindented statement.

        The terminator is indented with the rest of the block, which left the block open
        to the end of the file and surfaced as `SET_VAR FORMAT ERROR!` -- an error about
        the block's syntax for a block whose syntax is fine.
        """
        parsed = parse(
            "-- name: test_set_var\n"
            "SET_VAR {\n"
            '    PROPERTY: [{"pipeline_dop": 1}]\n'
            "    -- run it again at the default parallelism\n"
            "select 1;\n"
            "    -- result:\n"
            "    1\n"
            "    -- !result\n"
            "} END SET_VAR\n"
        )
        self.assertEqual([name for name, _, _ in parsed], ["test_set_var"])


class ImportIsolationTest(unittest.TestCase):
    """Importing this module must leave the process able to import the real thing."""

    def test_no_stub_is_left_behind(self):
        """Every stub is a bare ModuleType, so it has neither __file__ nor __path__."""
        for name in _TOUCHED:
            module = sys.modules.get(name)
            if module is None:
                continue
            self.assertTrue(
                getattr(module, "__file__", None) or getattr(module, "__path__", None),
                "%s was left in sys.modules as a stub" % name)

    def test_the_package_attribute_is_not_a_stub(self):
        """`from lib import sr_sql_lib` reads this before sys.modules."""
        package = sys.modules.get("lib")
        if package is None:
            return
        attribute = getattr(package, "sr_sql_lib", None)
        if attribute is None:
            return
        self.assertTrue(getattr(attribute, "__file__", None),
                        "lib.sr_sql_lib is bound to a stub")

    def test_the_parser_does_not_shadow_the_real_module(self):
        """Loading by path must not take lib.choose_cases' place.

        Asserting that entry is absent would make this depend on import order:
        _load_parser leaves a real lib.choose_cases alone, so a process that already had
        one keeps it, and anything may import it between collection and this running.
        What has to hold is that whatever is cached under that name is not the private
        module this file parses with.
        """
        self.assertEqual(choose_cases.__name__, "_choose_cases_under_test")
        self.assertNotIn("_choose_cases_under_test", sys.modules)
        self.assertIsNot(sys.modules.get("lib.choose_cases"), choose_cases)
        package = sys.modules.get("lib")
        if package is not None:
            self.assertIsNot(getattr(package, "choose_cases", None), choose_cases)


if __name__ == "__main__":
    unittest.main(verbosity=2)
