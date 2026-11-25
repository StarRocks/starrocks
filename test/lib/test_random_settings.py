#!/usr/bin/env python
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
Tests for random_settings.

Imported by path rather than as lib.random_settings, because importing the package would pull in
lib/__init__.py and with it cup and timeout_decorator. Nothing here needs a driver or a cluster.
Run with: python3 test/lib/test_random_settings.py
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import random_settings as rs


class PickTest(unittest.TestCase):
    def test_same_seed_and_case_give_the_same_settings(self):
        self.assertEqual(rs.pick(42, "test_a"), rs.pick(42, "test_a"))

    def test_seed_and_case_name_both_change_the_settings(self):
        base = rs.pick(42, "test_a")
        self.assertTrue(any(rs.pick(42, "test_%d" % i) != base for i in range(20)))
        self.assertTrue(any(rs.pick(seed, "test_a") != base for seed in range(1, 21)))

    def test_every_value_comes_from_one_alternative(self):
        allowed = {}
        for alternatives in rs.CHOICES:
            for alternative in alternatives:
                for name, value in alternative.items():
                    allowed.setdefault(name, set()).add(value)
        for seed in range(1, 200):
            for name, value in rs.pick(seed, "test_a").items():
                self.assertIn(value, allowed[name], name)

    def test_spill_is_set_with_its_mode(self):
        for seed in range(1, 200):
            settings = rs.pick(seed, "test_a")
            self.assertEqual("enable_spill" in settings, "spill_mode" in settings)

    def test_each_choice_is_sometimes_left_at_its_default(self):
        picks = [rs.pick(seed, "test_a") for seed in range(1, 400)]
        for alternatives in rs.CHOICES:
            name = next(iter(alternatives[0]))
            self.assertTrue(any(name not in settings for settings in picks), name)
            self.assertTrue(any(name in settings for settings in picks), name)

    def test_cases_waiting_for_the_global_dict_keep_low_cardinality_on(self):
        sqls = ["select 1;", "function: wait_global_dict_ready('c1', 't1')"]
        kept = ("cbo_enable_low_cardinality_optimize", "low_cardinality_optimize_v2")
        picks = [rs.pick(seed, "test_a", sqls) for seed in range(1, 200)]
        self.assertTrue(all(name not in p for p in picks for name in kept))
        # The rest of the settings are the ones the case would get without the wait.
        for seed in range(1, 50):
            full = rs.pick(seed, "test_a")
            for name in kept:
                full.pop(name, None)
            self.assertEqual(full, rs.pick(seed, "test_a", sqls))

    def test_variables_are_unique_across_choices(self):
        names = [name for alternatives in rs.CHOICES for name in alternatives[0]]
        self.assertEqual(len(names), len(set(names)))


class ExemptTest(unittest.TestCase):
    def test_plain_queries_take_random_settings(self):
        sqls = ["CREATE TABLE t (k INT) DUPLICATE KEY(k);", "[ORDER]select * from t order by k;",
                "select 'explain' as s, count(*) from t;"]
        self.assertIsNone(rs.exempt_reason(sqls, []))

    def test_opt_out_tag(self):
        self.assertEqual("@no_random_settings", rs.exempt_reason(["select 1;"], ["no_random_settings"]))

    def test_settings_dependent_statements(self):
        for stmt in ["explain select 1;", "[UC]EXPLAIN COSTS select 1;", "trace times select 1;",
                     "function: print_hit_materialized_view('select 1', 'mv')",
                     "function: assert_explain_costs_contains('select 1', 'x')",
                     "function: assert_query_contains('select 1', 'x')",
                     "select @@pipeline_dop;", "show variables like 'chunk_size';",
                     "select * from information_schema.session_variables;", "analyze profile from '1';"]:
            self.assertIsNotNone(rs.exempt_reason([stmt], []), stmt)

    def test_statements_inside_blocks(self):
        concurrency = {"type": "CONCURRENCY", "thread": [{"cmd": ["select 1;", "explain select 2;"]}]}
        loop = {"type": "LOOP", "stat": ["select @@chunk_size;"]}
        self.assertIsNotNone(rs.exempt_reason(["select 1;", concurrency], []))
        self.assertIsNotNone(rs.exempt_reason([loop], []))
        self.assertIsNone(rs.exempt_reason([{"type": "CLEANUP", "cmd": ["drop table t;"]}], []))


class FormatTest(unittest.TestCase):
    def test_set_statement_and_description(self):
        self.assertEqual("SET spill_mode = 'force';", rs.set_statement("spill_mode", "'force'"))
        settings = {"pipeline_dop": "2", "spill_mode": "'force'"}
        self.assertEqual("pipeline_dop=2, spill_mode='force'", rs.describe(settings))
        self.assertEqual("all defaults", rs.describe({}))

    def test_parse_seed(self):
        self.assertEqual(7, rs.parse_seed("7"))
        self.assertGreater(rs.parse_seed("auto"), 0)
        for bad in ["0", "-1", "x", "1.5", ""]:
            with self.assertRaises(ValueError):
                rs.parse_seed(bad)


if __name__ == "__main__":
    unittest.main()
