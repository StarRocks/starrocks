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
Random, result-invariant session variables for `run.py --random-settings=SEED`.

Every variable here changes how a query runs -- parallelism, spilling, caching, runtime filters,
plan shape -- but must not change what it returns. A run picks the values for each case from the
seed and the case name, so a failing case replays with the same seed and `--case_filter`, in any
order and at any concurrency. A failure under random settings is either a product bug or a case
whose output depends on the settings; the latter gets the `@no_random_settings` tag.

Imported by path in its tests, so it must not import anything outside the standard library.
"""

import hashlib
import random
import re

OPT_OUT_TAG = "no_random_settings"


def _bool(name):
    return [{name: "true"}, {name: "false"}]


def _values(name, *values):
    return [{name: value} for value in values]


# One entry per independent choice. Each alternative is the set of variables SET together; leaving
# everything at its default is always one more alternative, so a run keeps covering the default
# path as well. Values are SQL literals.
CHOICES = [
    # parallelism and execution
    _values("pipeline_dop", "1", "2", "4"),
    _values("chunk_size", "1024", "2048"),
    _values("io_tasks_per_scan_operator", "1", "8"),
    [{"enable_spill": "true", "spill_mode": "'force'"}],
    [{"enable_query_cache": "true"}],
    _bool("enable_group_execution"),
    _bool("enable_per_bucket_optimize"),
    _bool("enable_local_shuffle_agg"),
    _bool("enable_shared_scan"),
    _bool("enable_sort_aggregate"),
    _values("streaming_preaggregation_mode", "'force_preaggregation'", "'force_streaming'", "'auto'"),
    _bool("enable_hash_join_range_direct_mapping_opt"),
    # runtime filters
    _bool("enable_global_runtime_filter"),
    _bool("runtime_filter_on_exchange_node"),
    _bool("enable_join_runtime_filter_push_down"),
    # low-cardinality dictionary optimization
    _bool("cbo_enable_low_cardinality_optimize"),
    _bool("low_cardinality_optimize_v2"),
    # optimizer rewrites
    _values("new_planner_agg_stage", "1", "2", "3", "4"),
    _values("cbo_push_down_aggregate_mode", "-1", "0", "1", "2", "3"),
    _bool("enable_rewrite_simple_agg_to_meta_scan"),
    _bool("enable_eliminate_agg"),
    _bool("enable_predicate_reorder"),
    _bool("cbo_derive_range_join_predicate"),
    # Not cbo_cte_reuse: with it off, count(distinct a, b) next to count(distinct b, c) is an error.
    _bool("enable_rewrite_groupingsets_to_union_all"),
    _bool("cbo_push_down_distinct_below_window"),
    _bool("enable_lambda_pushdown"),
    _bool("enable_partition_column_value_only_optimization"),
    # join reorder
    _bool("cbo_enable_greedy_join_reorder"),
    _bool("cbo_enable_dp_join_reorder"),
    _values("cbo_max_reorder_node_use_dp", "0", "4", "10"),
]

# A case stays at the default settings when its output shows them: plans, profiles, traces and the
# variables themselves. Matched against every statement of the case, after its leading [UC] /
# [ORDER] / [UNORDERED] / [REGEX] flags and its `name=` variable assignment.
_STATEMENT_PREFIX = re.compile(r"^\s*(\[[A-Z]+\]\s*)*([a-zA-Z0-9_-]+=)?\s*")
_SETTINGS_DEPENDENT = re.compile(
    r"^(explain|trace)\b"
    # The function: checks that read a plan or a profile.
    r"|^function:\s*(print_hit_materialized_views?|check_(no_)?hit_materialized_view|assert_explain\w*"
    r"|assert_query_(not_)?contains\w*|get_col_stats_from_explain\w*|assert_trace\w*"
    r"|assert_is_identical_explain_plan|assert_paimon\w*|wait_plan_contains|print_plan_partition_selected_num"
    r"|get_query_stats_source|wait_compaction_finish)\b"
    r"|\banalyze\s+profile\b|\bget_query_profile\s*\(|\bshow\s+(variables|profilelist)\b|@@"
    r"|\b(session|global)_variables\b",
    re.IGNORECASE,
)


# Variables a case keeps at the default, while the rest are still random, because the case waits for
# something they turn off.
_KEEP_DEFAULT = [
    # These wait for or check the low-cardinality dictionary, which is only collected with the
    # optimization on; the dictionary of an ARRAY<STRING> column also needs its v2.
    (
        re.compile(r"^function:\s*(wait_global_dict_ready|try_collect_dict_N_times|assert_has_global_dict)\b",
                   re.IGNORECASE),
        {"cbo_enable_low_cardinality_optimize", "low_cardinality_optimize_v2"},
    ),
]


def _statements(sql_list):
    """Every statement of a case, including the ones inside LOOP, CONCURRENCY, CLEANUP and SET_VAR blocks."""
    for sql in sql_list:
        if isinstance(sql, str):
            yield sql
        elif isinstance(sql, dict):
            yield from sql.get("stat", [])
            yield from sql.get("cmd", [])
            for thread in sql.get("thread", []):
                yield from thread.get("cmd", [])


def exempt_reason(sql_list, tags):
    """Why a case keeps the default settings, or None when it can take random ones."""
    if OPT_OUT_TAG in tags:
        return "@" + OPT_OUT_TAG
    for stmt in _statements(sql_list):
        match = _SETTINGS_DEPENDENT.search(_STATEMENT_PREFIX.sub("", stmt, count=1))
        if match:
            return "output depends on settings: " + match.group(0).strip()
    return None


def keep_default(sql_list):
    """The variables this case must leave at the default."""
    names = set()
    for stmt in _statements(sql_list):
        stmt = _STATEMENT_PREFIX.sub("", stmt, count=1)
        for pattern, variables in _KEEP_DEFAULT:
            if pattern.search(stmt):
                names |= variables
    return names


def pick(seed, case_name, sql_list=()):
    """
    The settings of one case, as an ordered {variable: SQL literal}. Same seed and case name, same settings;
    the variables the case's statements need at the default are left out.
    """
    digest = hashlib.sha256(("%s:%s" % (seed, case_name)).encode("utf-8")).digest()
    rng = random.Random(int.from_bytes(digest[:8], "big"))
    settings = {}
    for alternatives in CHOICES:
        index = rng.randrange(len(alternatives) + 1)
        if index < len(alternatives):
            settings.update(alternatives[index])
    excluded = keep_default(sql_list)
    return {name: value for name, value in settings.items() if name not in excluded}


def set_statement(name, value):
    return "SET %s = %s;" % (name, value)


def describe(settings):
    return ", ".join("%s=%s" % (name, value) for name, value in settings.items()) or "all defaults"


def parse_seed(arg):
    """The seed of `--random-settings=ARG`: a positive integer, or a new one for `auto`."""
    if arg == "auto":
        return random.SystemRandom().randrange(1, 2**31)
    seed = int(arg) if arg.isdigit() else 0
    if seed <= 0:
        raise ValueError("seed must be a positive integer or auto: %s" % arg)
    return seed
