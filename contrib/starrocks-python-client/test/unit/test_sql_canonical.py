# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for AST-based SQL canonicalization (starrocks.common.sql_canonical)."""

import pytest

from starrocks.common.sql_canonical import canonicalize_sql, is_available


pytestmark = pytest.mark.skipif(not is_available(), reason="sqlglot not installed")


def _equiv(a: str, b: str, remove_qualifiers: bool = False) -> None:
    ca = canonicalize_sql(a, remove_qualifiers=remove_qualifiers)
    cb = canonicalize_sql(b, remove_qualifiers=remove_qualifiers)
    assert ca is not None and cb is not None
    assert ca == cb, f"{a!r} -> {ca!r} != {b!r} -> {cb!r}"


def _distinct(a: str, b: str, remove_qualifiers: bool = False) -> None:
    ca = canonicalize_sql(a, remove_qualifiers=remove_qualifiers)
    cb = canonicalize_sql(b, remove_qualifiers=remove_qualifiers)
    assert ca is not None and cb is not None
    assert ca != cb, f"{a!r} and {b!r} both -> {ca!r} (should differ)"


class TestSemanticEquivalence:
    """Syntactic variation StarRocks introduces must canonicalize to the same string."""

    def test_inner_join_keyword_optional(self):
        _equiv("select a from t join u on t.id = u.id",
               "select a from t inner join u on t.id = u.id")

    def test_outer_join_keyword_optional(self):
        _equiv("select a from t left join u on t.id = u.id",
               "select a from t left outer join u on t.id = u.id")

    def test_lateral_unnest_equivalent_to_comma(self):
        _equiv("select x from t, unnest(arr) as u(v)",
               "select x from t, lateral unnest(arr) u(v)")

    def test_optional_as_alias(self):
        _equiv("select a as x from t", "select a x from t")

    def test_redundant_where_paren(self):
        _equiv("select x from t where x > 100", "select x from t where (x > 100)")

    def test_redundant_case_paren(self):
        _equiv("select case when x > 100 then 1 else 0 end as c from t",
               "select case when (x > 100) then 1 else 0 end as c from t")

    def test_identifier_quoting_and_case(self):
        _equiv("select `A` from `T`", "select a from t")

    def test_keyword_and_identifier_case(self):
        _equiv("SELECT A FROM T", "select a from t")

    def test_catalog_db_qualifier_removed_when_requested(self):
        # StarRocks stores `db`.`t`.`col`; the model writes `t`.`col` -> must reconcile.
        _equiv("select db.t.a from db.t", "select t.a from t", remove_qualifiers=True)

    def test_three_part_alias_qualifier_matches_two_part(self):
        # StarRocks db-qualifies even join aliases (`db`.`a`.`id`) -> must reconcile.
        _equiv(
            "select db.a.id as x from db.t1 a join db.t2 b on db.a.id = db.b.id",
            "select a.id as x from t1 a join t2 b on a.id = b.id",
            remove_qualifiers=True,
        )


class TestRealChangesDetected:
    """Genuine definition changes must NOT collapse to the same string."""

    def test_arithmetic_paren_is_significant(self):
        # (a > 0) + b  !=  a > 0 + b  because '+' binds tighter than '>'.
        # This is the regex normalizer's precedence bug; the AST path gets it right.
        _distinct("select (a > 0) + b from t", "select a > 0 + b from t")

    def test_lateral_as_column_identifier(self):
        _distinct("select `lateral` as x from t", "select x from t")

    def test_comma_inside_string_literal(self):
        _distinct("select 'a,b' as c from t", "select 'a, b' as c from t")

    def test_comment_marker_inside_string_literal(self):
        _distinct("select 'x -- a' as c from t", "select 'x -- b' as c from t")

    def test_whitespace_inside_string_literal(self):
        _distinct("select 'a   b' as c from t", "select 'a b' as c from t")

    def test_and_or_grouping_preserved(self):
        _distinct("select x from t where (a = 1 or b = 2) and c = 3",
                  "select x from t where a = 1 or b = 2 and c = 3")

    def test_different_column(self):
        _distinct("select a from t", "select b from t")

    def test_left_join_not_equal_inner_join(self):
        _distinct("select a from t left join u on t.id = u.id",
                  "select a from t inner join u on t.id = u.id")

    def test_qualifiers_kept_distinct_columns(self):
        _distinct("select t.a from t", "select t.b from t", remove_qualifiers=True)

    def test_table_alias_qualifier_not_collapsed(self):
        # Regression: over-stripping turned `a.id` and `b.id` both into `id`, collapsing
        # two different join conditions -> silent missed migration.
        _distinct(
            "select x from t1 a join t2 b on a.id = 1",
            "select x from t1 a join t2 b on b.id = 1",
            remove_qualifiers=True,
        )

    def test_bare_column_differs_from_table_qualified(self):
        # sqlglot can't resolve bare `id` to its table without a schema, so a safe
        # spurious diff is preferred over a silent false match.
        _distinct("select id from t", "select t.id from t", remove_qualifiers=True)


class TestFallbackSignalling:
    """canonicalize_sql returns None (caller falls back to regex) on unusable input."""

    def test_none_input(self):
        assert canonicalize_sql(None) is None

    def test_empty_input(self):
        assert canonicalize_sql("") is None
        assert canonicalize_sql("   ") is None

    def test_unparseable_input_returns_none(self):
        assert canonicalize_sql("this (( is not )) valid sql ,,,") is None
