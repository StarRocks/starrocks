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

"""Tests for SQL normalization functions."""

from starrocks.common.utils import TableAttributeNormalizer


class TestStripIdentifierBackticks:
    """Test the _strip_identifier_backticks function."""

    def test_simple_backtick_removal(self):
        """Test basic backtick removal."""
        sql = "SELECT `column_name` FROM `table_name`"
        expected = "SELECT column_name FROM table_name"
        result = TableAttributeNormalizer.strip_identifier_backticks(sql)
        assert result == expected

    def test_preserve_string_literals_single_quotes(self):
        """Test that backticks inside single-quoted strings are preserved."""
        sql = "SELECT `id`, 'don\\'t remove `backticks` here' FROM `users`"
        expected = "SELECT id, 'don\\'t remove `backticks` here' FROM users"
        result = TableAttributeNormalizer.strip_identifier_backticks(sql)
        assert result == expected

    def test_preserve_string_literals_double_quotes(self):
        """Test that backticks inside double-quoted strings are preserved."""
        sql = 'SELECT `id`, "keep `backticks` in here" FROM `users`'
        expected = 'SELECT id, "keep `backticks` in here" FROM users'
        result = TableAttributeNormalizer.strip_identifier_backticks(sql)
        assert result == expected

    def test_escaped_quotes_in_strings(self):
        """Test proper handling of escaped quotes within strings."""
        sql = "SELECT `id`, 'it\\'s a `test`' FROM `table`"
        expected = "SELECT id, 'it\\'s a `test`' FROM table"
        result = TableAttributeNormalizer.strip_identifier_backticks(sql)
        assert result == expected

    def test_complex_sql_with_mixed_content(self):
        """Test complex SQL with functions, operators, and mixed quotes."""
        sql = """
        SELECT `u`.`id`, CONCAT('User: `', `u`.`name`, '`') as display_name
        FROM `users` `u`
        WHERE `u`.`status` = 'active'
        """
        expected = """
        SELECT u.id, CONCAT('User: `', u.name, '`') as display_name
        FROM users u
        WHERE u.status = 'active'
        """
        result = TableAttributeNormalizer.strip_identifier_backticks(sql)
        assert result == expected

    def test_no_backticks(self):
        """Test SQL without backticks remains unchanged."""
        sql = "SELECT id, name FROM users WHERE status = 'active'"
        result = TableAttributeNormalizer.strip_identifier_backticks(sql)
        assert result == sql

    def test_empty_string(self):
        """Test empty string handling."""
        result = TableAttributeNormalizer.strip_identifier_backticks("")
        assert result == ""

    def test_only_backticks(self):
        """Test string with only backticks."""
        sql = "````"
        expected = ""
        result = TableAttributeNormalizer.strip_identifier_backticks(sql)
        assert result == expected

    def test_mixed_quote_types(self):
        """Test mixed single and double quotes."""
        sql = '''SELECT `id`, "name with `backticks`", 'status with `backticks`' FROM `users`'''
        expected = '''SELECT id, "name with `backticks`", 'status with `backticks`' FROM users'''
        result = TableAttributeNormalizer.strip_identifier_backticks(sql)
        assert result == expected

    def test_nested_quotes_complex(self):
        """Test complex nested quote scenarios."""
        sql = "SELECT `col1`, 'value with \\'nested\\' and `backticks`' FROM `table`"
        expected = "SELECT col1, 'value with \\'nested\\' and `backticks`' FROM table"
        result = TableAttributeNormalizer.strip_identifier_backticks(sql)
        assert result == expected


class TestNormalizeSQL:
    """Test the normalize_sql function (which uses _strip_identifier_backticks)."""

    def test_full_normalization(self):
        """Test complete SQL normalization."""
        sql = """
        -- This is a comment
        SELECT `u`.`id`,   `u`.`name`
        FROM  `users`   `u`
        WHERE `u`.`status`  =  'active'
        """
        expected = "select u.id, u.name from users u where u.status = 'active'"
        result = TableAttributeNormalizer.normalize_sql(sql)
        assert result == expected

    def test_preserve_backticks_in_strings_during_normalization(self):
        """Test that normalization preserves backticks within string literals."""
        sql = "SELECT `id`, 'keep `these` backticks' FROM `table`"
        expected = "select id, 'keep `these` backticks' from table"
        result = TableAttributeNormalizer.normalize_sql(sql)
        assert result == expected

    def test_comment_removal(self):
        """Test that SQL comments are properly removed."""
        sql = """
        SELECT `id` -- user identifier
        FROM `users` -- main table
        """
        expected = "select id from users"
        result = TableAttributeNormalizer.normalize_sql(sql)
        assert result == expected

    def test_whitespace_normalization(self):
        """Test whitespace collapse and trimming."""
        sql = "  SELECT   `id`  ,  `name`   FROM   `users`  "
        expected = "select id, name from users"
        result = TableAttributeNormalizer.normalize_sql(sql)
        assert result == expected

    def test_none_input(self):
        """Test None input handling."""
        result = TableAttributeNormalizer.normalize_sql(None)
        assert result is None

    def test_empty_string_normalization(self):
        """Test empty string normalization."""
        result = TableAttributeNormalizer.normalize_sql("")
        assert result == ""

    def test_case_conversion(self):
        """Test case conversion to lowercase."""
        sql = "SELECT `ID`, `NAME` FROM `USERS` WHERE `STATUS` = 'ACTIVE'"
        expected = "select id, name from users where status = 'active'"
        result = TableAttributeNormalizer.normalize_sql(sql)
        assert result == expected


class TestStarRocksSpecificScenarios:
    """Test scenarios specific to StarRocks SQL patterns."""

    def test_distribution_clause(self):
        """Test DISTRIBUTED BY clause normalization."""
        sql = "DISTRIBUTED BY HASH(`user_id`) BUCKETS 10"
        expected = "distributed by hash(user_id) buckets 10"
        result = TableAttributeNormalizer.normalize_sql(sql)
        assert result == expected

    def test_partition_clause(self):
        """Test PARTITION BY clause normalization."""
        sql = "PARTITION BY RANGE(`date_col`) (PARTITION p1 VALUES [('2023-01-01'), ('2023-02-01')))"
        expected = "partition by range(date_col) (partition p1 values [('2023-01-01'), ('2023-02-01')))"
        result = TableAttributeNormalizer.normalize_sql(sql)
        assert result == expected

    def test_properties_clause(self):
        """Test PROPERTIES clause with quoted values."""
        sql = '''PROPERTIES ("replication_num" = "3", "storage_medium" = "SSD")'''
        expected = '''properties ("replication_num" = "3", "storage_medium" = "ssd")'''
        result = TableAttributeNormalizer.normalize_sql(sql)
        assert result == expected

    def test_view_definition_with_backticks(self):
        """Test view definition normalization (typical use case)."""
        sql = """
        SELECT
            `t1`.`id`,
            `t1`.`name`,
            `t2`.`category`
        FROM `table1` `t1`
        JOIN `table2` `t2` ON `t1`.`id` = `t2`.`table1_id`
        WHERE `t1`.`status` = 'active'
        """
        expected = "select t1.id, t1.name, t2.category from table1 t1 join table2 t2 on t1.id = t2.table1_id where t1.status = 'active'"
        result = TableAttributeNormalizer.normalize_sql(sql)
        assert result == expected


class TestRealCases():
    """Test real cases."""

    def test_real_case_01(self):
        """Test real case 0."""
        sql = "users.id"
        expected = "id"
        result = TableAttributeNormalizer.normalize_sql(sql, remove_qualifiers=True)
        assert result == expected

    def test_real_case_02(self):
        """Test real case 0."""
        sql = "`users`.`id`"
        expected = "id"
        result = TableAttributeNormalizer.normalize_sql(sql, remove_qualifiers=True)
        assert result == expected

    def test_real_case_1(self):
        """Test real case 1."""
        sql = "select users.id, users.name from test_sqla.users where users.active = true"
        expected = "select id, name from users where active = true"
        result = TableAttributeNormalizer.normalize_sql(sql, remove_qualifiers=True)
        assert result == expected

    def test_real_case_2_with_backticks(self):
        """Test real case 2 with backticks."""
        sql = "SELECT `users`.`id`, `users`.`name` FROM `test_sqla`.`users` WHERE `users`.`active` = TRUE"
        expected = "select id, name from users where active = true"
        result = TableAttributeNormalizer.normalize_sql(sql, remove_qualifiers=True)
        assert result == expected

    def test_real_case_3_with_liens(self):
        """Test real case 3."""
        sql = """
        SELECT `users`.`id`, `users`.`name`
        FROM `test_sqla`.`users`
        WHERE `users`.`active` = TRUE
        """
        expected = "select id, name from users where active = true"
        result = TableAttributeNormalizer.normalize_sql(sql, remove_qualifiers=True)
        assert result == expected

    def test_real_case_4_with_special_char_in_backticks(self):
        """Test real case 4 with qualifiers."""
        sql = "select `schema name`.`table@name@x`.`column x`"
        expected = "select column x"
        result = TableAttributeNormalizer.normalize_sql(sql, remove_qualifiers=True)
        assert result == expected

    def test_real_case_5_with_qualifiers(self):
        """Test real case 5 with special char in backticks."""
        sql = "select orders_part_expr.user_id, orders_part_expr.order_date, count(*) as cnt from test_sqla.orders_part_expr group by orders_part_expr.user_id, orders_part_expr.order_date"
        expected = "select user_id, order_date, count(*) cnt from orders_part_expr group by user_id, order_date"
        result = TableAttributeNormalizer.normalize_sql(sql, remove_qualifiers=True)
        assert result == expected

    def test_real_case_6_with_qualifiers_and_backticks(self):
        """Test real case 6 with special char in backticks."""
        sql = """SELECT `orders_part_expr`.`user_id`, `orders_part_expr`.`order_date`, count(*) AS `cnt`
            FROM `test_sqla`.`orders_part_expr`
            GROUP BY `orders_part_expr`.`user_id`, `orders_part_expr`.`order_date`"""
        expected = "select user_id, order_date, count(*) cnt from orders_part_expr group by user_id, order_date"
        result = TableAttributeNormalizer.normalize_sql(sql, remove_qualifiers=True)
        assert result == expected


class TestStarRocksCanonicalization:
    """Tests for canonicalizing StarRocks' own rewrites of view/MV definitions.

    On clusters older than 4.0.6, StarRocks stores a view definition in a canonical
    form that is semantically equal but syntactically different from the user-written
    SQL. normalize_sql must reconcile both into a common form so equivalent definitions
    compare equal. Each test asserts the *model* SQL normalizes to the same string as
    the *stored* SQL.
    """

    def _assert_equivalent(self, model_sql: str, stored_sql: str) -> None:
        model = TableAttributeNormalizer.normalize_sql(model_sql, remove_qualifiers=True)
        stored = TableAttributeNormalizer.normalize_sql(stored_sql, remove_qualifiers=True)
        assert model == stored, f"\n  model : {model}\n  stored: {stored}"

    def test_full_repro_unnest_table_alias(self):
        """The exact reproduction from the bug report (unnest + table alias + comma)."""
        model = (
            "SELECT `tenant_id`, `id` AS `ibg_id`, `ks_id` AS `keyword_set_id`\n"
            "FROM `tenant`.`ideal_buying_group`,\n"
            "unnest(`keyword_set_ids`) AS k(`ks_id`)"
        )
        stored = (
            "SELECT `tenant`.`ideal_buying_group`.`tenant_id`, "
            "`tenant`.`ideal_buying_group`.`id` AS `ibg_id`, "
            "`k`.`ks_id` AS `keyword_set_id` "
            "FROM `tenant`.`ideal_buying_group` , "
            "unnest(`tenant`.`ideal_buying_group`.`keyword_set_ids`) k(`ks_id`) ;"
        )
        self._assert_equivalent(model, stored)

    def test_inner_join_added(self):
        """StarRocks rewrites 'JOIN' to 'INNER JOIN'."""
        self._assert_equivalent(
            "select a, b from t join u on t.id = u.id",
            "select a, b from t inner join u on t.id = u.id",
        )

    def test_outer_join_added(self):
        """StarRocks rewrites 'LEFT JOIN' to 'LEFT OUTER JOIN'."""
        self._assert_equivalent(
            "select a from t left join u on t.id = u.id",
            "select a from t left outer join u on t.id = u.id",
        )

    def test_lateral_added_before_unnest(self):
        """StarRocks may add LATERAL and drop the table-function alias AS."""
        self._assert_equivalent(
            "select x from t, unnest(arr) as u(v)",
            "select x from t, lateral unnest(arr) u(v)",
        )

    def test_case_when_predicate_parenthesized(self):
        """StarRocks wraps the CASE WHEN predicate in redundant parentheses."""
        self._assert_equivalent(
            "select case when x > 100 then 1 else 0 end as c from t",
            "select case when (x > 100) then 1 else 0 end as c from t",
        )

    def test_where_predicate_parenthesized(self):
        """Redundant parentheses around a WHERE comparison are removed."""
        self._assert_equivalent(
            "select x from t where status = 'active'",
            "select x from t where (status = 'active')",
        )

    def test_multiple_predicate_parens(self):
        """Each flat comparison's redundant parentheses are removed independently."""
        self._assert_equivalent(
            "select x from t where a > 1 and b < 2",
            "select x from t where (a > 1) and (b < 2)",
        )

    def test_cte_column_list_not_stripped_in_fallback(self):
        """The regex fallback does NOT strip StarRocks-injected CTE column lists.

        Stripping the column list wholesale would cause definitions with different column
        lists (e.g. WITH c(a) vs WITH c(b)) to normalize to the same string, silently
        missing a real schema change. The trade-off is that the fallback path may emit a
        phantom diff when StarRocks injects a column list the user did not write; that case
        is handled correctly by the preferred temp-view path.
        """
        model = TableAttributeNormalizer.normalize_sql(
            "with cat as (select category, c from t) select category from cat"
        )
        stored = TableAttributeNormalizer.normalize_sql(
            "with cat (category, c) as (select category, c from t) select category from cat"
        )
        assert model != stored

    def test_cte_different_column_lists_detected(self):
        """Two definitions that differ only in CTE column names are not equal after normalization."""
        a = TableAttributeNormalizer.normalize_sql(
            "with c(a) as (select 1) select * from c"
        )
        b = TableAttributeNormalizer.normalize_sql(
            "with c(b) as (select 1) select * from c"
        )
        assert a != b

    def test_comma_spacing_normalized(self):
        """Comma spacing differences are normalized."""
        self._assert_equivalent(
            "select a,b,c from t",
            "select a , b , c from t",
        )

    def test_newline_differences_normalized(self):
        """Literal '\\n' and actual newlines normalize identically."""
        self._assert_equivalent(
            "select a,\nb from t",
            "select a,\\nb from t",
        )

    # --- Safety guards: real differences must still be detected ------------------

    def test_real_column_change_still_detected(self):
        model = TableAttributeNormalizer.normalize_sql("select a from t", remove_qualifiers=True)
        stored = TableAttributeNormalizer.normalize_sql("select b from t", remove_qualifiers=True)
        assert model != stored

    def test_grouping_parens_not_erased(self):
        """Parentheses that change AND/OR grouping must be preserved."""
        model = TableAttributeNormalizer.normalize_sql(
            "select x from t where (a = 1 or b = 2) and c = 3", remove_qualifiers=True)
        stored = TableAttributeNormalizer.normalize_sql(
            "select x from t where a = 1 or b = 2 and c = 3", remove_qualifiers=True)
        assert model != stored

    def test_alias_as_dropped_symmetrically(self):
        """The AS keyword is dropped uniformly; with-AS and without-AS forms compare equal."""
        self._assert_equivalent(
            "select x y, t.* from src t",
            "select x as y, t.* from src as t",
        )

    def test_table_alias_as_added_by_starrocks(self):
        """StarRocks 3.5.x stores table aliases with AS ('src AS a'); the model omits it."""
        self._assert_equivalent(
            "select a.id from src a join src b on a.id = b.id",
            "select a.id from src as a inner join src as b on a.id = b.id",
        )

    def test_cast_as_normalized_symmetrically(self):
        """CAST(... AS type) loses its AS too, but symmetrically, so it stays comparable."""
        self._assert_equivalent(
            "select cast(x as int) y from t",
            "select cast(x as int) as y from t",
        )

    def test_function_call_parens_preserved(self):
        """Parentheses of a function call must not be stripped even with '=' inside."""
        result = TableAttributeNormalizer.normalize_sql("select array_map(x -> x = 1, arr) from t")
        assert "array_map(x -> x = 1, arr)" in result


    def test_alias_as_not_fired_inside_string_literal(self):
        """_ALIAS_AS_PATTERN skips single- and double-quoted strings, so ' as ' inside a
        quoted value is preserved unchanged.
        """
        sql = "select id from t where status = 'pending as review'"
        result = TableAttributeNormalizer.normalize_sql(sql)
        assert "pending as review" in result

    def test_alias_as_not_fired_inside_double_quoted_string(self):
        """Double-quoted string literals are also skipped by _ALIAS_AS_PATTERN."""
        sql = 'select id from t where label = "listed as active"'
        result = TableAttributeNormalizer.normalize_sql(sql)
        assert "listed as active" in result

    def test_alias_as_still_fires_outside_strings(self):
        """AS is still removed for aliases that appear outside any string literal."""
        self._assert_equivalent(
            "select x y, t.* from src t",
            "select x as y, t.* from src as t",
        )
