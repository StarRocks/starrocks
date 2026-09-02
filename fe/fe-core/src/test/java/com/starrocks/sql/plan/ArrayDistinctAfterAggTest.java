// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.plan;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ArrayDistinctAfterAggTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @Test
    public void testArrayDistinctAfterAgg() throws Exception {
        String sql = "select array_distinct(array_agg(v2)) from t0 group by v1";
        String sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "array_agg_distinct");

        sql = "select array_length(array_distinct(array_agg(v2))) from t0 group by v1";
        sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "array_agg_distinct");
    }

    @Test
    public void testArrayDistinctAfterAggWithPredicate() throws Exception {
        String sql = "select array_distinct(array_agg(v2)) from t0 group by v1 having " +
                "array_length(array_distinct(array_agg(v2))) > 1";
        String sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "array_agg_distinct");

        sql = "select array_length(array_distinct(array_agg(v2))) from t0 group by v1 having " +
                "array_length(array_distinct(array_agg(v2))) > 1";
        sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "array_agg_distinct");

        sql = "select array_distinct(array_agg(v2)) from t0 group by v1 having array_length(array_agg(v2)) > 1";
        sqlPlan = getFragmentPlan(sql);
        assertNotContains(sqlPlan, "array_agg_distinct");
    }

    /**
     * array_agg_distinct is registered for numeric, string, date and time only -- see
     * FunctionSet.registerBuiltinArrayAggDistinctFunction. The rule already handles "no such overload"
     * by leaving the aggregation alone, but the lookup uses IS_NONSTRICT_SUPERTYPE_OF, which accepts a
     * match reached through an implicit cast. JSON casts to BOOLEAN, so the lookup returned the BOOLEAN
     * overload instead of null: the rule then built a CallOperator returning ARRAY<BOOLEAN> while the
     * ColumnRefOperator it rewrote kept the original ARRAY<JSON>, and the plan failed validation with
     *
     *   Invalid plan: ... the type of arg N: array_agg_distinct is defined as ARRAY&lt;JSON&gt;,
     *   but the actual type is ARRAY&lt;BOOLEAN&gt;
     *
     * Declining the rewrite is not merely a way to avoid the crash: array_distinct answers correctly
     * for JSON. It buckets on the serialized bytes and falls back to JSON's own comparison on a hash
     * collision, and velocypack normalizes member order and numeric form as parse_json builds a value.
     */
    @Test
    public void testSkipRewriteWhenTheOverloadWouldChangeTheResultType() throws Exception {
        String sql = "select array_distinct(array_agg(v_json)) from tjson group by v_int";
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "array_agg_distinct");
        assertCContains(plan, "array_agg");

        sql = "select array_length(array_distinct(array_agg(v_json))) from tjson group by v_int";
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "array_agg_distinct");

        // The predicate path goes through the same rewrite.
        sql = "select array_distinct(array_agg(v_json)) from tjson group by v_int having "
                + "array_length(array_distinct(array_agg(v_json))) > 1";
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "array_agg_distinct");

        // A type array_agg_distinct really does support still gets the rewrite. Grouping by the JSON
        // column is not an option here -- JSON cannot be a group-by key at all -- so this control uses
        // the same shape the tests above it use.
        sql = "select array_distinct(array_agg(v_int)) from tjson group by v_int";
        plan = getFragmentPlan(sql);
        assertCContains(plan, "array_agg_distinct");
    }

    /**
     * The return-type check guarding the rewrite above must stay narrow: every type
     * array_agg_distinct is actually registered for resolves to an overload returning the same
     * ARRAY<T> array_agg returns, so none of them lose the optimization. Type.matchesType is what
     * keeps this true for the near misses -- it ignores nullability, string length and decimal
     * precision, and only decimal scale has to agree.
     */
    @Test
    public void testRewriteStillAppliesToEverySupportedArgumentType() throws Exception {
        String[] columns = {"t1a", "t1b", "t1c", "t1d", "t1e", "t1f", "id_datetime", "id_date", "id_decimal"};
        for (String column : columns) {
            String sql = String.format("select array_distinct(array_agg(%s)) from test_all_type group by t1b", column);
            assertCContains(getFragmentPlan(sql), "array_agg_distinct");
        }

        String sql = "select array_distinct(array_agg(id_bool)) from test_bool group by t1b";
        assertCContains(getFragmentPlan(sql), "array_agg_distinct");
    }

}
