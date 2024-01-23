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

import org.junit.BeforeClass;
import org.junit.Test;

public class ArrayDistinctAfterAggTest extends PlanTestBase {
    @BeforeClass
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
}
