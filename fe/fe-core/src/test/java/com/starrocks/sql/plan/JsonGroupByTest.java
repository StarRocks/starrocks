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

import com.starrocks.type.Type;
import org.junit.jupiter.api.Test;

public class JsonGroupByTest extends PlanTestBase {

    // ---- positive: top-level JSON is groupable ----

    @Test
    public void testGroupByTopLevelJson() throws Exception {
        String plan = getFragmentPlan("select v_json, count(*) from tjson group by v_json");
        assertContains(plan, "AGGREGATE");
    }

    @Test
    public void testCountDistinctJsonPlansWithoutError() throws Exception {
        String plan = getFragmentPlan("select count(distinct v_json) from tjson");
        assertContains(plan, "AGGREGATE");
    }

    @Test
    public void testGroupByJsonExpression() throws Exception {
        String plan = getFragmentPlan("select count(*) from tjson group by json_query(v_json, '$.k')");
        assertContains(plan, "AGGREGATE");
    }

    @Test
    public void testGroupByJsonInDerivedTable() throws Exception {
        String plan = getFragmentPlan(
                "select count(*) from (select v_json, count(*) c from tjson group by v_json) s");
        assertContains(plan, "AGGREGATE");
    }

    // ---- negative: nested JSON stays rejected ----

    @Test
    public void testGroupByNestedJsonArrayRejected() {
        starRocksAssert.query("select count(*) from tjson group by [v_json]")
                .analysisError(Type.NOT_SUPPORT_GROUP_BY_ERROR_MSG);
    }

    @Test
    public void testGroupByNestedJsonMapRejected() {
        starRocksAssert.query("select count(*) from tjson group by map{'k': v_json}")
                .analysisError(Type.NOT_SUPPORT_GROUP_BY_ERROR_MSG);
    }

    @Test
    public void testGroupByNestedJsonStructRejected() {
        starRocksAssert.query("select count(*) from tjson group by named_struct('a', v_json)")
                .analysisError(Type.NOT_SUPPORT_GROUP_BY_ERROR_MSG);
    }

    // ---- scope boundary: DISTINCT/ORDER BY on JSON stay rejected ----

    @Test
    public void testSelectDistinctJsonStillRejected() {
        starRocksAssert.query("select distinct v_json from tjson")
                .analysisError("DISTINCT can only be applied to comparable types");
    }

    @Test
    public void testOrderByJsonStillRejected() {
        starRocksAssert.query("select v_json from tjson group by v_json order by v_json")
                .analysisError(Type.NOT_SUPPORT_ORDER_ERROR_MSG);
    }

    // ---- grouping sets: top-level JSON allowed ----

    @Test
    public void testGroupingSetsTopLevelJson() throws Exception {
        String plan = getFragmentPlan(
                "select v_json, count(*) from tjson group by grouping sets((v_json))");
        assertContains(plan, "AGGREGATE");
    }

    // ---- grouping sets / rollup: nested JSON rejected (the hardening) ----

    @Test
    public void testGroupingSetsNestedJsonRejected() {
        starRocksAssert.query("select count(*) from tjson group by grouping sets(([v_json]))")
                .analysisError(Type.NOT_SUPPORT_GROUP_BY_ERROR_MSG);
    }

    @Test
    public void testRollupNestedJsonRejected() {
        starRocksAssert.query("select count(*) from tjson group by rollup([v_json])")
                .analysisError(Type.NOT_SUPPORT_GROUP_BY_ERROR_MSG);
    }

    // ---- grouping sets: metric type rejected (the pre-existing gap this fixes) ----

    @Test
    public void testGroupingSetsBitmapRejected() {
        starRocksAssert.query(
                "select count(*) from test.bitmap_table group by grouping sets((id2))")
                .analysisError(Type.NOT_SUPPORT_GROUP_BY_ERROR_MSG);
    }
}
