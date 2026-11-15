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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

// A filter above an ASOF join applies to the match the join picked for each left row, so a predicate on the right
// side must stay above the join: filtering the right input first makes the join pick another (earlier) row.
public class AsofJoinFilterPushDownTest extends PlanTestBase {

    @Test
    public void testRightSideFilterStaysAboveAsofInnerJoin() throws Exception {
        String plan = getLogicalFragmentPlan(
                "select * from t2 asof join t0 r on t2.v7 = r.v1 and t2.v8 >= r.v2 where r.v3 > 0");
        assertContains(plan, "ASOF INNER JOIN (join-predicate [1: v7 = 4: v1 AND 2: v8 >= 5: v2] " +
                "post-join-predicate [6: v3 > 0])");
        assertContains(plan, "SCAN (columns[4: v1, 5: v2, 6: v3] predicate[4: v1 IS NOT NULL])");
    }

    @Test
    public void testRightSideFilterStaysAboveAsofLeftJoin() throws Exception {
        // the NULL rejection turns the ASOF LEFT JOIN into an inner one; the filter still stays above it
        String plan = getLogicalFragmentPlan(
                "select * from t2 asof left join t0 r on t2.v7 = r.v1 and t2.v8 >= r.v2 where r.v3 > 0");
        assertContains(plan, "post-join-predicate [6: v3 > 0])");
        assertContains(plan, "SCAN (columns[4: v1, 5: v2, 6: v3] predicate[4: v1 IS NOT NULL])");
    }

    @Test
    public void testLeftSideAndKeyFiltersAreStillPushedDown() throws Exception {
        String plan = getLogicalFragmentPlan(
                "select * from t2 asof join t0 r on t2.v7 = r.v1 and t2.v8 >= r.v2 where t2.v9 > 0 and r.v1 > 3");
        // a filter on the equality key drops whole key groups, so it goes down on both sides
        assertContains(plan, "SCAN (columns[1: v7, 2: v8, 3: v9] predicate[1: v7 IS NOT NULL AND 3: v9 > 0 " +
                "AND 1: v7 > 3])");
        assertContains(plan, "SCAN (columns[4: v1, 5: v2, 6: v3] predicate[4: v1 IS NOT NULL AND 4: v1 > 3])");
        assertContains(plan, "post-join-predicate [null])");
    }

    @Test
    public void testFilterInsideTheRightSideIsPushedDown() throws Exception {
        String plan = getLogicalFragmentPlan(
                "select * from t2 asof join (select * from t0 where v3 > 0) r on t2.v7 = r.v1 and t2.v8 >= r.v2");
        assertContains(plan, "post-join-predicate [null])");
        assertContains(plan, "predicate[4: v1 IS NOT NULL AND 6: v3 > 0]");
    }

    @Test
    public void testNullRejectionAboveKeepsOuterJoinInsideTheRightSide() throws Exception {
        String sql = "select * from t2 asof join (select t0.v1, t0.v2, t1.v5 from t0 left join t1 on t0.v1 = t1.v4) r " +
                "on t2.v7 = r.v1 and t2.v8 >= r.v2 join t3 on r.v5 = t3.v10";
        String plan = getLogicalFragmentPlan(sql);
        assertContains(plan, "LEFT OUTER JOIN (join-predicate [4: v1 = 7: v4]");
        assertContains(plan, "post-join-predicate [8: v5 IS NOT NULL])");
    }

    @Test
    public void testRuntimeFilterAboveIsNotPushedIntoTheRightSide() throws Exception {
        // the runtime filter on r.v5 built by the join above would drop the NULL-extended rows of the outer join
        // inside the right side, making the ASOF join pick an earlier row; it may only be used at the ASOF join
        String sql = "select * from t2 asof join (select t0.v1, t0.v2, t1.v5 from t0 left join t1 on t0.v1 = t1.v4) r " +
                "on t2.v7 = r.v1 and t2.v8 >= r.v2 join t3 on r.v5 = t3.v10";
        String plan = getVerboseExplain(sql);
        int asofJoin = plan.indexOf("join op: ASOF INNER JOIN");
        int outerJoin = plan.indexOf("join op: LEFT OUTER JOIN");
        int probe = plan.indexOf("probe_expr = (8: v5)");
        Assertions.assertTrue(asofJoin < probe && probe < outerJoin, plan);
        Assertions.assertEquals(probe, plan.lastIndexOf("probe_expr = (8: v5)"), plan);
    }
}
