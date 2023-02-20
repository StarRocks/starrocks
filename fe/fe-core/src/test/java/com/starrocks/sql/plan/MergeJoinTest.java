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

import org.junit.Test;

public class MergeJoinTest extends PlanTestBase {


    @Test
    public void testInnerJoinWithPredicate() throws Exception {
        PlanTestBase.connectContext.getSessionVariable().setJoinImplementationMode("merge");
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 1;";
        String planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "PREDICATES: 1: v1 = 1");
    }

    @Test
    public void testJoinColumnsPrune() throws Exception {
        PlanTestBase.connectContext.getSessionVariable().setJoinImplementationMode("merge");
        String sql = " select count(a.v3) from t0 a join t0 b on a.v3 = b.v3;";
        String planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "MERGE JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)");

        sql = " select a.v2 from t0 a join t0 b on a.v3 = b.v3;";
        planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "Project\n"
                + "  |  <slot 2> : 2: v2");
    }

    @Test
    public void testJoinWithAutoMode() throws Exception {
        PlanTestBase.connectContext.getSessionVariable().setJoinImplementationMode("auto");
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 1;";
        String planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "PREDICATES: 1: v1 = 1");

        sql = " select count(a.v3) from t0 a join t0 b on a.v3 = b.v3;";
        planFragment = getFragmentPlan(sql);
        assertContains(planFragment, " 3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)");

        sql = " select a.v2 from t0 a join t0 b on a.v3 = b.v3;";
        planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "Project\n"
                + "  |  <slot 2> : 2: v2");

        sql = "SELECT * from t0 join test_all_type where t0.v1 = 2;";
        planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "PREDICATES: 1: v1 = 2");
    }

}
