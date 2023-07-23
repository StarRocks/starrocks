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

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import org.junit.BeforeClass;
import org.junit.Test;

public class TPCHPlanTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
    }

    @Test
    public void testJoin() {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable table1 = (OlapTable) globalStateMgr.getDb("test").getTable("t0");
        setTableStatistics(table1, 10000);
        runFileUnitTest("optimized-plan/join");
        setTableStatistics(table1, 0);
    }

    @Test
    public void testSelect() {
        runFileUnitTest("optimized-plan/select");
    }

    @Test
    public void testPredicatePushDown() {
        runFileUnitTest("optimized-plan/predicate-pushdown");
    }

    @Test
    public void testInSubquery() {
        runFileUnitTest("subquery/in-subquery");
    }

    @Test
    public void test() throws Exception {
        String sql = "select v1, v4 in (select t1a from test_all_type where t1a + v1 = v4 + t1c and v2 = 1 and v5 = 1) from t0, t1;";
        String plan = getCostExplain(sql);
        System.out.println(plan);
    }

    @Test
    public void testExists() {
        runFileUnitTest("subquery/exists-subquery");
    }

    @Test
    public void testScalar() {
        runFileUnitTest("subquery/scalar-subquery");
    }

    @Test
    public void testWindow() {
        runFileUnitTest("optimized-plan/window");
    }

    @Test
    public void testExtractPredicate() {
        runFileUnitTest("optimized-plan/predicate-extract");
    }

    @Test
    public void testConstant() {
        runFileUnitTest("optimized-plan/constant-query");
    }

    @Test
    public void testSetOperation() {
        runFileUnitTest("optimized-plan/set-operation");
    }

    @Test
    public void testGroupingSets() {
        runFileUnitTest("optimized-plan/grouping-sets");
    }

    @Test
    public void testLateral() {
        runFileUnitTest("optimized-plan/lateral");
    }

    @Test
    public void testTPCH1() {
        runFileUnitTest("tpch/q1");
    }

    @Test
    public void testTPCH2() {
        runFileUnitTest("tpch/q2");
    }

    @Test
    public void testTPCH3() {
        runFileUnitTest("tpch/q3");
    }

    @Test
    public void testTPCH4() {
        runFileUnitTest("tpch/q4");
    }

    @Test
    public void testTPCH5() {
        runFileUnitTest("tpch/q5");
    }

    @Test
    public void testTPCH6() {
        runFileUnitTest("tpch/q6");
    }

    @Test
    public void testTPCH7() {
        runFileUnitTest("tpch/q7");
    }

    @Test
    public void testTPCH8() {
        runFileUnitTest("tpch/q8");
    }

    @Test
    public void testTPCH9() {
        runFileUnitTest("tpch/q9");
    }

    @Test
    public void testTPCH10() {
        runFileUnitTest("tpch/q10");
    }

    @Test
    public void testTPCH11() {
        runFileUnitTest("tpch/q11");
    }

    @Test
    public void testTPCH12() {
        runFileUnitTest("tpch/q12");
    }

    @Test
    public void testTPCH13() {
        runFileUnitTest("tpch/q13");
    }

    @Test
    public void testTPCH14() {
        runFileUnitTest("tpch/q14");
    }

    @Test
    public void testTPCH15() {
        runFileUnitTest("tpch/q15");
    }

    @Test
    public void testTPCH16() {
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
        runFileUnitTest("tpch/q16");
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
    }

    @Test
    public void testTPCH17() {
        runFileUnitTest("tpch/q17");
    }

    @Test
    public void testTPCH18() {
        runFileUnitTest("tpch/q18");
    }

    @Test
    public void testTPCH19() {
        runFileUnitTest("tpch/q19");
    }

    @Test
    public void testTPCH20() {
        runFileUnitTest("tpch/q20");
    }

    @Test
    public void testTPCH21() {
        runFileUnitTest("tpch/q21");
    }

    @Test
    public void testTPCH22() {
        runFileUnitTest("tpch/q22");
    }
}
