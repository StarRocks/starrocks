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
// limitations under the License.package com.starrocks.sql.plan;
package com.starrocks.sql.plan;

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class GroupingSetsTest extends PlanTestBase {
    private static final int NUM_TABLE0_ROWS = 10000;

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        Config.alter_scheduler_interval_millisecond = 1;
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("t0");
        setTableStatistics(t0, NUM_TABLE0_ROWS);
        FeConstants.runningUnitTest = true;
    }

    @Before
    public void before() {
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testRepeatNodeWithUnionAllRewrite1() throws Exception {
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(true);
        String sql = "select v1, v2, SUM(v3) from t0 group by rollup(v1, v2)";
        String plan = getFragmentPlan(sql).replaceAll(" ", "");
        Assert.assertTrue(plan.contains("1:UNION\n" +
                "|\n" +
                "|----15:EXCHANGE\n" +
                "|\n" +
                "|----21:EXCHANGE\n" +
                "|\n" +
                "8:EXCHANGE\n"));

        sql = "select v1, SUM(v3) from t0 group by rollup(v1)";
        plan = getFragmentPlan(sql).replaceAll(" ", "");
        Assert.assertTrue(plan.contains("1:UNION\n" +
                "|\n" +
                "|----14:EXCHANGE\n" +
                "|\n" +
                "8:EXCHANGE\n"));

        sql = "select SUM(v3) from t0 group by grouping sets(())";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 5: GROUPING_ID\n" +
                "\n" +
                "  2:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: sum(3: v3)\n" +
                "  |  group by: 5: GROUPING_ID\n" +
                "  |  \n" +
                "  1:REPEAT_NODE"));
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(false);
    }

    @Test
    public void testGroupingSetsToUnionRewrite1() throws Exception {
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(true);
        String sql = "select v1, grouping(v1) as b, sum(v3) " +
                "   from t0 group by grouping sets((), (v1)) order by v1, b";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("14:Project\n" +
                "  |  <slot 12> : 12: v1\n" +
                "  |  <slot 14> : 14: sum\n" +
                "  |  <slot 16> : 0\n" +
                "  |  \n" +
                "  13:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(14: sum)\n" +
                "  |  group by: 12: v1"));
        Assert.assertTrue(plan.contains("  7:Project\n" +
                "  |  <slot 8> : 8: sum\n" +
                "  |  <slot 9> : NULL\n" +
                "  |  <slot 11> : 1\n" +
                "  |  \n" +
                "  6:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(8: sum)\n" +
                "  |  group by: "));
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(false);
    }

    @Test
    public void testGroupingSetsToUnionRewrite2() throws Exception {
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(true);
        String sql = "select v1, v2, grouping_id(v1, v2) as b, sum(v3) " +
                "from t0 group by grouping sets((), (v1, v2)) order by v1, b";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("14:Project\n" +
                "  |  <slot 13> : 13: v1\n" +
                "  |  <slot 14> : 14: v2\n" +
                "  |  <slot 16> : 16: sum\n" +
                "  |  <slot 18> : 0\n" +
                "  |  \n" +
                "  13:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(16: sum)\n" +
                "  |  group by: 13: v1, 14: v2"));
        Assert.assertTrue(plan.contains("14:Project\n" +
                "  |  <slot 13> : 13: v1\n" +
                "  |  <slot 14> : 14: v2\n" +
                "  |  <slot 16> : 16: sum\n" +
                "  |  <slot 18> : 0\n" +
                "  |  \n" +
                "  13:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(16: sum)\n" +
                "  |  group by: 13: v1, 14: v2"));
        Assert.assertTrue(plan.contains("7:Project\n" +
                "  |  <slot 8> : 8: sum\n" +
                "  |  <slot 9> : NULL\n" +
                "  |  <slot 10> : NULL\n" +
                "  |  <slot 12> : 3\n" +
                "  |  \n" +
                "  6:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(8: sum)\n" +
                "  |  group by: "));
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(false);
    }

    @Test
    public void testGroupingSetsToUnionRewrite3() throws Exception {
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(true);
        String sql = "select v1, v2, sum(v3) " +
                "from t0 group by grouping sets((), (v1, v2)) order by v1, v2";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("  7:Project\n" +
                "  |  <slot 7> : 7: sum\n" +
                "  |  <slot 8> : NULL\n" +
                "  |  <slot 9> : NULL\n" +
                "  |  \n" +
                "  6:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(7: sum)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  5:EXCHANGE"));
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(false);
    }
    @Test
    public void testRollupToUnionRewrite1() throws Exception {
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(true);
        String sql = "select v1, grouping(v1) as b, sum(v3) " +
                "   from t0 group by rollup(v1, v2) order by v1, b";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("21:Project\n" +
                "  |  <slot 19> : 19: v1\n" +
                "  |  <slot 22> : 22: sum\n" +
                "  |  <slot 24> : 0"));
        Assert.assertTrue(plan.contains("14:Project\n" +
                "  |  <slot 13> : 13: v1\n" +
                "  |  <slot 15> : 15: sum\n" +
                "  |  <slot 18> : 0"));
        Assert.assertTrue(plan.contains("  7:Project\n" +
                        "  |  <slot 8> : 8: sum\n" +
                        "  |  <slot 9> : NULL\n" +
                        "  |  <slot 12> : 1"));
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(false);
    }

    @Test
    public void testCubeUnionRewrite1() throws Exception {
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(true);
        String sql = "select v1, grouping_id(v1) as b, count(1) " +
                "   from t0 group by rollup(v1, v2, v3) order by v1, b";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("  1:UNION\n" +
                "  |  \n" +
                "  |----15:EXCHANGE\n" +
                "  |    \n" +
                "  |----22:EXCHANGE\n" +
                "  |    \n" +
                "  |----29:EXCHANGE\n" +
                "  |    \n" +
                "  8:EXCHANGE"));
        Assert.assertTrue(plan.contains("  28:Project\n" +
                "  |  <slot 26> : 26: v1\n" +
                "  |  <slot 29> : 29: count\n" +
                "  |  <slot 31> : 0\n"));
        Assert.assertTrue(plan.contains("  21:Project\n" +
                "  |  <slot 20> : 20: v1\n" +
                "  |  <slot 22> : 22: count\n" +
                "  |  <slot 25> : 0\n"));
        Assert.assertTrue(plan.contains("  14:Project\n" +
                "  |  <slot 14> : 14: v1\n" +
                "  |  <slot 15> : 15: count\n" +
                "  |  <slot 19> : 0\n"));
        connectContext.getSessionVariable().setEnableRewriteGroupingSetsToUnionAll(false);
    }
}
