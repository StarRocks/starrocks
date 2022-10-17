// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultiJoinReorderTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();

        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();

        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("t0");
        setTableStatistics(t0, 1);

        OlapTable t1 = (OlapTable) globalStateMgr.getDb("test").getTable("t1");
        setTableStatistics(t1, 10);
        OlapTable t2 = (OlapTable) globalStateMgr.getDb("test").getTable("t2");
        setTableStatistics(t2, 100000);

        OlapTable t3 = (OlapTable) globalStateMgr.getDb("test").getTable("t3");
        setTableStatistics(t3, 1000000000);
        connectContext.getSessionVariable().setMaxTransformReorderJoins(2);
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testCrossJoinReorderGreedy() throws Exception {
        connectContext.getSessionVariable().disableDPJoinReorder();

        String sql = "select * from t1, t2, t3, t0;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment, planFragment.contains("4:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----3:EXCHANGE"));
        Assert.assertTrue(planFragment.contains("  9:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----8:EXCHANGE"));
        Assert.assertTrue(planFragment.contains("  6:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----5:EXCHANGE\n"));
    }

    @Test
    public void testCrossAndInnerJoinReorderGreedy() throws Exception {
        connectContext.getSessionVariable().disableDPJoinReorder();
        String sql = "select * from t1 join t3 on t1.v4 = t3.v10 join t0 join t2";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("1:OlapScanNode\n" +
                "     TABLE: t0"));
        Assert.assertTrue(planFragment.contains(" |----2:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t3"));
        Assert.assertTrue(planFragment.contains("|----6:EXCHANGE\n" +
                "  |    \n" +
                "  4:OlapScanNode\n" +
                "     TABLE: t2"));
        Assert.assertTrue(planFragment.contains(" 9:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v10 = 1: v4\n" +
                "  |  \n" +
                "  |----8:EXCHANGE"));
    }

    @Test
    public void testInnerJoinReorderGreedy() throws Exception {
        connectContext.getSessionVariable().disableDPJoinReorder();
        String sql = "select * from t1 " +
                "join t3 on t1.v4 = t3.v10 " +
                "join t0 on t1.v4 = t0.v2 " +
                "join t2 on t1.v5 = t2.v8 ";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  3:OlapScanNode\n" +
                "     TABLE: t0"));
        Assert.assertTrue(planFragment.contains("  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  2:OlapScanNode\n" +
                "     TABLE: t1"));
        Assert.assertTrue(planFragment.contains("  |----6:EXCHANGE\n" +
                "  |    \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t2"));
        Assert.assertTrue(planFragment.contains("  |----8:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t3"));
    }

    @Test
    public void testLeftJoinReorderGreedy() throws Exception {
        connectContext.getSessionVariable().disableDPJoinReorder();
        String sql = "select v6 from t1 " +
                "left join (select t1.v5 from t1 join t3 on t1.v4 = t3.v10 join t0 join t2) a " +
                "on t1.v6 = a.v5";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("16:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (PARTITIONED)"));

        Assert.assertTrue(planFragment.contains("  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  2:OlapScanNode\n" +
                "     TABLE: t1"));
        Assert.assertTrue(planFragment.contains("  |----7:EXCHANGE\n" +
                "  |    \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t2"));
        Assert.assertTrue(planFragment.contains("  |----10:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t3"));
    }

    @Test
    public void testTwoJoinRootGreedy() throws Exception {
        connectContext.getSessionVariable().disableDPJoinReorder();
        String sql = "select * from (select t1.v5 from t1 join t3 on t3.v10 = t1.v4 join t0 join t2) b " +
                "left join (select t1.v5 from t1 join t3 on t3.v10 = t1.v4 join t0 join t2) a " +
                "on b.v5 = a.v5";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  25:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 16: v10 = 13: v4\n" +
                "  |  \n" +
                "  |----24:EXCHANGE\n" +
                "  |    \n" +
                "  14:OlapScanNode\n" +
                "     TABLE: t3"));
        Assert.assertTrue(planFragment.contains("  11:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v10 = 1: v4\n" +
                "  |  \n" +
                "  |----10:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t3"));
    }

    // Should produce three join tree and reorder three join tree.
    @Test
    public void testThreeJoinRootGreedy() throws Exception {
        connectContext.getSessionVariable().disableDPJoinReorder();
        connectContext.getSessionVariable().enableGreedyJoinReorder();
        String sql = "select * from (select count(t1.v5) as v55 from t1 join t3 on t3.v10 = t1.v4 join t0) b " +
                "inner join (select t1.v5 from t1 join t3 on t3.v10 = t1.v4 join t0 join t2) a " +
                "on b.v55 = a.v5";
        String planFragment = getFragmentPlan(sql);

        // Top join tree
        assertContains(planFragment, " 25:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 14: v10 = 11: v4\n" +
                "  |  \n" +
                "  |----24:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t3");

        // Left sub join tree (b)
        Assert.assertTrue(planFragment.contains("23:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 10: count = 12: v5\n" +
                "  |  \n" +
                "  |----22:EXCHANGE"));

        // Right sub join tree (a)
        assertContains(planFragment, "  20:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----19:EXCHANGE\n" +
                "  |    ");
    }

    @Test
    public void testTwoJoinRootGreedy2() throws Exception {
        connectContext.getSessionVariable().disableDPJoinReorder();
        connectContext.getSessionVariable().enableGreedyJoinReorder();
        String sql = "select t0.v1 from t1 " +
                "join t3 on t1.v4 = t3.v10 " +
                "join t0 on t1.v4 = t0.v2 " +
                "join (select * from t1 join t3 on t1.v4 = t3.v10 join t0 on t1.v4 = t0.v2 join t2 on t1.v5 = t2.v8) as a  " +
                "on t1.v5 = a.v8 ";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("24:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 10: v4 = 13: v10"));

        Assert.assertTrue(planFragment.contains("19:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v10 = 1: v4"));
    }

    @Test
    public void testCrossJoinReorderDP() throws Exception {
        connectContext.getSessionVariable().enableDPJoinReorder();
        String sql = "select * from t1, t2, t3, t0;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment, planFragment.contains("4:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----3:EXCHANGE"));
        Assert.assertTrue(planFragment.contains("  9:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----8:EXCHANGE\n"));
        Assert.assertTrue(planFragment.contains("  6:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----5:EXCHANGE\n"));
    }

    @Test
    public void testCrossAndInnerJoinReorderDP() throws Exception {
        connectContext.getSessionVariable().enableDPJoinReorder();
        connectContext.getSessionVariable().disableGreedyJoinReorder();
        String sql = "select * from t1 join t3 on t1.v4 = t3.v10 join t0 join t2";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  3:OlapScanNode\n" +
                "     TABLE: t0"));
        Assert.assertTrue(planFragment.contains("  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  2:OlapScanNode\n" +
                "     TABLE: t1"));
        Assert.assertTrue(planFragment.contains("  |----6:EXCHANGE\n" +
                "  |    \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t2"));
        Assert.assertTrue(planFragment.contains("  9:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v10 = 1: v4\n" +
                "  |  \n" +
                "  |----8:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t3"));
    }

    @Test
    public void testInnerJoinReorderDP() throws Exception {
        connectContext.getSessionVariable().enableDPJoinReorder();
        String sql = "select * from t1 " +
                "join t3 on t1.v4 = t3.v10 " +
                "join t0 on t1.v4 = t0.v2 " +
                "join t2 on t1.v5 = t2.v8 ";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  3:OlapScanNode\n" +
                "     TABLE: t0"));
        Assert.assertTrue(planFragment.contains("  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  2:OlapScanNode\n" +
                "     TABLE: t1"));
        Assert.assertTrue(planFragment.contains("  |----6:EXCHANGE\n" +
                "  |    \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t2"));
        Assert.assertTrue(planFragment.contains("  |----8:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t3"));
    }

    @Test
    public void testLeftJoinReorderDP() throws Exception {
        connectContext.getSessionVariable().enableDPJoinReorder();
        String sql = "select v6 from t1 " +
                "left join (select t1.v5 from t1 join t3 on t1.v4 = t3.v10 join t0 join t2) a " +
                "on t1.v6 = a.v5";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  16:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (PARTITIONED)"));

        Assert.assertTrue(planFragment.contains("  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  2:OlapScanNode\n" +
                "     TABLE: t1"));
        Assert.assertTrue(planFragment.contains("  |----7:EXCHANGE\n" +
                "  |    \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t2"));
        Assert.assertTrue(planFragment.contains("  |----10:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t3"));
    }

    @Test
    public void testTwoJoinRootDP() throws Exception {
        connectContext.getSessionVariable().enableDPJoinReorder();
        connectContext.getSessionVariable().disableGreedyJoinReorder();
        String sql = "select * from (select t1.v5 from t1 join t3 on t3.v10 = t1.v4 join t0 join t2) b " +
                "left join (select t1.v5 from t1 join t3 on t3.v10 = t1.v4 join t0 join t2) a " +
                "on b.v5 = a.v5";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  25:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 16: v10 = 13: v4\n" +
                "  |  \n" +
                "  |----24:EXCHANGE\n" +
                "  |    \n" +
                "  14:OlapScanNode\n" +
                "     TABLE: t3"));
        Assert.assertTrue(planFragment.contains("  11:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v10 = 1: v4\n" +
                "  |  \n" +
                "  |----10:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t3"));
    }

    // Should produce three join tree and reorder three join tree.
    @Test
    public void testThreeJoinRootDP() throws Exception {
        connectContext.getSessionVariable().enableDPJoinReorder();
        connectContext.getSessionVariable().disableGreedyJoinReorder();
        String sql = "select * from (select count(t1.v5) as v55 from t1 join t3 on t3.v10 = t1.v4 join t0) b " +
                "inner join (select t1.v5 from t1 join t3 on t3.v10 = t1.v4 join t0 join t2) a " +
                "on b.v55 = a.v5";
        String planFragment = getFragmentPlan(sql);

        // Top join tree
        assertContains(planFragment, "  25:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 14: v10 = 11: v4\n" +
                "  |  \n" +
                "  |----24:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t3\n");

        // Left sub join tree (b)
        Assert.assertTrue(planFragment, planFragment.contains("  23:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 10: count = 12: v5\n" +
                "  |  \n" +
                "  |----22:EXCHANGE\n" +
                "  |    \n" +
                "  20:Project\n" +
                "  |  <slot 10> : 10: count\n" +
                "  |  \n" +
                "  19:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----18:EXCHANGE\n" +
                "  |    \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t2\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: t2\n" +
                "     tabletRatio=3/3\n"));

        // Right sub join tree (a)
        assertContains(planFragment, "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 18\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  17:Project\n" +
                "  |  <slot 10> : 10: count\n" +
                "  |  \n" +
                "  16:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----15:EXCHANGE\n" +
                "  |    \n" +
                "  2:OlapScanNode\n" +
                "     TABLE: t0\n");
    }

    @Test
    public void testTwoJoinRootDP2() throws Exception {
        connectContext.getSessionVariable().enableDPJoinReorder();
        connectContext.getSessionVariable().disableGreedyJoinReorder();
        String sql = "select t0.v1 from t1 " +
                "join t3 on t1.v4 = t3.v10 " +
                "join t0 on t1.v4 = t0.v2 " +
                "join (select * from t1 join t3 on t1.v4 = t3.v10 join t0 on t1.v4 = t0.v2 join t2 on t1.v5 = t2.v8) as a  " +
                "on t1.v5 = a.v8 ";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 13: v10 = 10: v4\n"));

        Assert.assertTrue(planFragment.contains("  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v10 = 1: v4\n"));
    }

    @Test
    public void testOutputConstant() throws Exception {
        String sql = "select v from (select v1, 2 as v, 3 from t0 inner join t1 on v2 = v4) t,t2;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment, planFragment.contains("8:Project\n" +
                "  |  <slot 7> : 7: expr\n" +
                "  |  \n" +
                "  7:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n"));

        sql = "select * from (select v1, 2 as v, 3 from t0 inner join t1 on v2 = v4) t,t2;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("5:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 7> : 2\n" +
                "  |  <slot 8> : 3\n" +
                "  |  \n" +
                "  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)"));
    }

    @Test
    public void testMultiCrossJoinReorder() throws Exception {
        // check multi cross join reorder without exception
        String sql = "select count(*) from t0,t1,t2,t3,t0 as t4, t1 as t5 where true";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("17:NESTLOOP JOIN"));
    }

    @Test
    public void testInsertWithMultiJoin() throws Exception {
        String sql = "insert into test_all_type(t1b, t1c, t1d, t1a) select v1,v4,v7,t1b from (" +
                "select v1,v4 from t0 join t1 on v1 = v4 ) a join (" +
                "select t1a, null as t1b,v7 from test_all_type join t2 on t1a = v7) b on v1 = t1a";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("<slot 27> : NULL"));
    }
}
