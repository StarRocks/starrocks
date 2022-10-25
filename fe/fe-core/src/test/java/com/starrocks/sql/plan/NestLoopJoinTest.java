// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NestLoopJoinTest extends PlanTestBase {

    @Before
    public void before() {
        PlanTestBase.connectContext.getSessionVariable().enableJoinReorder(false);
    }

    @After
    public void after() {
        PlanTestBase.connectContext.getSessionVariable().enableJoinReorder(true);
    }

    @Test
    public void testJoinColumnsPrune() throws Exception {
        PlanTestBase.connectContext.getSessionVariable().setJoinImplementationMode("nestloop");
        String sql = " select count(a.v3) from t0 a join t0 b on a.v3 < b.v3;";
        getFragmentPlan(sql);

        sql = " select a.v2 from t0 a join t0 b on a.v3 < b.v3;";
        String planFragment = getFragmentPlan(sql);
        System.err.println(planFragment);
        Assert.assertTrue(planFragment, planFragment.contains(" 3:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: 3: v3 < 6: v3\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n"));
    }

    @Test
    public void testNLJoinWithPredicate() throws Exception {
        PlanTestBase.connectContext.getSessionVariable().setJoinImplementationMode("nestloop");
        String sql = "SELECT * from t0 join test_all_type where t0.v1 = 2;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment, planFragment.contains("NESTLOOP JOIN"));

        // Outer join
        PlanTestBase.connectContext.getSessionVariable().setJoinImplementationMode("auto");
        sql = "SELECT * from t0 left join test_all_type t1 on t1.t1c = 2";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment, planFragment.contains("LEFT OUTER JOIN"));

        sql = "SELECT * from t0 left join test_all_type t1 on 2 = t0.v1";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment, planFragment.contains("LEFT OUTER JOIN"));
    }

    private void assertNestloopJoin(String sql, String joinType, String onPredicate) throws Exception {
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment, planFragment.contains("NESTLOOP JOIN\n" +
                "  |  join op: " + joinType + "\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: " + onPredicate));
    }

    /**
     * Join on non-equal predicate
     */
    @Test
    public void testNLJoinExplicit() throws Exception {
        PlanTestBase.connectContext.getSessionVariable().setJoinImplementationMode("nestloop");
        assertNestloopJoin("SELECT * from t0 a join t0 b on a.v1 < b.v1", "INNER JOIN", "1: v1 < 4: v1");
        assertNestloopJoin("SELECT * from t0 a left join [broadcast] t0 b on a.v1 < b.v1", "LEFT OUTER JOIN", "1: v1 < 4: v1");
        assertNestloopJoin("SELECT * from t0 a right join t0 b on a.v1 < b.v1", "RIGHT OUTER JOIN", "1: v1 < 4: v1");
        assertNestloopJoin("SELECT * from t0 a full join t0 b on a.v1 < b.v1", "FULL OUTER JOIN", "1: v1 < 4: v1");

        PlanTestBase.connectContext.getSessionVariable().setJoinImplementationMode("");
        // Non-Equal join could only be implemented by NestLoopJoin
        assertNestloopJoin("SELECT * from t0 a join t0 b on a.v1 < b.v1", "INNER JOIN", "1: v1 < 4: v1");
        assertNestloopJoin("SELECT * from t0 a left join [broadcast] t0 b on a.v1 < b.v1", "LEFT OUTER JOIN", "1: v1 < 4: v1");
        assertNestloopJoin("SELECT * from t0 a right join t0 b on a.v1 < b.v1", "RIGHT OUTER JOIN", "1: v1 < 4: v1");
        assertNestloopJoin("SELECT * from t0 a full join t0 b on a.v1 < b.v1", "FULL OUTER JOIN", "1: v1 < 4: v1");
    }

    // Right outer join needs a GATHER distribution
    @Test
    public void testNLJoinRight() throws Exception {
        String planFragment = getFragmentPlan("select * from t0 a right join t0 b on a.v1 < b.v1");
        Assert.assertTrue(planFragment, planFragment.contains("  4:NESTLOOP JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: 1: v1 < 4: v1\n" +
                "  |  \n" +
                "  |----3:EXCHANGE\n"));

        // full join
        planFragment = getFragmentPlan("select * from t0 a full join t0 b on a.v1 < b.v1");
        Assert.assertTrue(planFragment, planFragment.contains("  4:NESTLOOP JOIN\n" +
                "  |  join op: FULL OUTER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: 1: v1 < 4: v1\n" +
                "  |  \n" +
                "  |----3:EXCHANGE\n" +
                "  |    \n" +
                "  1:EXCHANGE"));
    }

    @Test
    public void testRuntimeFilter() throws Exception {
        String sql = "select * from t0 where t0.v1 > (select max(v1) from t0 )";
        assertVerbosePlanContains(sql, "  |  build runtime filters:");

        sql = "select * from t0 a join t0 b where a.v1 > b.v1";
        assertVerbosePlanContains(sql, "  |  build runtime filters:");

        sql = "select * from t0 a join t0 b where a.v1 < b.v1";
        assertVerbosePlanContains(sql, "  |  build runtime filters:");

        sql = "select * from t0 a join t0 b where a.v1 < 100";
        assertVerbosePlanNotContains(sql, "  |  build runtime filters:");

        sql = "select * from t0 a join t0 b where a.v1 in (1,2,3)";
        assertVerbosePlanNotContains(sql, "  |  build runtime filters:");

        sql = "select * from t0 a join t0 b where a.v1 != b.v1";
        assertVerbosePlanNotContains(sql, "  |  build runtime filters:");

        sql = "select * from t0 a join t0 b where a.v1 < a.v1 + b.v1";
        assertVerbosePlanNotContains(sql, "  |  build runtime filters:");

        sql = "select * from t0 a join t0 b where a.v1 + b.v1 < 5";
        assertVerbosePlanNotContains(sql, "  |  build runtime filters:");

        sql = "select * from t0 a join t0 b where 1 < a.v1 + b.v1";
        assertVerbosePlanNotContains(sql, "  |  build runtime filters:");

        sql = "select * from t0 a join t0 b where a.v1 + 1 < b.v1";
        assertVerbosePlanNotContains(sql, "  |  build runtime filters:");

        sql = "select * from t0 a join t0 b where a.v1 + b.v1 < b.v1";
        assertVerbosePlanNotContains(sql, "  |  build runtime filters:");
    }
}
