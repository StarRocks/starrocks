// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import org.junit.Assert;
import org.junit.Test;

public class NestLoopJoinTest extends PlanTestBase {

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
                "  |----2:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode\n"));
    }

    @Test
    public void testNLJoinWithPredicate() throws Exception {
        PlanTestBase.connectContext.getSessionVariable().setJoinImplementationMode("nestloop");
        String sql = "SELECT * from t0 join test_all_type where t0.v1 = 2;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment, planFragment.contains("NESTLOOP JOIN"));
    }

    private void assertNestloopJoin(String sql, String joinType, String onPredicate) throws Exception {
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment, planFragment.contains("  3:NESTLOOP JOIN\n" +
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
        assertNestloopJoin("SELECT * from t0 a left join t0 b on a.v1 < b.v1", "LEFT OUTER JOIN", "1: v1 < 4: v1");
        assertNestloopJoin("SELECT * from t0 a right join t0 b on a.v1 < b.v1", "RIGHT OUTER JOIN", "1: v1 < 4: v1");
        assertNestloopJoin("SELECT * from t0 a full join t0 b on a.v1 < b.v1", "FULL OUTER JOIN", "1: v1 < 4: v1");

        // TODO: support equal-predicate join
        // assertNestloopJoin("SELECT * from t0 a join t0 b on a.v1 = b.v1", "INNER JOIN", "1: v1 = 4: v1");
        // assertNestloopJoin("SELECT * from t0 a left join t0 b on a.v1 = b.v1", "LEFT OUTER JOIN", "1: v1 = 4: v1");
        // assertNestloopJoin("SELECT * from t0 a right join t0 b on a.v1 = b.v1", "RIGHT OUTER JOIN", "1: v1 = 4: v1");
        // assertNestloopJoin("SELECT * from t0 a full join t0 b on a.v1 = b.v1", "FULL OUTER JOIN", "1: v1 = 4: v1");

        PlanTestBase.connectContext.getSessionVariable().setJoinImplementationMode("");
        // Non-Equal join could only be implemented by NestLoopJoin
        // assertNestloopJoin("SELECT * from t0 a join t0 b on a.v1 < b.v1", "INNER JOIN", "1: v1 < 4: v1");
        // assertNestloopJoin("SELECT * from t0 a left join t0 b on a.v1 < b.v1", "LEFT OUTER JOIN", "1: v1 < 4: v1");
        // assertNestloopJoin("SELECT * from t0 a right join t0 b on a.v1 < b.v1", "RIGHT OUTER JOIN", "1: v1 < 4: v1");
        // assertNestloopJoin("SELECT * from t0 a full join t0 b on a.v1 < b.v1", "FULL OUTER JOIN", "1: v1 < 4: v1");
    }

}
