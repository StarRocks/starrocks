// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.starrocks.sql.analyzer.SemanticException;
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

        // Outer join
        PlanTestBase.connectContext.getSessionVariable().setJoinImplementationMode("auto");
        sql = "SELECT * from t0 left join test_all_type t1 on t1.t1c = 2";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment, planFragment.contains("RIGHT OUTER JOIN"));

        sql = "SELECT * from t0 left join test_all_type t1 on 2 = t0.v1";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment, planFragment.contains("RIGHT OUTER JOIN"));
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
                "  |----3:EXCHANGE\n" +
                "  |    \n" +
                "  1:EXCHANGE"));

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
    public void testUnsupportedNLJoin() throws Exception {
        String sql = "select v1 from t0 where 1 IN ((SELECT v4 FROM t1, t2, t3 WHERE CASE WHEN true " +
                "THEN (CAST(((((-1710265121)%(1583445171)))%(CAST(v1 AS INT ) )) AS STRING ) )  " +
                "BETWEEN (v4) AND (v5)   " +
                "WHEN CASE  WHEN  (v3) >= ( v1 )  THEN  (v9) = (v10)   " +
                "WHEN false THEN NULL ELSE false END THEN true  WHEN false THEN false ELSE " +
                "CASE WHEN (((((331435726)/(599089901)))%(((-1103769432)/(1943795037)))))  " +
                "BETWEEN (((((468244514)%(2000495251)))/(560246333))) AND (((CAST(v8 AS INT ) )/(170534098))) " +
                "THEN (NOT (true)) WHEN NULL THEN (DAYOFMONTH('1969-12-30')) IN (154771541, NULL, 91180822) END END));";

        Assert.assertThrows(SemanticException.class, () -> getFragmentPlan(sql));
    }

    @Test
    public void testRuntimeFilter() throws Exception {
        String sql = "select * from t0 where t0.v1 > (select max(v1) from t0 )";
        assertPlanContains(sql, "  6:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: 1: v1 > 7: max\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (7: max), remote = false\n");
    }
}
