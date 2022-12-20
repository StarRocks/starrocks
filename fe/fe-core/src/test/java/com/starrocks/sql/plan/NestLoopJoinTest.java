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

        PlanTestBase.connectContext.getSessionVariable().setJoinImplementationMode("auto");
        // Prune should make the HASH JOIN(LEFT ANTI) could output the left table, but not join slot
        sql = "select distinct('const') from t0, t1, " +
                " (select * from t2 where cast(v7 as string) like 'ss%' ) sub1 " +
                "left anti join " +
                " (select * from t3 where cast(v10 as string) like 'ss%' ) sub2" +
                " on substr(cast(sub1.v7 as string), 1) = substr(cast(sub2.v10 as string), 1)";
        assertPlanContains(sql, "11:Project\n" +
                "  |  <slot 7> : 7: v7\n" +
                "  |  \n" +
                "  10:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 14: substr = 15: substr");

        // RIGHT ANTI JOIN + AGGREGATE count(*)
        sql = "select count(*) from (select t2.id_char, t2.id_varchar " +
                "from test_all_type_nullable t1 " +
                "right anti join test_all_type_nullable2 t2 " +
                "on t1.id_char = 0) as a;";
        assertVerbosePlanContains(sql, "4:Project\n" +
                "  |  output columns:\n" +
                "  |  34 <-> [34: id_char, CHAR, false]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  3:NESTLOOP JOIN\n" +
                "  |  join op: LEFT ANTI JOIN\n" +
                "  |  other join predicates: [8: id_char, CHAR, false] = '0'\n" +
                "  |  cardinality: 1");

        // RIGHT ANTI JOIN + AGGREGATE count(column)
        sql = "select count(a.id_char) " +
                "from (select t2.id_char, t2.id_varchar " +
                "from test_all_type_nullable t1 " +
                "right anti join test_all_type_nullable2 t2 " +
                "on t1.id_char = 0) as a;";
        assertVerbosePlanContains(sql, "  4:Project\n" +
                "  |  output columns:\n" +
                "  |  34 <-> [34: id_char, CHAR, false]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  3:NESTLOOP JOIN\n" +
                "  |  join op: LEFT ANTI JOIN\n" +
                "  |  other join predicates: [8: id_char, CHAR, false] = '0'\n" +
                "  |  cardinality: 1");

        // LEFT ANTI JOIN + AGGREGATE
        sql = "select count(*) from (" +
                "select id_char, id_varchar " +
                "from test_all_type_nullable t1 " +
                "left anti join test_all_type_nullable2 t2 " +
                "on t1.id_char = 0) as a;";
        assertVerbosePlanContains(sql, "  4:Project\n" +
                "  |  output columns:\n" +
                "  |  8 <-> [8: id_char, CHAR, false]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  3:NESTLOOP JOIN\n" +
                "  |  join op: LEFT ANTI JOIN\n" +
                "  |  other join predicates: [8: id_char, CHAR, false] = '0'\n" +
                "  |  cardinality: 1");
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
        assertNestloopJoin("SELECT * from t0 a left join [broadcast] t0 b on a.v1 < b.v1", "LEFT OUTER JOIN",
                "1: v1 < 4: v1");
        assertNestloopJoin("SELECT * from t0 a right join t0 b on a.v1 < b.v1", "RIGHT OUTER JOIN", "1: v1 < 4: v1");
        assertNestloopJoin("SELECT * from t0 a full join t0 b on a.v1 < b.v1", "FULL OUTER JOIN", "1: v1 < 4: v1");

        PlanTestBase.connectContext.getSessionVariable().setJoinImplementationMode("");
        // Non-Equal join could only be implemented by NestLoopJoin
        assertNestloopJoin("SELECT * from t0 a join t0 b on a.v1 < b.v1", "INNER JOIN", "1: v1 < 4: v1");
        assertNestloopJoin("SELECT * from t0 a left join [broadcast] t0 b on a.v1 < b.v1", "LEFT OUTER JOIN",
                "1: v1 < 4: v1");
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
    public void testSemiNLJoin() throws Exception {
        String sql = "select v1 from t0 where 1 IN ((SELECT v4 FROM t1, t2, t3 WHERE CASE WHEN true " +
                "THEN (CAST(((((-1710265121)%(1583445171)))%(CAST(v1 AS INT ) )) AS STRING ) )  " +
                "BETWEEN (v4) AND (v5)   " +
                "WHEN CASE  WHEN  (v3) >= ( v1 )  THEN  (v9) = (v10)   " +
                "WHEN false THEN NULL ELSE false END THEN true  WHEN false THEN false ELSE " +
                "CASE WHEN (((((331435726)/(599089901)))%(((-1103769432)/(1943795037)))))  " +
                "BETWEEN (((((468244514)%(2000495251)))/(560246333))) AND (((CAST(v8 AS INT ) )/(170534098))) " +
                "THEN (NOT (true)) WHEN NULL THEN (DAYOFMONTH('1969-12-30')) IN (154771541, NULL, 91180822) END END));";
        assertPlanContains(sql, "NESTLOOP JOIN");

        assertPlanContains("select * from t0,t1 where 1 in (select 2 from t2,t3 where t0.v1 = 1 and t1.v4 = 2)",
                "NESTLOOP JOIN");
        assertPlanContains("select * from t0,t1 where 1 in (select v7 from t2,t3 where t0.v1 = 1 and t1.v4 = 2)",
                "NESTLOOP JOIN");
        assertPlanContains("select * from t0,t1 where v1 in (select 1+2+3 from t2,t3 where t0.v1 = 1 and t1.v4 = 2)",
                "NESTLOOP JOIN");
        assertPlanContains(
                "select * from t0,t1 where abs(1) - 1 in (select 'abc' from t2,t3 where t0.v1 = 1 and t1.v4 = 2)",
                "NESTLOOP JOIN");
        assertPlanContains("select * from t0,t1 where 1 not in (select v7 from t2,t3 where t0.v1 = 1 and t1.v4 = 2)",
                "NESTLOOP JOIN");
        assertPlanContains("select * from t0,t1 where 1 not in (select v7 from t2,t3 where t0.v1 = 1 and t1.v4 = 2)",
                "NESTLOOP JOIN");
        assertPlanContains(
                "select * from t0,t1 where v1 not in (select 1+2+3 from t2,t3 where t0.v1 = 1 and t1.v4 = 2)",
                "NESTLOOP JOIN");
        assertPlanContains(
                "select * from t0,t1 where abs(1) - 1 not in (select v7 + 1 from t2,t3 where t0.v1 = 1 and t1.v4 = 2)",
                "NESTLOOP JOIN");
        assertPlanContains("select * from t0 left semi join t1 on t0.v1 < t1.v4", "NESTLOOP JOIN");
        assertPlanContains("select * from t0 left anti join t1 on t0.v1 < t1.v4", "NESTLOOP JOIN");
        assertPlanContains("select * from t0 right semi join t1 on t0.v1 < t1.v4", "NESTLOOP JOIN");
        assertPlanContains("select * from t0 right anti join t1 on t0.v1 < t1.v4", "NESTLOOP JOIN");
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


    @Test
    public void testMultipleNlJoinInSingleFragment() throws Exception {
        connectContext.getSessionVariable().disableJoinReorder();
        String sql = "select count(1) from " +
                "t0 a right join (" +
                "   select d.v1 from " +
                "       (select ba.v1 from t0 ba where false) b " +
                "       join t0 c " +
                "       join t0 d " +
                ") e on a.v1 < e.v1";
        assertPlanContains(sql, "  11:NESTLOOP JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: 1: v1 < 10: v1\n" +
                "  |  \n" +
                "  |----10:Project\n" +
                "  |    |  <slot 10> : 10: v1\n" +
                "  |    |  \n" +
                "  |    9:NESTLOOP JOIN\n" +
                "  |    |  join op: CROSS JOIN\n" +
                "  |    |  colocate: false, reason: \n" +
                "  |    |  \n" +
                "  |    |----8:EXCHANGE\n" +
                "  |    |    \n" +
                "  |    6:Project\n" +
                "  |    |  <slot 4> : 4: v1\n" +
                "  |    |  \n" +
                "  |    5:NESTLOOP JOIN\n" +
                "  |    |  join op: CROSS JOIN\n" +
                "  |    |  colocate: false, reason: \n" +
                "  |    |  \n" +
                "  |    |----4:EXCHANGE\n" +
                "  |    |    \n" +
                "  |    2:EMPTYSET\n" +
                "  |    \n" +
                "  1:EXCHANGE");
    }
}
