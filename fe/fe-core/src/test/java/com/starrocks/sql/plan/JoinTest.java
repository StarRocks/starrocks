// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.rule.RuleSet;
import com.starrocks.sql.optimizer.rule.transformation.JoinAssociativityRule;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

public class JoinTest extends PlanTestBase {
    @Test
    public void testColocateDistributeSatisfyShuffleColumns() throws Exception {
        String sql = "select * from colocate1 left join colocate2 on colocate1.k1=colocate2.k1;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("colocate: false"));
        Assert.assertTrue(plan.contains("join op: LEFT OUTER JOIN (BROADCAST)"));

        sql = "select * from colocate1 left join colocate2 on colocate1.k1=colocate2.k1 and colocate1.k2=colocate2.k2;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("colocate: true"));
        Assert.assertTrue(plan.contains("join op: LEFT OUTER JOIN (COLOCATE)"));
    }

    @Test
    public void testInnerJoinWithPredicate() throws Exception {
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 1;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 1: v1 = 1"));
    }

    @Test
    public void testInnerJoinWithConstPredicate() throws Exception {
        String sql = "SELECT * from t0 join test_all_type on NOT NULL >= NULL";

        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  0:EMPTYSET\n"));
    }

    @Test
    public void testInnerJoinWithCastPredicate() throws Exception {
        String sql = "SELECT t0.v1 from t0 join test_all_type on t0.v1 = test_all_type.t1c";
        getFragmentPlan(sql);
    }

    @Test
    public void testCorssJoinWithPredicate() throws Exception {
        String sql = "SELECT * from t0 join test_all_type where t0.v1 = 2;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 1: v1 = 2"));
    }

    @Test
    public void testLeftOuterJoinWithPredicate() throws Exception {
        String sql = "SELECT * from t0 left join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 > 1;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 1: v1 > 1"));
    }

    @Test
    public void testCrossJoinToInnerJoin() throws Exception {
        String sql = "SELECT t0.v1 from t0, test_all_type where t0.v1 = test_all_type.t1d";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("join op: INNER JOIN"));
        Assert.assertTrue(planFragment.contains("equal join conjunct: 1: v1 = 7: t1d"));
    }

    @Test
    public void testJoinColumnsPrune() throws Exception {
        String sql = " select count(a.v3) from t0 a join t0 b on a.v3 = b.v3;";
        getFragmentPlan(sql);

        sql = " select a.v2 from t0 a join t0 b on a.v3 = b.v3;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("4:Project\n"
                + "  |  <slot 2> : 2: v2"));
    }

    @Test
    public void testCrossJoin() throws Exception {
        String sql = "SELECT * from t0 join test_all_type;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  3:CROSS JOIN\n" +
                "  |  cross join:\n" +
                "  |  predicates is NULL.\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode"));

        sql = "select * from t0 join test_all_type on NOT 69 IS NOT NULL where true";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  3:CROSS JOIN\n" +
                "  |  cross join:\n" +
                "  |  predicates is NULL.\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
                "  |    \n" +
                "  0:EMPTYSET"));
    }

    @Test
    public void testFullOuterJoin() throws Exception {
        String sql = "select * from t0 full outer join t1 on t0.v1 = t1.v4 where abs(1) > 2;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("     TABLE: t1\n"
                + "     PREAGGREGATION: ON\n"
                + "     PREDICATES: abs(1) > 2"));
        Assert.assertTrue(planFragment.contains("     TABLE: t0\n"
                + "     PREAGGREGATION: ON\n"
                + "     PREDICATES: abs(1) > 2"));
    }

    @Test
    public void testFullOuterJoinPredicatePushDown() throws Exception {
        String sql = "select * from t0 full outer join t1 on t0.v1 = t1.v4 " +
                " where (NOT (t0.v2 IS NOT NULL))";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("other predicates: 2: v2 IS NULL"));
    }

    @Test
    public void testRightSemiJoinWithFilter() throws Exception {
        String sql = "select t1.v4 from t0 right semi join t1 on t0.v1 = t1.v4 and t0.v1 > 1 ";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 1: v1 > 1"));
    }

    @Test
    public void testColocateJoin() throws Exception {
        String queryStr = "select * from test.colocate1 t1, test.colocate2 t2 " +
                "where t1.k1 = t2.k1 and t1.k2 = t2.k2 and t1.k3 = t2.k3";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        // t1.k1 = t2.k2 not same order with distribute column
        queryStr = "select * from test.colocate1 t1, test.colocate2 t2 " +
                "where t1.k1 = t2.k2 and t1.k2 = t2.k1 and t1.k3 = t2.k3";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));

        queryStr = "select * from test.colocate1 t1, test.colocate2 t2 where t1.k1 = t2.k1";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));

        queryStr = "select * from test.colocate1 t1, test.colocate2 t2 where t1.k2 = t2.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));

        queryStr = "select * from test.colocate1 t1, test.colocate2 t2 " +
                "where t1.k1 = t2.k1 and t1.k2 = t2.k2 + 1";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));
    }

    @Test
    public void testColocateJoinWithOneAggChild() throws Exception {
        String queryStr =
                "select * from test.colocate1 t1 left join (select k1, k2, count(k3) from test.colocate2 group by k1,"
                        + " k2) t2 on  "
                        +
                        "t1.k1 = t2.k1 and t1.k2 = t2.k2";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from test.colocate1 t1 left join (select k1, k2, k3, count(k3) from test.colocate2 group by"
                        + " k1, k2, k3) t2 on  "
                        +
                        "t1.k1 = t2.k1 and t1.k2 = t2.k2 and t1.k3 = t2.k3";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from (select k1, k2, count(k3) from test.colocate2 group by k1, k2) t2 left join test"
                        + ".colocate1 t1 on  "
                        +
                        "t2.k1 = t1.k1 and t2.k2 = t1.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from test.colocate1 t1 left join (select k1, k2, k3, count(k3) from test.colocate2 group by"
                        + " k1, k2, k3) t2 on  "
                        +
                        "t1.k1 = t2.k1 and t1.k2 = t2.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from test.colocate1 t1 left join (select k1, k2, count(k3) from test.colocate2 group by k2,"
                        + " k1) t2 on  "
                        +
                        "t1.k1 = t2.k1 and t1.k2 = t2.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from test.colocate1 t1 left join (select k1, k2, count(k3) from test.colocate2 group by k2,"
                        + " k1) t2 on  "
                        +
                        "t1.k1 = t2.k1";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));

        queryStr =
                "select * from test.colocate1 t1 left join (select k1, k2, count(k3) from test.colocate2 group by k2,"
                        + " k1) t2 on  "
                        +
                        "t1.k2 = t2.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));

        queryStr =
                "select * from test.colocate1 t1 left join (select k1, k2, count(k3) from test.colocate2 group by k2,"
                        + " k1) t2 on  "
                        +
                        "t1.k1 = t2.k2 and t1.k2 = t2.k1";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));
    }

    @Test
    public void testColocateJoinWithTwoAggChild() throws Exception {
        String queryStr =
                "select * from (select k1, k2, count(k3) from test.colocate1 group by k1, k2) t1 left join (select "
                        + "k1, k2, count(k3) from test.colocate2 group by k1, k2) t2 on  "
                        +
                        "t1.k1 = t2.k1 and t1.k2 = t2.k2";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from (select k1, k2, k3, count(k3) from test.colocate1 group by k1, k2, k3) t1 left join "
                        + "(select k1, k2, k3, count(k3) from test.colocate2 group by k1, k2, k3) t2 on  "
                        +
                        "t1.k1 = t2.k1 and t1.k2 = t2.k2 ";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from (select k1, k2, k3, count(k3) from test.colocate1 group by k1, k2, k3) t1 left join "
                        + "(select k1, k2, count(k3) from test.colocate2 group by k1, k2) t2 on  "
                        +
                        "t1.k1 = t2.k1 and t1.k2 = t2.k2 ";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from (select k1, k2, k3, count(k3) from test.colocate1 group by k1, k2, k3) t1 left join "
                        + "(select k1, k2, k3, count(k3) from test.colocate2 group by k1, k2, k3) t2 on  "
                        +
                        "t1.k1 = t2.k1 and t1.k2 = t2.k2 and t1.k3 = t2.k3";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from (select k1, k2, count(k3) from test.colocate1 group by k2, k1) t1 left join (select "
                        + "k1, k2, count(k3) from test.colocate2 group by k1, k2) t2 on  "
                        +
                        "t1.k1 = t2.k1 and t1.k2 = t2.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from (select k1, k2, count(k3) from test.colocate1 group by k1, k2) t1 left join (select "
                        + "k1, k2, count(k3) from test.colocate2 group by k1, k2) t2 on  "
                        +
                        "t1.k2 = t2.k1 and t1.k1 = t2.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));

        queryStr =
                "select * from (select k1, k2, count(k3) from test.colocate1 group by k1, k2) t1 left join (select "
                        + "k1, k2, count(k3) from test.colocate2 group by k1, k2) t2 on  "
                        +
                        "t1.k1 = t2.k1";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));
    }

    @Test
    public void testColocateJoinWithTwoAggChild2() throws Exception {
        String queryStr =
                "select * from (select k2, count(k3) from test.colocate1 group by k2) t1 left join (select "
                        + "k1, k2, count(k3) from test.colocate2 group by k1, k2) t2 on  "
                        + "t1.k2 = t2.k2";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));
    }

    @Test
    public void testCrossJoinEliminate() throws Exception {
        String query = "select t1.* from t0, t2, t3, t1 where t1.v4 = t2.v7 " +
                "and t1.v4 = t3.v1 and t3.v1 = t0.v1";
        String explainString = getFragmentPlan(query);
        Assert.assertFalse(explainString.contains("CROSS JOIN"));
    }

    @Test
    public void testSemiJoinPushDown() throws Exception {
        String sql = "SELECT *\n"
                + "FROM (\n"
                + "    SELECT t0.v1, t0.v2, t0.v3\n"
                + "    FROM t0\n"
                + ") subt0\n"
                + "    LEFT SEMI JOIN (\n"
                + "        SELECT t1.v4, t1.v5, t1.v6\n"
                + "        FROM t1\n"
                + "    ) subt1\n"
                + "    ON subt0.v1 = subt1.v4\n"
                + "        AND subt0.v2 != subt0.v2\n"
                + "        AND subt0.v2 = subt1.v5\n"
                + "        AND (subt0.v3 <= subt0.v3 < subt1.v6) = (subt1.v5)\n";

        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  colocate: false, reason: \n"
                + "  |  equal join conjunct: 4: v4 = 1: v1\n"
                + "  |  equal join conjunct: 5: v5 = 2: v2\n"
                + "  |  other join predicates: CAST(CAST(3: v3 <= 3: v3 AS BIGINT) < 6: v6 AS BIGINT) = 5: v5\n"));
    }

    @Test
    public void testInnerJoinPushDown() throws Exception {
        String sql = "SELECT *\n"
                + "FROM (\n"
                + "    SELECT t0.v1, t0.v2, t0.v3\n"
                + "    FROM t0\n"
                + ") subt0\n"
                + "    INNER JOIN (\n"
                + "        SELECT t1.v4, t1.v5, t1.v6\n"
                + "        FROM t1\n"
                + "    ) subt1\n"
                + "    ON subt0.v1 = subt1.v4\n"
                + "        AND subt0.v2 != subt0.v2\n"
                + "        AND subt0.v2 = subt1.v5\n"
                + "        AND (subt0.v3 <= subt0.v3 < subt1.v6) = (subt1.v5)\n";

        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  colocate: false, reason: \n"
                + "  |  equal join conjunct: 4: v4 = 1: v1\n"
                + "  |  equal join conjunct: 5: v5 = 2: v2\n"
                + "  |  other join predicates: CAST(CAST(3: v3 <= 3: v3 AS BIGINT) < 6: v6 AS BIGINT) = 5: v5\n"));
    }

    @Test
    public void testOuterJoinToInnerWithCast() throws Exception {
        String sql = "select * from test_all_type a left join test_all_type b on a.t1c = b.t1c " +
                "where b.id_date = '2021-05-19'";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("join op: INNER JOIN"));
    }

    @Test
    public void testUsingJoin() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select * from t0 as x0 join t0 as x1 using(v1);";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("  2:HASH JOIN\n"
                + "  |  join op: INNER JOIN (COLOCATE)\n"
                + "  |  hash predicates:\n"
                + "  |  colocate: true\n"
                + "  |  equal join conjunct: 1: v1 = 4: v1"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testJoinAssociativityConst() throws Exception {
        String sql = "SELECT x0.*\n" +
                "FROM (\n" +
                "    SELECT 49 AS v0, v1\n" +
                "    FROM t0\n" +
                "    WHERE v1 is not null\n" +
                ") x0\n" +
                "    INNER JOIN test_all_type s0 ON x0.v0 = s0.t1a\n" +
                "    INNER JOIN tall l1 ON x0.v0 = l1.tf\n" +
                "\n" +
                "WHERE l1.tc < s0.t1c";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 49\n" +
                "  |  <slot 26> : CAST(49 AS VARCHAR(1048576))\n" +
                "  |  \n" +
                "  0:OlapScanNode"));
    }

    @Test
    public void testFourTableShuffleBucketShuffle() throws Exception {
        // check top join use shuffle bucket join
        //                   join(shuffle bucket)
        //                   /                  \
        //              join(partitioned)   join(partitioned)
        String sql = "with join1 as (\n" +
                "  select * from t2 join t3 on v7=v1\n" +
                "), \n" +
                "join2 as (\n" +
                "  select * from t0 join t1 on v1=v4\n" +
                ")\n" +
                "SELECT \n" +
                "  * \n" +
                "from \n" +
                "  join1 \n" +
                "  inner join[shuffle] join2 on v4 = v7;";

        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("10:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))"));
        Assert.assertTrue(plan.contains("4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)"));
        Assert.assertTrue(plan.contains("9:HASH JOIN\n" +
                "  |    |  join op: INNER JOIN (PARTITIONED)"));
    }

    @Test
    public void testSemiReorder() throws Exception {
        String sql = "select 0 from t0,t1 left semi join t2 on v4 = v7";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:10: expr\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  8:Project\n" +
                "  |  <slot 10> : 0\n" +
                "  |  \n"));
    }

    @Test
    public void testEmptyNodeWithJoin() throws Exception {
        // check no exception
        String sql = "SELECT\n" +
                "        subq_0.c3, ref_2.id_datetime        \n" +
                "FROM (\n" +
                "        SELECT\n" +
                "                ref_0.id_date AS c3\n" +
                "        FROM\n" +
                "                test_all_type AS ref_0 WHERE FALSE) AS subq_0\n" +
                "        INNER JOIN test_all_type AS ref_1 ON (subq_0.c3 = ref_1.id_date)\n" +
                "        INNER JOIN test_all_type AS ref_2 ON (subq_0.c3 = ref_2.id_datetime)\n" +
                "WHERE\n" +
                "        ref_2.t1a >= ref_1.t1a";
        String plan = getFragmentPlan(sql);
    }

    @Test
    public void testJoinReorderWithExpressions() throws Exception {
        Config.enable_decimal_v3 = true;
        String sql = "SELECT t2.*\n" +
                "FROM t2,(\n" +
                "    SELECT *\n" +
                "    FROM t1 \n" +
                "    WHERE false) subt1 \n" +
                "    LEFT OUTER JOIN (\n" +
                "        SELECT *\n" +
                "        FROM t3 \n" +
                "        WHERE CAST(t3.v1 AS BOOLEAN) BETWEEN (t3.v2) AND (t3.v2) ) subt3 \n" +
                "    ON subt1.v4 = subt3.v1 AND subt1.v4 >= subt3.v1 AND subt1.v5 > subt3.v1 AND subt1.v5 = subt3.v1 \n" +
                "WHERE (subt1.v5 BETWEEN subt1.v5 AND CAST(subt1.v5 AS DECIMAL64)) = subt3.v2;";

        RuleSet mockRule = new RuleSet() {
            @Override
            public void addJoinTransformationRules() {
                this.getTransformRules().clear();
                this.getTransformRules().add(JoinAssociativityRule.getInstance());
            }
        };

        new MockUp<OptimizerContext>() {
            @Mock
            public RuleSet getRuleSet() {
                return mockRule;
            }
        };

        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PLAN FRAGMENT 3\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    HASH_PARTITIONED: 4: v4, 5: v5, 10: cast\n" +
                "\n" +
                "  1:EMPTYSET"));
        Config.enable_decimal_v3 = false;
    }

    @Test
    public void testFullOuterJoinOutputRowCount() throws Exception {
        Config.enable_decimal_v3 = true;
        String sql = "SELECT\n" +
                "    (NOT(FALSE))\n" +
                "FROM (\n" +
                "    SELECT t0.v1,t0.v2,t0.v3 \n" +
                "    FROM t0\n" +
                "    WHERE (t0.v1) BETWEEN(CAST(t0.v2 AS DECIMAL64)) AND(t0.v1)) subt0\n" +
                "    FULL OUTER JOIN (\n" +
                "    SELECT t1.v4, t1.v5, t1.v6\n" +
                "    FROM t1\n" +
                "    WHERE TRUE) subt1 ON subt0.v3 = subt1.v6\n" +
                "    AND subt0.v1 > ((1808124905) % (1336789350))\n" +
                "WHERE\n" +
                "    BITMAP_CONTAINS (bitmap_hash (\"dWyMZ\"), ((- 817000778) - (- 809159836)))\n" +
                "GROUP BY\n" +
                "    1.38432132E8, \"1969-12-20 10:26:22\"\n" +
                "HAVING (COUNT(NULL))\n" +
                "IN(- 1210205071)\n";
        String plan = getFragmentPlan(sql);
        // Just make sure we can get the final plan, and not crashed because of stats calculator error.
        System.out.println(sql);
    }

    @Test
    public void testSelectConstantFormJoin() throws Exception {
        String sql = "SELECT \n" +
                "  * \n" +
                "from \n" +
                "  (\n" +
                "    select \n" +
                "      ref_0.t1c as c5, \n" +
                "      37 as c6 \n" +
                "    from \n" +
                "      test_all_type as ref_0 \n" +
                "      inner join test_all_type as ref_1 on (\n" +
                "        ref_0.t1f = ref_1.t1f\n" +
                "      ) \n" +
                "    where \n" +
                "      ref_0.t1c <> ref_0.t1c\n" +
                "  ) as subq_0 \n" +
                "  inner join part as ref_2 on (subq_0.c5 = ref_2.P_PARTKEY) \n" +
                "  inner join supplier as ref_3 on (subq_0.c5 = ref_3.S_SUPPKEY) \n" +
                "where \n" +
                "  (\n" +
                "    (ref_3.S_NAME > ref_2.P_TYPE) \n" +
                "    and (true)\n" +
                "  ) \n" +
                "  and (\n" +
                "    (subq_0.c6 = ref_3.S_NATIONKEY) \n" +
                "    and (true)\n" +
                "  ) \n" +
                "limit \n" +
                "  45;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("6:Project\n" +
                "  |  <slot 3> : 3: t1c\n" +
                "  |  <slot 40> : CAST(37 AS INT)"));
    }

    @Test
    public void testNullSafeEqualJoin() throws Exception {
        String sql = "select * from t0 join t1 on t0.v3 <=> t1.v4";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("equal join conjunct: 3: v3 <=> 4: v4"));

        sql = "select * from t0 left join t1 on t0.v3 <=> t1.v4";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("equal join conjunct: 3: v3 <=> 4: v4"));
    }

    @Test
    public void testColocateHint() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select * from t0 as x0 inner join t0 as x1 on x0.v1 = x1.v1;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: INNER JOIN (COLOCATE)\n"
                + "  |  hash predicates:\n"
                + "  |  colocate: true"));

        sql = "select * from t0 as x0 inner join[shuffle] t0 as x1 on x0.v1 = x1.v1;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: INNER JOIN (PARTITIONED)\n"
                + "  |  hash predicates:\n"
                + "  |  colocate: false, reason: "));

        sql = "select * from t0 as x0 inner join[colocate] t0 as x1 on x0.v1 = x1.v1;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: INNER JOIN (COLOCATE)\n"
                + "  |  hash predicates:\n"
                + "  |  colocate: true"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testBucketHint() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select * from t0 as x0 inner join t1 as x1 on x0.v1 = x1.v4;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n"
                + "  |  hash predicates:\n"
                + "  |  colocate: false, reason: "));

        sql = "select * from t0 as x0 inner join[shuffle] t1 as x1 on x0.v1 = x1.v4;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: INNER JOIN (PARTITIONED)\n"
                + "  |  hash predicates:\n"
                + "  |  colocate: false, reason: "));

        sql = "select * from t0 as x0 inner join[bucket] t1 as x1 on x0.v1 = x1.v4;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n"
                + "  |  hash predicates:\n"
                + "  |  colocate: false, reason: "));
        FeConstants.runningUnitTest = false;
    }



}
