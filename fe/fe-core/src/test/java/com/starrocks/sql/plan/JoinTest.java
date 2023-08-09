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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.rule.RuleSet;
import com.starrocks.sql.optimizer.rule.transformation.JoinAssociativityRule;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class JoinTest extends PlanTestBase {

    @Test
    public void testColocateDistributeSatisfyShuffleColumns() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select * from colocate1 left join colocate2 on colocate1.k1=colocate2.k1;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "colocate: false");
        assertContains(plan, "join op: LEFT OUTER JOIN (BROADCAST)");

        sql = "select * from colocate1 left join colocate2 on colocate1.k1=colocate2.k1 and colocate1.k2=colocate2.k2;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "colocate: true");
        assertContains(plan, "join op: LEFT OUTER JOIN (COLOCATE)");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testInnerJoinWithPredicate() throws Exception {
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 1;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 1: v1 = 1");
    }

    @Test
    public void testInnerJoinWithConstPredicate() throws Exception {
        String sql = "SELECT * from t0 join test_all_type on NOT NULL >= NULL";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "  0:EMPTYSET\n");
    }

    @Test
    public void testInnerJoinWithCastPredicate() throws Exception {
        String sql = "SELECT t0.v1 from t0 join test_all_type on t0.v1 = test_all_type.t1c";
        getFragmentPlan(sql);
    }

    @Test
    public void testCrossJoinWithPredicate() throws Exception {
        String sql = "SELECT * from t0 join test_all_type where t0.v1 = 2;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 1: v1 = 2");
    }

    @Test
    public void testLeftOuterJoinWithPredicate() throws Exception {
        String sql = "SELECT * from t0 left join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 > 1;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 1: v1 > 1");
    }

    @Test
    public void testCrossJoinToInnerJoin() throws Exception {
        String sql = "SELECT t0.v1 from t0, test_all_type where t0.v1 = test_all_type.t1d";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "join op: INNER JOIN");
        assertContains(plan, "equal join conjunct: 1: v1 = 7: t1d");
    }

    @Test
    public void testJoinColumnsPrune() throws Exception {
        String sql = " select count(a.v3) from t0 a join t0 b on a.v3 = b.v3;";
        getFragmentPlan(sql);

        sql = " select a.v2 from t0 a join t0 b on a.v3 = b.v3;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "4:Project\n"
                + "  |  <slot 2> : 2: v2");
    }

    @Test
    public void testCrossJoin() throws Exception {
        String sql = "SELECT * from t0 join test_all_type;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan, plan.contains("  3:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n"));

        sql = "select * from t0 join test_all_type on NOT 69 IS NOT NULL where true";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan,
                plan.contains("  3:NESTLOOP JOIN\n" +
                        "  |  join op: CROSS JOIN\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  \n" +
                        "  |----2:EXCHANGE\n" +
                        "  |    \n" +
                        "  0:EMPTYSET"));
    }

    @Test
    public void testFullOuterJoin() throws Exception {
        String sql = "select * from t0 full outer join t1 on t0.v1 = t1.v4 where abs(1) > 2;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "     TABLE: t1\n"
                + "     PREAGGREGATION: ON\n"
                + "     PREDICATES: abs(1) > 2");
        assertContains(plan, "     TABLE: t0\n"
                + "     PREAGGREGATION: ON\n"
                + "     PREDICATES: abs(1) > 2");
    }

    @Test
    public void testFullOuterJoinPredicatePushDown() throws Exception {
        String sql = "select * from t0 full outer join t1 on t0.v1 = t1.v4 " +
                " where (NOT (t0.v2 IS NOT NULL))";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "other predicates: 2: v2 IS NULL");
    }

    @Test
    public void testRightSemiJoinWithFilter() throws Exception {
        String sql = "select t1.v4 from t0 right semi join t1 on t0.v1 = t1.v4 and t0.v1 > 1 ";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 1: v1 > 1");
    }

    @Test
    public void testColocateJoin() throws Exception {
        FeConstants.runningUnitTest = true;
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
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testColocateJoinWithOneAggChild() throws Exception {
        FeConstants.runningUnitTest = true;
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
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testColocateJoinWithTwoAggChild() throws Exception {
        FeConstants.runningUnitTest = true;
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
        FeConstants.runningUnitTest = false;
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
                "and t1.v4 = t3.v10 and t3.v10 = t0.v1";
        String explainString = getFragmentPlan(query);
        Assert.assertFalse(explainString.contains("NESTLOOP JOIN"));
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
        assertContains(plan, "|  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  equal join conjunct: 2: v2 = 5: v5\n" +
                "  |  other join predicates: CAST(CAST(3: v3 <= 3: v3 AS BIGINT) < 6: v6 AS BIGINT) = 5: v5");
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
        assertContains(plan, "|  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  equal join conjunct: 2: v2 = 5: v5\n" +
                "  |  other join predicates: CAST(CAST(3: v3 <= 3: v3 AS BIGINT) < 6: v6 AS BIGINT) = 5: v5");
    }

    @Test
    public void testOuterJoinToInnerWithCast() throws Exception {
        String sql = "select * from test_all_type a left join test_all_type b on a.t1c = b.t1c " +
                "where b.id_date = '2021-05-19'";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "join op: INNER JOIN");
    }

    @Test
    public void testUsingJoin() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select * from t0 as x0 join t0 as x1 using(v1);";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  2:HASH JOIN\n"
                + "  |  join op: INNER JOIN (COLOCATE)\n"
                + "  |  colocate: true\n"
                + "  |  equal join conjunct: 1: v1 = 4: v1");
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
        assertContains(plan, "  1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 49\n" +
                "  |  <slot 26> : CAST(49 AS VARCHAR(1048576))\n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }

    @Test
    public void testFourTableShuffleBucketShuffle() throws Exception {
        // check top join use shuffle bucket join
        //                   join(shuffle bucket)
        //                   /                  \
        //              join(partitioned)   join(partitioned)
        String sql = "with join1 as (\n" +
                "  select * from t2 join t3 on v7=v10\n" +
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
        assertContains(plan, "10:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))");
        assertContains(plan, "4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)");
        assertContains(plan, "9:HASH JOIN\n" +
                "  |    |  join op: INNER JOIN (PARTITIONED)");
    }

    @Test
    public void testShuffleBucketShuffle() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql =
                "select a.v1, a.v4 from (select v1, v2, v4 from t0 join[shuffle] t1 on t0.v1 = t1.v4) a join[shuffle] " +
                        "(select v7,v8, v10 from t2 join[shuffle] t3 on t2.v7 = t3.v10) b on a.v1 = b.v7 and a.v2 = b.v8";
        String plan = getFragmentPlan(sql);
        assertContains(plan, " 11:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 7: v7\n" +
                "  |  equal join conjunct: 2: v2 = 8: v8\n" +
                "  |  \n" +
                "  |----10:Project\n" +
                "  |    |  <slot 7> : 7: v7\n" +
                "  |    |  <slot 8> : 8: v8\n" +
                "  |    |  \n" +
                "  |    9:HASH JOIN\n" +
                "  |    |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |    |  colocate: false, reason: \n" +
                "  |    |  equal join conjunct: 7: v7 = 10: v10\n" +
                "  |    |  \n" +
                "  |    |----8:EXCHANGE\n" +
                "  |    |    \n" +
                "  |    6:EXCHANGE\n" +
                "  |    \n" +
                "  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4");

        sql = "select a.v1, a.v4 from (select v1, v2, v4 from t0 join[shuffle] t1 on t0.v1 = t1.v4) a join[shuffle] " +
                "(select v7,v8, v10 from t2 join[shuffle] t3 on t2.v8 = t3.v10) b on a.v1 = b.v7 and a.v2 = b.v8";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  12:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 7: v7\n" +
                "  |  equal join conjunct: 2: v2 = 8: v8\n" +
                "  |  \n" +
                "  |----11:EXCHANGE\n" +
                "  |    \n" +
                "  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n");
        sql = "select a.v1, a.v4 from (select v1, v2, v4 from t0 join[shuffle] t1 on t0.v1 = t1.v4) a join[shuffle] " +
                "(select v7,v8 from t2) b on a.v1 = b.v7 and a.v2 = b.v8";
        plan = getFragmentPlan(sql);
        assertContains(plan, " 7:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 7: v7\n" +
                "  |  equal join conjunct: 2: v2 = 8: v8\n" +
                "  |  \n" +
                "  |----6:EXCHANGE\n" +
                "  |    \n" +
                "  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4");
        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 06\n" +
                "    HASH_PARTITIONED: 7: v7");

        sql = "select a.v1, a.v4 from (select v7,v8 from t2) b " +
                "join[shuffle] (select v1, v2, v4 from t0 join[shuffle] t1 on t0.v2 = t1.v4) a " +
                "on a.v1 = b.v7 and a.v2 = b.v8";
        plan = getFragmentPlan(sql);
        assertContains(plan, " 8:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v7 = 4: v1\n" +
                "  |  equal join conjunct: 2: v8 = 5: v2\n" +
                "  |  \n" +
                "  |----7:EXCHANGE\n" +
                "  |    \n" +
                "  1:EXCHANGE");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testSemiReorder() throws Exception {
        String sql = "select 0 from t0,t1 left semi join t2 on v4 = v7";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:10: expr\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  9:Project\n" +
                "  |  <slot 10> : 0\n" +
                "  |  \n");
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
        String sql = "SELECT t2.*\n" +
                "FROM t2,(\n" +
                "    SELECT *\n" +
                "    FROM t1 \n" +
                "    WHERE false) subt1 \n" +
                "    LEFT OUTER JOIN (\n" +
                "        SELECT *\n" +
                "        FROM t3 \n" +
                "        WHERE CAST(t3.v10 AS BOOLEAN) BETWEEN (t3.v11) AND (t3.v11) ) subt3 \n" +
                "    ON subt1.v4 = subt3.v10 AND subt1.v4 >= subt3.v10 AND subt1.v5 > subt3.v10 AND subt1.v5 = subt3.v10 \n" +
                "WHERE (subt1.v5 BETWEEN subt1.v5 AND CAST(subt1.v5 AS DECIMAL64)) = subt3.v11;";

        RuleSet mockRule = new RuleSet() {
            @Override
            public void addJoinTransformationRules() {
                this.getTransformRules().clear();
                this.getTransformRules().add(JoinAssociativityRule.INNER_JOIN_ASSOCIATIVITY_RULE);
            }
        };

        new MockUp<OptimizerContext>() {
            @Mock
            public RuleSet getRuleSet() {
                return mockRule;
            }
        };

        String plan = getFragmentPlan(sql);
        assertContains(plan, "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 06\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  5:Project\n" +
                "  |  <slot 12> : 1\n" +
                "  |  \n" +
                "  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v4 = 7: v10\n" +
                "  |  equal join conjunct: 5: v5 = 7: v10\n" +
                "  |  equal join conjunct: 10: cast = 8: v11\n" +
                "  |  other join predicates: 4: v4 >= 7: v10, 5: v5 > 7: v10\n" +
                "  |  \n" +
                "  |----3:EXCHANGE\n" +
                "  |    \n" +
                "  1:EMPTYSET");
    }

    @Test
    public void testFullOuterJoinOutputRowCount() throws Exception {
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
        assertContains(plan, "9:Project\n" +
                "  |  <slot 3> : 3: t1c\n" +
                "  |  <slot 21> : 21: expr\n" +
                "  |  <slot 22> : 22: P_PARTKEY\n" +
                "  |  <slot 23> : 23: P_NAME\n" +
                "  |  <slot 24> : 24: P_MFGR\n" +
                "  |  <slot 25> : 25: P_BRAND\n" +
                "  |  <slot 26> : 26: P_TYPE\n" +
                "  |  <slot 27> : 27: P_SIZE\n" +
                "  |  <slot 28> : 28: P_CONTAINER\n" +
                "  |  <slot 29> : 29: P_RETAILPRICE\n" +
                "  |  <slot 30> : 30: P_COMMENT\n" +
                "  |  <slot 31> : 31: PAD\n" +
                "  |  <slot 40> : CAST(21: expr AS INT)");
    }

    @Test
    public void testNullSafeEqualJoin() throws Exception {
        String sql = "select * from t0 join t1 on t0.v3 <=> t1.v4";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "equal join conjunct: 3: v3 <=> 4: v4");

        sql = "select * from t0 left join t1 on t0.v3 <=> t1.v4";
        plan = getFragmentPlan(sql);
        assertContains(plan, "equal join conjunct: 3: v3 <=> 4: v4");
    }

    @Test
    public void testColocateHint() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select * from t0 as x0 inner join t0 as x1 on x0.v1 = x1.v1;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  |  join op: INNER JOIN (COLOCATE)\n"
                + "  |  colocate: true");

        sql = "select * from t0 as x0 inner join[shuffle] t0 as x1 on x0.v1 = x1.v1;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  |  join op: INNER JOIN (PARTITIONED)\n"
                + "  |  colocate: false, reason: ");

        sql = "select * from t0 as x0 inner join[colocate] t0 as x1 on x0.v1 = x1.v1;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  |  join op: INNER JOIN (COLOCATE)\n"
                + "  |  colocate: true");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testBucketHint() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select * from t0 as x0 inner join t1 as x1 on x0.v1 = x1.v4;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n"
                + "  |  colocate: false, reason: ");

        sql = "select * from t0 as x0 inner join[shuffle] t1 as x1 on x0.v1 = x1.v4;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  |  join op: INNER JOIN (PARTITIONED)\n"
                + "  |  colocate: false, reason: ");

        sql = "select * from t0 as x0 inner join[bucket] t1 as x1 on x0.v1 = x1.v4;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n"
                + "  |  colocate: false, reason: ");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testJoinOnInDatePredicate() throws Exception {
        String sql =
                "select a.id_datetime from test_all_type as a join test_all_type as b where a.id_date in (b.id_date)";
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "CAST(9: id_date AS DATETIME)");
        assertContains(plan, "equal join conjunct: 9: id_date = 19: id_date");
    }

    @Test
    public void testOnlyCrossJoin() throws Exception {
        String sql = "select * from t0 as x0 join t1 as x1 on (1 = 2) is not null;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "3:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n");
    }

    @Test
    public void testSemiJoinReorder() throws Exception {
        String sql = "SELECT v2 \n" +
                "FROM t0 \n" +
                "WHERE v1 IN (SELECT v2 \n" +
                "             FROM t0 \n" +
                "             WHERE (v2 IN (SELECT v1 FROM t0) \n" +
                "                   OR (v2 IN (SELECT v1 FROM t0))) \n" +
                "             AND (v3 IN (SELECT v1 FROM t0)));";
        // check no exception
        String plan = getFragmentPlan(sql);
        assertContains(plan, "11:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 5: v2 = 24: v1");
    }

    @Test
    public void testJoinOutput() throws Exception {
        String sql = "select v1,v4 from t0, t1 where v2 = v5";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "output columns: 1, 4");

        sql = "select v1+1,v4 from t0, t1 where v2 = v5";
        plan = getVerboseExplain(sql);
        assertContains(plan, "output columns: 1, 4");

        sql = "select v2+1,v4 from t0, t1 where v2 = v5";
        plan = getVerboseExplain(sql);
        assertContains(plan, "output columns: 2, 4");

        sql = "select v1+1,v4 from t0, t1 where v2 = v5 and v3 > v6";
        plan = getVerboseExplain(sql);
        assertContains(plan, "output columns: 1, 3, 4, 6");

        sql = "select (v2+v6 = 1 or v2+v6 = 5) from t0, t1 where v2 = v5 ";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  4:Project\n" +
                "  |  output columns:\n" +
                "  |  7 <-> (8: add = 1) OR (8: add = 5)\n" +
                "  |  common expressions:\n" +
                "  |  8 <-> [2: v2, BIGINT, true] + [6: v6, BIGINT, true]\n" +
                "  |  cardinality: 1");
        assertContains(plan, "output columns: 2, 6");

        sql = "select * from t0,t1 where v1 = v4";
        plan = getVerboseExplain(sql);
        assertNotContains(plan, "output columns");
    }

    @Test
    public void testSemiJoinReorderWithProject() throws Exception {
        String sql = "select x1.s1 from " +
                "(select t0.v1 + 1 as s1, t0.v2 from t0 left join t1 on t0.v2 = t1.v4) as x1 " +
                "left semi join t2 on x1.v2 = t2.v7";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  4:Project\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 7> : 1: v1 + 1\n" +
                "  |  \n" +
                "  3:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN");
    }

    @Test
    public void testCrossJoinOnPredicate() throws Exception {
        String sql = "select * from t0 cross join t1 on t0.v1 != t1.v4 and t0.v2 != t1.v5";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  3:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: 1: v1 != 4: v4, 2: v2 != 5: v5\n");
    }

    @Test
    public void testCrossJoinCastToInner() throws Exception {
        String sql = "select * from t0 cross join t1 on t0.v1 = t1.v4 and t0.v2 != t1.v5";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  other join predicates: 2: v2 != 5: v5");
    }

    @Test
    public void testJoinPushBitmapCount() throws Exception {
        String sql = "SELECT 1 FROM t0 LEFT OUTER JOIN t1 ON t0.v1=t1.v4 " +
                "WHERE NOT CAST(bitmap_count(CASE WHEN t1.v4 in (10000) THEN bitmap_hash('abc') END) AS BOOLEAN)";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "join op: LEFT OUTER JOIN (BROADCAST)");
        assertContains(plan,
                "other predicates: NOT (CAST(bitmap_count(if(4: v4 = 10000, bitmap_hash('abc'), NULL)) AS BOOLEAN))");
    }

    @Test
    public void testShuffleColumnsAdjustOrders() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select t0.v1, t1.v4, t2.v7 from t0 join[shuffle] t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5 " +
                "join[shuffle] t2 on t0.v2 = t2.v8 and t0.v1 = t2.v7";
        String plan = getFragmentPlan(sql);
        assertContains(plan, " 8:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 8: v8\n" +
                "  |  equal join conjunct: 1: v1 = 7: v7");
        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 5: v5, 4: v4");
        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    HASH_PARTITIONED: 2: v2, 1: v1");

        sql = "select t0.v1, t1.v4, t2.v7 from t0 join[shuffle] t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5 " +
                "join[shuffle] t2 on t0.v2 = t2.v8 and t0.v1 = t2.v7 join[shuffle] t3 on t2.v7 = t3.v10 and t2.v8 = t3.v11 ";
        plan = getFragmentPlan(sql);
        assertContains(plan, "12:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 7: v7 = 10: v10\n" +
                "  |  equal join conjunct: 8: v8 = 11: v11");
        assertContains(plan, "8:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 8: v8\n" +
                "  |  equal join conjunct: 1: v1 = 7: v7");
        assertContains(plan, " STREAM DATA SINK\n" +
                "    EXCHANGE ID: 07\n" +
                "    HASH_PARTITIONED: 7: v7, 8: v8");
        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    HASH_PARTITIONED: 1: v1, 2: v2");

        sql = "select t0.v1, t1.v4, t2.v7 from t0 join[shuffle] t1 on t0.v2 = t1.v5 and t0.v1 = t1.v4 " +
                "join[shuffle] t2 on t0.v2 = t2.v8 and t0.v1 = t2.v7 join[shuffle] t3 on t2.v7 = t3.v10 and t2.v8 = t3.v11 ";
        plan = getFragmentPlan(sql);
        assertContains(plan, " 8:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 8: v8\n" +
                "  |  equal join conjunct: 1: v1 = 7: v7");
        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    HASH_PARTITIONED: 1: v1, 2: v2");
        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 4: v4, 5: v5");

        sql = "select t0.v1, t1.v4, t2.v7 from t0 join[shuffle] t1 on t0.v2 = t1.v5 and t0.v1 = t1.v4 " +
                "join[shuffle] t2 on t0.v2 = t2.v8 and t0.v1 = t2.v7 join[shuffle] t3 on t0.v1 = t3.v10 and t0.v2 = t3.v11 ";
        plan = getFragmentPlan(sql);
        assertContains(plan, "12:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 10: v10\n" +
                "  |  equal join conjunct: 2: v2 = 11: v11");
        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    HASH_PARTITIONED: 1: v1, 2: v2");
        assertContains(plan, " STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 4: v4, 5: v5");

        sql = "select * from t0 left join[shuffle] (\n" +
                "    select t1.* from t1 left join[shuffle] t2 \n" +
                "    on t1.v4 = t2.v7 \n" +
                "    and t1.v6 = t2.v9 \n" +
                "    and t1.v5 = t2.v8) as j2\n" +
                "on t0.v3 = j2.v6\n" +
                "  and t0.v1 = j2.v4\n" +
                "  and t0.v2 = j2.v5;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "8:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 3: v3 = 6: v6\n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  equal join conjunct: 2: v2 = 5: v5");
        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 6: v6, 4: v4, 5: v5");
        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 05\n" +
                "    HASH_PARTITIONED: 9: v9, 7: v7, 8: v8");

        sql = "select a.v1, a.v4, b.v7, b.v10 " +
                "from (select v1, v2, v4 from t0 join[shuffle] t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5) a join[shuffle] " +
                "(select v7, v8, v10 from t2 join[shuffle] t3 on t2.v7 = t3.v10 and t2.v8 = t3.v11) b " +
                "on a.v2 = b.v8 and a.v1 = b.v7";
        plan = getFragmentPlan(sql);
        assertContains(plan, "12:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 8: v8\n" +
                "  |  equal join conjunct: 1: v1 = 7: v7");
        assertContains(plan, " STREAM DATA SINK\n" +
                "    EXCHANGE ID: 09\n" +
                "    HASH_PARTITIONED: 11: v11, 10: v10");
        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 07\n" +
                "    HASH_PARTITIONED: 8: v8, 7: v7");
        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 5: v5, 4: v4");
        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    HASH_PARTITIONED: 2: v2, 1: v1");

        // check can not adjust column orders
        sql = "select a.v1, a.v4, b.v7, b.v10 from " +
                "(select v1, v2, v4, v5 from t0 join[shuffle] t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5) a join[shuffle] " +
                "(select v7, v8, v10, v11 from t2 join[shuffle] t3 on t2.v7 = t3.v10 and t2.v8 = t3.v11) b " +
                "on a.v2 = b.v8 and a.v4 = b.v8";
        plan = getFragmentPlan(sql);
        assertContains(plan, "14:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 8: v8\n" +
                "  |  equal join conjunct: 4: v4 = 8: v8");
        assertContains(plan, " 11:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 7: v7 = 10: v10\n" +
                "  |  equal join conjunct: 8: v8 = 11: v11");
        assertContains(plan, " STREAM DATA SINK\n" +
                "    EXCHANGE ID: 08\n" +
                "    HASH_PARTITIONED: 7: v7, 8: v8");
        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 10\n" +
                "    HASH_PARTITIONED: 10: v10, 11: v11");

        // check can not adjust column orders
        sql = "select a.v1, a.v4, b.v7, b.v10 " +
                "from (select v1, v2, v4, v5 from t0 join[shuffle] t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5) a join[shuffle] " +
                "(select v7, v8, v10, v11 from t2 join[shuffle] t3 on t2.v7 = t3.v10 and t2.v8 = t3.v11) b " +
                "on a.v2 = b.v8 and a.v4 = b.v10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "14:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 8: v8\n" +
                "  |  equal join conjunct: 4: v4 = 10: v10");
        assertContains(plan, "11:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 7: v7 = 10: v10\n" +
                "  |  equal join conjunct: 8: v8 = 11: v11");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testJoinOnPredicateRewrite() throws Exception {
        String sql = "select * from t0 left outer join t1 on v1=v4 and cast(v2 as bigint) = v5 and false";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "equal join conjunct: 1: v1 = 4: v4");
        assertContains(plan, "1:EMPTYSET");
    }

    @Test
    public void testSemiJoinFalsePredicate() throws Exception {
        String sql = "select * from t0 left semi join t3 on t0.v1 = t3.v10 " +
                "AND CASE WHEN NULL THEN t0.v1 ELSE '' END = " +
                "CASE WHEN true THEN 'fGrak3iTt' WHEN false THEN t3.v10 ELSE 'asf' END";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "|  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v10");
    }

    @Test
    public void testJoinReorderWithPredicate() throws Exception {
        connectContext.getSessionVariable().setMaxTransformReorderJoins(2);
        String sql = "select t0.v1 from t0, t1, t2, t3 where t0.v1 + t3.v10 = 2";
        String plan = getFragmentPlan(sql);
        connectContext.getSessionVariable().setMaxTransformReorderJoins(4);
        assertContains(plan, "13:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: 1: v1 + 10: v10 = 2");
    }

    @Test
    public void testOuterJoinBucketShuffle() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "SELECT DISTINCT t0.v1 FROM t0 RIGHT JOIN[BUCKET] t1 ON t0.v1 = t1.v4";
        String plan = getFragmentPlan(sql);
        assertContains(plan, " 5:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 1: v1\n" +
                "  |  \n" +
                "  4:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  \n" +
                "  3:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (BUCKET_SHUFFLE)");

        sql = "SELECT DISTINCT t0.v1 FROM t0 FULL JOIN[BUCKET] t1 ON t0.v1 = t1.v4";
        plan = getFragmentPlan(sql);
        assertContains(plan, "3:HASH JOIN\n" +
                "  |  join op: FULL OUTER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0");

        sql = "SELECT DISTINCT t1.v4 FROM t0 LEFT JOIN[BUCKET] t1 ON t0.v1 = t1.v4";
        plan = getFragmentPlan(sql);
        assertContains(plan, "7:AGGREGATE (merge finalize)\n" +
                "  |  group by: 4: v4\n" +
                "  |  \n" +
                "  6:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 06\n" +
                "    HASH_PARTITIONED: 4: v4\n" +
                "\n" +
                "  5:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 4: v4\n" +
                "  |  \n" +
                "  4:Project\n" +
                "  |  <slot 4> : 4: v4\n" +
                "  |  \n" +
                "  3:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BUCKET_SHUFFLE)");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testSemiJoinPredicateDerive() throws Exception {
        String sql = "select * from t0 left semi join t1 on v1 = v4 where v1 = 2";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 1: v1 = 2");
    }

    @Test
    public void testShuffleHashBucket() throws Exception {
        String sql = "SELECT COUNT(*)\n" +
                "FROM lineitem JOIN [shuffle] orders o1 ON l_orderkey = o1.o_orderkey\n" +
                "JOIN [shuffle] orders o2 ON l_orderkey = o2.o_orderkey";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "join op: INNER JOIN (BUCKET_SHUFFLE(S))");
    }

    @Test
    public void testShuffleHashBucket2() throws Exception {
        String sql = "select count(1) from lineitem t1 join [shuffle] orders t2 on " +
                "t1.l_orderkey = t2.o_orderkey and t2.O_ORDERDATE = t1.L_SHIPDATE join [shuffle] orders t3 " +
                "on t1.l_orderkey = t3.o_orderkey and t3.O_ORDERDATE = t1.L_SHIPDATE join [shuffle] orders t4 on\n" +
                "t1.l_orderkey = t4.o_orderkey and t4.O_ORDERDATE = t1.L_SHIPDATE;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "12:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))");
        assertContains(plan, "8:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))");
        assertContains(plan, "4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)");
    }

    @Test
    public void testJoinReorderTakeEffect() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        Database db = globalStateMgr.getDb("test");
        Table table = db.getTable("join2");
        OlapTable olapTable1 = (OlapTable) table;
        new Expectations(olapTable1) {
            {
                olapTable1.getRowCount();
                result = 2L;
                minTimes = 0;
            }
        };
        String sql = "select * from join1 join join2 on join1.id = join2.id;";
        String explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: id = 5: id"));
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join1"));
        Assert.assertTrue(explainString.contains("  1:OlapScanNode\n" +
                "     TABLE: join2"));
    }

    @Test
    public void testJoinReorderWithWithClause() throws Exception {
        connectContext.setDatabase("test");
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        Table table = globalStateMgr.getDb("test").getTable("join2");
        OlapTable olapTable1 = (OlapTable) table;
        new Expectations(olapTable1) {
            {
                olapTable1.getRowCount();
                result = 2L;
                minTimes = 0;
            }
        };
        String sql = "WITH t_temp AS (select join1.id as id1, " +
                "join2.id as id2 from join1 join join2 on join1.id = join2.id) select * from t_temp";
        String explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("equal join conjunct: 8: id = 11: id"));
        Assert.assertTrue(explainString.contains("  |----2:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: join1"));
        Assert.assertTrue(explainString.contains("  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON"));
    }

    @Test
    @Ignore("disable replicate join temporarily")
    public void testReplicatedJoin() throws Exception {
        connectContext.getSessionVariable().setEnableReplicationJoin(true);
        String sql = "select * from join1 join join2 on join1.id = join2.id;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "join op: INNER JOIN (REPLICATED)");
        assertNotContains(plan, "EXCHANGE");

        sql = "select * from join2 right join join1 on join1.id = join2.id;";
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "join op: INNER JOIN (REPLICATED)");

        sql = "select * from join1 as a join (select sum(id),id from join2 group by id) as b on a.id = b.id;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "join op: INNER JOIN (REPLICATED)");
        assertNotContains(plan, "EXCHANGE");

        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        sql = "select * from join1 as a join (select sum(id),dt from join2 group by dt) as b on a.id = b.dt;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "join op: INNER JOIN (BROADCAST)");
        assertContains(plan, "EXCHANGE");
        connectContext.getSessionVariable().setNewPlanerAggStage(0);

        sql = "select a.* from join1 as a join join1 as b ;";
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "EXCHANGE");

        sql = "select a.* from join1 as a join (select sum(id) from join1 group by dt) as b ;";
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "EXCHANGE");

        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        sql = "select a.* from join1 as a join (select sum(id) from join1 group by dt) as b ;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "EXCHANGE");
        connectContext.getSessionVariable().setNewPlanerAggStage(0);

        connectContext.getSessionVariable().setEnableReplicationJoin(false);
    }

    @Test
    public void testReplicationJoinWithPartitionTable() throws Exception {
        connectContext.getSessionVariable().setEnableReplicationJoin(true);
        FeConstants.runningUnitTest = true;
        String sql = "select * from join1 join pushdown_test on join1.id = pushdown_test.k1;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "INNER JOIN (BROADCAST)");
        FeConstants.runningUnitTest = false;
        connectContext.getSessionVariable().setEnableReplicationJoin(false);
    }

    // todo(ywb) disable replicate join temporarily
    public void testReplicationJoinWithEmptyNode() throws Exception {
        // check replicate join without exception
        connectContext.getSessionVariable().setEnableReplicationJoin(true);
        FeConstants.runningUnitTest = true;
        String sql = "with cross_join as (\n" +
                "  select * from \n" +
                "  (SELECT \n" +
                "      t0.v1, \n" +
                "      t0.v2, \n" +
                "      t0.v3\n" +
                "    FROM \n" +
                "      t0 \n" +
                "    WHERE \n" +
                "      false)\n" +
                "  subt0 LEFT SEMI \n" +
                "  JOIN \n" +
                "    (SELECT \n" +
                "      t2.v7, \n" +
                "      t2.v8, \n" +
                "      t2.v9\n" +
                "    FROM \n" +
                "      t2 \n" +
                "    WHERE \n" +
                "      false)\n" +
                "  subt2 ON subt0.v3 = subt2.v8, \n" +
                "  t1 \n" +
                ")\n" +
                "SELECT \n" +
                "  DISTINCT cross_join.v1 \n" +
                "FROM \n" +
                "  t0 LEFT JOIN\n" +
                "  cross_join\n" +
                "  ON cross_join.v4 = t0.v2;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "9:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (REPLICATED)");
        FeConstants.runningUnitTest = false;
        connectContext.getSessionVariable().setEnableReplicationJoin(false);
    }

    @Test
    public void testColocateCoverReplicate() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select * from join1 join join1 as xx on join1.id = xx.id;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  |  join op: INNER JOIN (COLOCATE)\n");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testConstPredicateInRightJoin() throws Exception {
        String sql = "select * from test.join1 right join test.join2 on join1.id = join2.id where round(2.0, 0) > 3.0";
        String explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  2:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: CAST(round(2.0, 0) AS DOUBLE) > 3.0"));

        sql = "select * from test.join1 right semi join test.join2 on join1.id = join2.id where round(2.0, 0) > 3.0";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: CAST(round(2.0, 0) AS DOUBLE) > 3.0"));

        sql = "select * from test.join1 right anti join test.join2 on join1.id = join2.id where round(2.0, 0) > 3.0";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: CAST(round(2.0, 0) AS DOUBLE) > 3.0"));

        sql = "select * from test.join1 left join test.join2 on join1.id = join2.id where round(2.0, 0) > 3.0";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: CAST(round(2.0, 0) AS DOUBLE) > 3.0"));

        sql = "select * from test.join1 left semi join test.join2 on join1.id = join2.id where round(2.0, 0) > 3.0";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: CAST(round(2.0, 0) AS DOUBLE) > 3.0"));

        sql = "select * from test.join1 left anti join test.join2 on join1.id = join2.id where round(2.0, 0) > 3.0";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: CAST(round(2.0, 0) AS DOUBLE) > 3.0"));

        sql = "select * from test.join1 inner join test.join2 on join1.id = join2.id where round(2.0, 0) > 3.0";
        explainString = getFragmentPlan(sql);
        assertContains(explainString, "  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: CAST(round(2.0, 0) AS DOUBLE) > 3.0");

        sql = "select * from test.join1 where round(2.0, 0) > 3.0";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: CAST(round(2.0, 0) AS DOUBLE) > 3.0"));
    }

    @Test
    public void testSelfColocateJoin() throws Exception {
        // single partition
        FeConstants.runningUnitTest = true;
        String queryStr = "select * from test.jointest t1, test.jointest t2 where t1.k1 = t2.k1";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        // multi partition
        queryStr = "select * from test.dynamic_partition t1, test.dynamic_partition t2 where t1.k1 = t2.k1";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testSemiJoinNameResolve() {
        String sql = "select join1.dt from  test.join1 right semi join test.join2 on join1.id = join2.id";
        starRocksAssert.query(sql).analysisError("Column '`join1`.`dt`' cannot be resolved");

        sql = "select a.dt from test.join1 a left ANTI join test.join2 b on a.id = b.id " +
                "right ANTI join test.join2 d on a.id = d.id";
        starRocksAssert.query(sql).analysisError("Column '`a`.`dt`' cannot be resolved");
    }

    @Test
    public void testLeftOuterJoinOnOrPredicate() throws Exception {
        connectContext.setDatabase("test");

        String sql = "select * from join1 left join join2 on join1.id = join2.id\n" +
                "and (join2.id > 1 or join2.id < 10);";
        String explainString = getFragmentPlan(sql);

        assertContains(explainString, "join op: LEFT OUTER JOIN (BROADCAST)");
        assertContains(explainString, "PREDICATES: (5: id > 1) OR (5: id < 10)");
        assertContains(explainString, "equal join conjunct: 2: id = 5: id");
    }

    @Test
    public void testColocateJoin2() throws Exception {
        FeConstants.runningUnitTest = true;
        String queryStr =
                "select * from test.colocate1 t1, test.colocate2 t2 where t1.k1 = t2.k1 and t1.k2 = t2.k2 and t1.k3 = t2.k3";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr = "select * from test.colocate1 t1 join " +
                "(select k1, k2 from test.colocate2 group by k1, k2) t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr = "select * from test.colocate1 t1 " +
                "join (select k1, k2 from test.colocate2 group by k1, k2, k3) t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr = "select * from (select k1, k2 from test.colocate1 group by k1, k2) t1 " +
                "join (select k1, k2 from test.colocate2 group by k1, k2) t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from test.colocate1 t1 join [shuffle] test.colocate2 t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));

        // t1.k1 = t2.k2 not same order with distribute column
        queryStr =
                "select * from test.colocate1 t1, test.colocate2 t2 where t1.k1 = t2.k2 and t1.k2 = t2.k1 and t1.k3 = t2.k3";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));

        queryStr = "select * from test.colocate1 t1, test.colocate2 t2 where t1.k2 = t2.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));

        queryStr = "select count(*) from test.colocate1 t1 group by t1.k1, t1.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("1:AGGREGATE (update finalize)"));
        Assert.assertFalse(explainString.contains("3:AGGREGATE (merge finalize)"));

        queryStr = "select count(*) from test.colocate1 t1 group by t1.k1";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("1:AGGREGATE (update finalize)"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testOuterJoinEliminate() throws Exception {
        // test left join eliminate
        String sql = "select * from join1 left join join2 on join1.id = join2.id\n" +
                "where join2.id > 1;";
        starRocksAssert.query(sql).explainContains("  3:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (BROADCAST)\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id",
                "  0:OlapScanNode\n" +
                        "     TABLE: join1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 2: id > 1",
                "  1:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: id > 1");

        // test left join eliminate with compound predicate
        sql = "select * from join1 left join join2 on join1.id = join2.id\n" +
                "where join2.id > 1 or join2.id < 10 ;";
        starRocksAssert.query(sql).explainContains("  3:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (BROADCAST)\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id",
                "  1:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: (5: id > 1) OR (5: id < 10)");

        // test left join eliminate with compound predicate
        sql = "select * from join1 left join join2 on join1.id = join2.id\n" +
                "where join2.id > 1 or join2.id is null;";
        //        getFragmentPlan(sql);
        starRocksAssert.query(sql).explainContains("  3:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: id = 5: id\n" +
                "  |  other predicates: (5: id > 1) OR (5: id IS NULL)");

        // test left join eliminate with inline view
        sql = "select * from join1 left join (select * from join2) b on join1.id = b.id\n" +
                "where b.id > 1;";
        starRocksAssert.query(sql).explainContains("  3:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (BROADCAST)\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id",
                "  0:OlapScanNode\n" +
                        "     TABLE: join1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 2: id > 1",
                "  1:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: id > 1");

        // test left join eliminate with inline view
        sql = "select * from (select * from join1) a left join (select * from join2) b on a.id = b.id\n" +
                "where b.id > 1;";
        starRocksAssert.query(sql).explainContains("  3:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (BROADCAST)\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id",
                "  0:OlapScanNode\n" +
                        "     TABLE: join1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 2: id > 1",
                "  1:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: id > 1");

        // test not left join eliminate
        sql = "select * from join1 left join join2 on join1.id = join2.id\n" +
                "where join2.id is null;";
        starRocksAssert.query(sql).explainContains("  3:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: id = 5: id\n" +
                "  |  other predicates: 5: id IS NULL");

        // test having group column
        sql = "select count(*) from join1 left join join2 on join1.id = join2.id\n" +
                "group by join2.id having join2.id > 1;";
        starRocksAssert.query(sql).explainContains("  4:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (PARTITIONED)\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id",
                "  0:OlapScanNode\n" +
                        "     TABLE: join1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 2: id > 1",
                "  2:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: id > 1");

        // test having aggregate column
        sql = "select count(*) as count from join1 left join join2 on join1.id = join2.id\n" +
                "having count > 1;";
        starRocksAssert.query(sql).explainContains("7:AGGREGATE (merge finalize)\n" +
                        "  |  output: count(7: count)\n" +
                        "  |  group by: \n" +
                        "  |  having: 7: count > 1",
                "  3:HASH JOIN\n" +
                        "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id");

        // test right join eliminate
        sql = "select * from join1 right join join2 on join1.id = join2.id\n" +
                "where join1.id > 1;";
        starRocksAssert.query(sql).explainContains("  3:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (BROADCAST)\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id",
                "  0:OlapScanNode\n" +
                        "     TABLE: join1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 2: id > 1",
                "  1:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: id > 1");

        // test full outer join convert to left join
        sql = "select * from join1 full outer join join2 on join1.id = join2.id\n" +
                "where join1.id > 1;";
        starRocksAssert.query(sql).explainContains("  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id",
                "  0:OlapScanNode\n" +
                        "     TABLE: join1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 2: id > 1",
                "  1:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: id > 1");

        sql = "select * from join1 full outer join join2 on join1.id = join2.id and join1.dt != 2\n" +
                "where join1.id > 1;";
        starRocksAssert.query(sql).explainContains("  3:HASH JOIN\n" +
                        "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id\n" +
                        "  |  other join predicates: 1: dt != 2",
                "  0:OlapScanNode\n" +
                        "     TABLE: join1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 2: id > 1",
                "  1:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: id > 1");

        // test full outer join convert to right join
        sql = "select * from join1 full outer join join2 on join1.id = join2.id\n" +
                "where join2.id > 1;";
        starRocksAssert.query(sql).explainContains("  4:HASH JOIN\n" +
                        "  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id",
                "  2:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: id > 1",
                "  0:OlapScanNode\n" +
                        "     TABLE: join1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 2: id > 1");

        // test full outer join convert to inner join
        sql = "select * from join1 full outer join join2 on join1.id = join2.id\n" +
                "where join2.id > 1 and join1.id > 10;";
        starRocksAssert.query(sql).explainContains("  3:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (BROADCAST)\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id",
                "  0:OlapScanNode\n" +
                        "     TABLE: join1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 2: id > 10",
                "  1:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: id > 1");

        // test multi left join eliminate
        sql = "select * from join1 left join join2 as b on join1.id = b.id\n" +
                "left join join2 as c on join1.id = c.id \n" +
                "where b.id > 1;";

        starRocksAssert.query(sql).explainContains("7:HASH JOIN\n" +
                        "  |  join op: LEFT OUTER JOIN (BUCKET_SHUFFLE(S))\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 8: id",
                "4:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (PARTITIONED)\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id\n",
                "2:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: id > 1",
                "0:OlapScanNode\n" +
                        "     TABLE: join1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 2: id > 1");

        sql = "select * from join1 left join join2 as b on join1.id = b.id\n" +
                "left join join2 as c on join1.id = c.id \n" +
                "where b.dt > 1 and c.dt > 1;";
        assertContains(starRocksAssert.query(sql).explainQuery(), "  7:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 8: id",
                "  4:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (PARTITIONED)\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id",
                "  5:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 7: dt > 1",
                "  2:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 4: dt > 1");
    }

    @Test
    public void testJoinReorderWithReanalyze() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        Table table = globalStateMgr.getDb("test").getTable("join2");
        OlapTable olapTable1 = (OlapTable) table;
        new Expectations(olapTable1) {
            {
                olapTable1.getRowCount();
                result = 2L;
                minTimes = 0;
            }
        };
        String sql = "select * from join1 join join2 on join1.id = join2.id and 1 < join1.id ";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: id > 1");
        assertContains(plan, "  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: id > 1");
    }

    @Test
    public void testAntiJoinOnFalseConstantPredicate() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select join2.id from join1 RIGHT ANTI JOIN join2 on join1.id = join2.id" +
                " and 1 > 2 group by join2.id" +
                " union select join2.id from join1 RIGHT ANTI JOIN join2 on join1.id = join2.id " +
                " and 1 > 2 WHERE (NOT (true)) group by join2.id ";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "4:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 5: id = 2: id");
        assertContains(plan, "  2:EMPTYSET\n");
        assertContains(plan, "  8:EMPTYSET\n");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testFullOuterJoin2() throws Exception {
        String sql =
                "SELECT 1 FROM join1 RIGHT ANTI JOIN join2 on join1.id = join2.id and join2.dt = 1 FULL OUTER JOIN "
                        + "pushdown_test on join2.dt = pushdown_test.k3 WHERE join2.value != join2.value";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "8:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: dt = 9: k3");
        assertContains(plan, "4:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 5: id = 2: id\n" +
                "  |  other join predicates: 4: dt = 1");
    }

    @Test
    public void testFullOuterJoin3() throws Exception {
        String sql =
                "SELECT 1 FROM join1 RIGHT ANTI JOIN join2 on join1.id = join2.id FULL OUTER JOIN "
                        + "pushdown_test on join2.dt = pushdown_test.k3 WHERE join2.value != join2.value";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "8:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: dt = 9: k3");
        assertContains(plan, "4:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 5: id = 2: id");
    }

    @Test
    public void testJoinConst() throws Exception {
        String sql = "with user_info as (select 2 as user_id, 'mike' as user_name), " +
                "address as (select 1 as user_id, 'newzland' as address_name) \n" +
                "select * from address a right join user_info b on b.user_id=a.user_id;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  6:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 8: expr = 11: expr");
        assertContains(plan, "  4:Project\n" +
                "  |  <slot 11> : 2\n" +
                "  |  <slot 12> : 'mike'\n" +
                "  |  \n" +
                "  3:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
        assertContains(plan, "  1:Project\n" +
                "  |  <slot 8> : 1\n" +
                "  |  <slot 9> : 'newzland'\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
    }

    @Test
    public void testJoinPredicateTransitivity() throws Exception {
        // test left join : left table where binary predicate
        String sql = "select join1.id\n" +
                "from join1\n" +
                "left join join2 on join1.id = join2.id\n" +
                "where join1.id > 1;";
        String explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("PREDICATES: 2: id > 1"));
        Assert.assertTrue(explainString.contains("PREDICATES: 5: id > 1"));

        // test left join: left table where in predicate
        sql = "select join1.id\n" +
                "from join1\n" +
                "left join join2 on join1.id = join2.id\n" +
                "where join1.id in (2);";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("PREDICATES: 2: id = 2"));
        Assert.assertTrue(explainString.contains("PREDICATES: 5: id = 2"));

        // test left join: left table where between predicate
        sql = "select join1.id\n" +
                "from join1\n" +
                "left join join2 on join1.id = join2.id\n" +
                "where join1.id BETWEEN 1 AND 2;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("PREDICATES: 2: id >= 1, 2: id <= 2"));
        Assert.assertTrue(explainString.contains("PREDICATES: 5: id >= 1, 5: id <= 2"));

        // test left join: left table join predicate, left table couldn't push down
        sql = "select *\n from join1\n" +
                "left join join2 on join1.id = join2.id\n" +
                "and join1.id > 1;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  3:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: id = 5: id\n" +
                "  |  other join predicates: 2: id > 1"));
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1"));
        Assert.assertTrue(explainString.contains("  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: id > 1"));

        // test left join: right table where predicate.
        // If we eliminate outer join, we could push predicate down to join1 and join2.
        // Currently, we push predicate to join1 and keep join predicate for join2
        sql = "select *\n from join1\n" +
                "left join join2 on join1.id = join2.id\n" +
                "where join2.id > 1;";
        explainString = getFragmentPlan(sql);
        assertContains(explainString, "  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: id > 1");

        // test left join: right table join predicate, only push down right table
        sql = "select *\n from join1\n" +
                "left join join2 on join1.id = join2.id\n" +
                "and join2.id > 1;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: id > 1"));
        Assert.assertTrue(explainString.contains("0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: join1"));

        // test inner join: left table where predicate, both push down left table and right table
        sql = "select *\n from join1\n" +
                "join join2 on join1.id = join2.id\n" +
                "where join1.id > 1;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: id > 1"));
        Assert.assertTrue(explainString.contains("  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: id > 1"));

        // test inner join: left table join predicate, both push down left table and right table
        sql = "select *\n from join1\n" +
                "join join2 on join1.id = join2.id\n" +
                "and join1.id > 1;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: id > 1"));
        Assert.assertTrue(explainString.contains("  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: id > 1"));

        // test inner join: right table where predicate, both push down left table and right table
        sql = "select *\n from join1\n" +
                "join join2 on join1.id = join2.id\n" +
                "where join2.id > 1;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: id > 1"));
        Assert.assertTrue(explainString.contains("  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: id > 1"));

        // test inner join: right table join predicate, both push down left table and right table
        sql = "select *\n from join1\n" +
                "join join2 on join1.id = join2.id\n" +
                "and 1 < join2.id;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: id > 1"));
        Assert.assertTrue(explainString.contains("  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: id > 1"));

        sql = "select *\n from join1\n" +
                "join join2 on join1.id = join2.value\n" +
                "and join2.value in ('abc');";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("equal join conjunct: 7: cast = 6: value"));
        Assert.assertTrue(explainString.contains("<slot 7> : CAST(2: id AS VARCHAR(1048576))"));
        Assert.assertTrue(explainString.contains("  2:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 6: value = 'abc'"));

        // test anti join, right table join predicate, only push to right table
        sql = "select *\n from join1\n" +
                "left anti join join2 on join1.id = join2.id\n" +
                "and join2.id > 1;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: id > 1"));
        Assert.assertTrue(explainString.contains("  3:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: id = 5: id"));

        // test semi join, right table join predicate, only push to right table
        sql = "select *\n from join1\n" +
                "left semi join join2 on join1.id = join2.id\n" +
                "and join2.id > 1;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  3:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: id = 5: id"));
        Assert.assertTrue(explainString.contains("  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: id > 1"));

        // test anti join, left table join predicate, left table couldn't push down
        sql = "select *\n from join1\n" +
                "left anti join join2 on join1.id = join2.id\n" +
                "and join1.id > 1;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  3:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: id = 5: id\n" +
                "  |  other join predicates: 2: id > 1"));

        // test semi join, left table join predicate, only push to left table
        sql = "select *\n from join1\n" +
                "left semi join join2 on join1.id = join2.id\n" +
                "and join1.id > 1;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  3:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: id = 5: id"));
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: id > 1"));

        // test anti join, left table where predicate, only push to left table
        sql = "select join1.id\n" +
                "from join1\n" +
                "left anti join join2 on join1.id = join2.id\n" +
                "where join1.id > 1;";
        explainString = getFragmentPlan(sql);
        assertContains(explainString, "3:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: id = 5: id");
        assertContains(explainString, "0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: id > 1");

        // test semi join, left table where predicate, only push to left table
        sql = "select join1.id\n" +
                "from join1\n" +
                "left semi join join2 on join1.id = join2.id\n" +
                "where join1.id > 1;";
        explainString = getFragmentPlan(sql);
        assertContains(explainString, "  3:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: id = 5: id");
        assertContains(explainString, "  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: id > 1");
    }

    @Test
    public void testJoinPredicateTransitivityWithSubqueryInWhereClause() throws Exception {
        String sql = "SELECT *\n" +
                "FROM test.pushdown_test\n" +
                "WHERE 0 < (\n" +
                "    SELECT MAX(k9)\n" +
                "    FROM test.pushdown_test);";
        String plan = starRocksAssert.query(sql).explainQuery();
        assertContains(plan, "    UNPARTITIONED\n" +
                "\n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  output: max(22: k9)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:OlapScanNode");
    }

    @Test
    public void testPushDownEquivalenceDerivePredicate() throws Exception {
        // check is null predicate on t1.v5 which equivalences derive from t1.v4 can not push down to scan node
        String sql = "SELECT \n" +
                "  subt0.v2, \n" +
                "  t1.v6\n" +
                "FROM \n" +
                "  (\n" +
                "    SELECT \n" +
                "      t0.v1, \n" +
                "      t0.v2, \n" +
                "      t0.v3\n" +
                "    FROM \n" +
                "      t0\n" +
                "  ) subt0 \n" +
                "  LEFT JOIN t1 ON subt0.v3 = t1.v4 \n" +
                "  AND subt0.v3 = t1.v4 \n" +
                "  AND subt0.v3 = t1.v5 \n" +
                "  AND subt0.v3 >= t1.v5 \n" +
                "WHERE \n" +
                "  (\n" +
                "    (\n" +
                "      (t1.v4) < (\n" +
                "        (\n" +
                "          (-650850438)-(\n" +
                "            (\n" +
                "              (2000266938)%(-1243652117)\n" +
                "            )\n" +
                "          )\n" +
                "        )\n" +
                "      )\n" +
                "    ) IS NULL\n" +
                "  ) \n" +
                "GROUP BY \n" +
                " subt0.v2, \n" +
                "  t1.v6;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, " 0:OlapScanNode\n" +
                "     TABLE: t1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1");
    }

    @Test
    public void testPredicateOnThreeTables() throws Exception {
        String sql = "SELECT DISTINCT t1.v4 \n" +
                "FROM t1, t3 subt3 FULL JOIN t0 ON subt3.v12 != t0.v1 AND subt3.v12 = t0.v1 \n" +
                "WHERE ((t0.v2) BETWEEN (CAST(subt3.v11 AS STRING)) AND (t0.v2)) = (t1.v4);";
        String plan = getFragmentPlan(sql);
        // check no exception
        assertContains(plan, "  11:AGGREGATE (update finalize)\n" +
                "  |  group by: 1: v4\n" +
                "  |  \n" +
                "  10:Project\n" +
                "  |  <slot 1> : 1: v4\n" +
                "  |  \n" +
                "  9:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v4 = 10: cast");
    }

    @Test
    public void testExplicitlyBroadcastJoin() throws Exception {
        String sql = "select * from db1.tbl1 join [BROADCAST] db1.tbl2 on tbl1.k1 = tbl2.k3";
        String plan = getFragmentPlan(sql);
        Assert.assertEquals(1, StringUtils.countMatches(plan, "INNER JOIN (BROADCAST)"));

        sql = "select * from db1.tbl1 join [SHUFFLE] db1.tbl2 on tbl1.k1 = tbl2.k3";
        plan = getFragmentPlan(sql);
        Assert.assertEquals(1, StringUtils.countMatches(plan, "INNER JOIN (PARTITIONED)"));
    }

    @Test
    public void testEquivalenceTest() throws Exception {
        String sql = "select * from t0 as x1 join t0 as x2 on x1.v2 = x2.v2 where x2.v2 = 'zxcv';";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: CAST(2: v2 AS VARCHAR(1048576)) = 'zxcv'");
        assertContains(plan, "  1:OlapScanNode\n"
                + "     TABLE: t0\n"
                + "     PREAGGREGATION: ON\n"
                + "     PREDICATES: CAST(5: v2 AS VARCHAR(1048576)) = 'zxcv'\n");
    }

    @Test
    public void testEquivalenceLoopDependency() throws Exception {
        String sql = "select * from t0 join t1 on t0.v1 = t1.v4 and cast(t0.v1 as STRING) = t0.v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "|  equal join conjunct: 1: v1 = 4: v4");
        assertContains(plan, "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: CAST(1: v1 AS VARCHAR(65533)) = CAST(1: v1 AS VARCHAR(1048576))\n" +
                "     partitions=0/1\n");
    }

    @Test
    public void testJoinCastFloat() throws Exception {
        String sql = "select * from t1, t3 right semi join test_all_type as a on t3.v10 = a.t1a and 1 > 2;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "equal join conjunct: 7: t1a = 17: cast");
    }

    @Test
    public void testWherePredicatesToOnPredicate() throws Exception {
        String sql =
                "SELECT t0.v1 from t0 join test_all_type on t0.v2 = test_all_type.t1d where t0.v1 = test_all_type.t1d";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "join op: INNER JOIN");
        assertContains(plan, "  |  equal join conjunct: 2: v2 = 7: t1d\n"
                + "  |  equal join conjunct: 1: v1 = 7: t1d");
    }

    @Test
    public void testDeriveOutputColumns() throws Exception {
        String sql = "select rand() as c0, round(cast(rand() as DOUBLE)) as c1 \n" +
                "from (\n" +
                "    select subq_0.v1 as c0 \n" +
                "    from ( select v1,v2,v3\n" +
                "        from t0 as ref_0 \n" +
                "        where ref_0.v1 = ref_0.v2 \n" +
                "        limit 72\n" +
                "      ) as subq_0 \n" +
                "      right join t1 as ref_1 on (subq_0.v3 = ref_1.v5) \n" +
                "    where subq_0.v2 <> subq_0.v3 \n" +
                "    limit 126 ) as subq_1 \n" +
                "where 66 <= unix_timestamp() \n" +
                "limit 155;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  7:Project\n" +
                "  |  <slot 7> : rand()\n" +
                "  |  <slot 8> : round(rand())\n" +
                "  |  limit: 126");
    }

    @Test
    public void testEmptyTableDisableBucketJoin() throws Exception {
        String sql = "select colocate1.k1 from colocate1 " +
                "join[bucket] test_agg on colocate1.k1 = test_agg.k1 and colocate1.k2 = test_agg.k2";
        String plan = getFragmentPlan(sql);
        assertContains(plan, " 4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)");

        sql = "select colocate1.k1 from colocate1 " +
                "join[bucket] colocate2 on colocate1.k1 = colocate2.k2 and colocate1.k2 = colocate2.k3";
        plan = getFragmentPlan(sql);
        assertContains(plan, "4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)");
    }

    @Test
    public void testColocateJoinWithDiffPredicateOrders() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select a.v1 from t0 a join t0 b on a.v1 = b.v2 and a.v2 = b.v1";
        String plan = getFragmentPlan(sql);
        // check cannot use colcoate join
        assertContains(plan, "3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)");

        sql = "select a.t1a from test_all_type a join test_all_type b on a.t1a = b.t1b and a.t1b = b.t1a";
        plan = getFragmentPlan(sql);
        assertContains(plan, " 5:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: t1a = 21: cast\n" +
                "  |  equal join conjunct: 22: cast = 11: t1a");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testColocateJoinWithProject() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select a.v1 from t0 as a join t0 b on a.v1 = b.v1 and a.v1 = b.v1 + 1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (COLOCATE)");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testShuffleJoinEqEquivalentPropertyInfo() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            String sql =
                    "select * from ( select * from t0 join[shuffle] t1 on t0.v2 = t1.v5 ) s1 join[shuffle] t2 on s1.v5 = t2.v8";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 5: v5 = 8: v8\n" +
                    "  |  \n" +
                    "  |----6:EXCHANGE\n" +
                    "  |    \n" +
                    "  4:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (PARTITIONED)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 5: v5");
        }
        {
            String sql =
                    "select * from ( select * from t0 join[shuffle] t1 on t0.v2 = t1.v5 and t0.v3 = t1.v6 ) s1 " +
                            "join[shuffle] t2 on s1.v2 = t2.v8 and s1.v6 = t2.v9";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 8: v8\n" +
                    "  |  equal join conjunct: 6: v6 = 9: v9\n" +
                    "  |  \n" +
                    "  |----6:EXCHANGE\n" +
                    "  |    \n" +
                    "  4:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (PARTITIONED)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 5: v5\n" +
                    "  |  equal join conjunct: 3: v3 = 6: v6");
        }
        {
            String sql =
                    "select * from ( select * from t0 join[shuffle] t1 on t0.v2 = t1.v5 and t0.v3 = t1.v6 ) s1 " +
                            "join[shuffle] t2 on s1.v5 = t2.v8 and s1.v3 = t2.v9";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 5: v5 = 8: v8\n" +
                    "  |  equal join conjunct: 3: v3 = 9: v9\n" +
                    "  |  \n" +
                    "  |----6:EXCHANGE\n" +
                    "  |    \n" +
                    "  4:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (PARTITIONED)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 5: v5\n" +
                    "  |  equal join conjunct: 3: v3 = 6: v6");
        }
        {
            String sql =
                    "select * from ( select * from t0 join[shuffle] t1 on t0.v2 = t1.v5 and t0.v3 = t1.v6 ) s1 " +
                            "join[shuffle] t2 on s1.v5 = t2.v8 and s1.v6 = t2.v9";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 5: v5 = 8: v8\n" +
                    "  |  equal join conjunct: 6: v6 = 9: v9\n" +
                    "  |  \n" +
                    "  |----6:EXCHANGE\n" +
                    "  |    \n" +
                    "  4:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (PARTITIONED)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 5: v5\n" +
                    "  |  equal join conjunct: 3: v3 = 6: v6");
        }
        {
            // mismatch shuffle orders
            String sql =
                    "select * from ( select * from t0 join[shuffle] t1 on t0.v2 = t1.v5 and t0.v3 = t1.v6 ) s1 " +
                            "join[shuffle] t2 on s1.v6 = t2.v9 and s1.v5 = t2.v8";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  7:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 6: v6 = 9: v9\n" +
                    "  |  equal join conjunct: 5: v5 = 8: v8\n" +
                    "  |  \n" +
                    "  |----6:EXCHANGE\n" +
                    "  |    \n" +
                    "  4:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (PARTITIONED)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 5: v5\n" +
                    "  |  equal join conjunct: 3: v3 = 6: v6\n" +
                    "  |  \n" +
                    "  |----3:EXCHANGE\n" +
                    "  |    \n" +
                    "  1:EXCHANGE\n");
            assertContains(plan, "  STREAM DATA SINK\n" +
                    "    EXCHANGE ID: 06\n" +
                    "    HASH_PARTITIONED: 9: v9, 8: v8");
            assertContains(plan, "  STREAM DATA SINK\n" +
                    "    EXCHANGE ID: 03\n" +
                    "    HASH_PARTITIONED: 6: v6, 5: v5");
            assertContains(plan, "  STREAM DATA SINK\n" +
                    "    EXCHANGE ID: 01\n" +
                    "    HASH_PARTITIONED: 3: v3, 2: v2");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testBucketSingleJoinEqEquivalentPropertyInfo() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            String sql =
                    "select * from ( select * from t0 join[bucket] t1 on t0.v1 = t1.v4 ) s1 join[bucket] t2 on s1.v4 = t2.v7";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 7: v7");
        }
        {
            // Change on predicate order
            String sql =
                    "select * from ( select * from t0 join[bucket] t1 on t1.v4 = t0.v1 ) s1 join[bucket] t2 on s1.v4 = t2.v7";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 7: v7");
        }
        {
            // Only output right table's attribute
            String sql =
                    "select * from ( select t1.v4 from t0 join[bucket] t1 on t0.v1 = t1.v4 ) s1 join[bucket] t2 on s1.v4 = t2.v7";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 7: v7");
        }
        {
            // Bushy join
            String sql =
                    "select * from ( select * from t0 join[bucket] t1 on t0.v1 = t1.v4 ) s1, " +
                            "( select * from t2 join[bucket] t3 on t2.v7 = t3.v10 ) s2 " +
                            "where s1.v4 = s2.v10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  9:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 10: v10");
        }
        {
            // Multi level joins
            String sql =
                    "select * from t0 join[bucket] t1 on t0.v1 = t1.v4 " +
                            "join[bucket] t2 on t1.v4 = t2.v7 " +
                            "join[bucket] t3 on t2.v7 = t3.v10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 7: v7 = 10: v10");
        }
        {
            // Multi level joins
            String sql =
                    "select * from t0 join[bucket] t1 on t0.v1 = t1.v4 " +
                            "join[bucket] t2 on t1.v4 = t2.v7 " +
                            "join[bucket] t3 on t2.v7 = t3.v10 " +
                            "join[bucket] t4 on t3.v10 = t4.v13";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 10: v10 = 13: v13");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testBucketMultiJoinEqEquivalentPropertyInfo() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            String sql =
                    "select * from ( select * from t0 join[bucket] t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5) s1 " +
                            "join[bucket] t2 on s1.v1 = t2.v7 and s1.v5 = t2.v8";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 7: v7\n" +
                    "  |  equal join conjunct: 5: v5 = 8: v8");
        }
        {
            String sql =
                    "select * from ( select * from t0 join[bucket] t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5) s1 " +
                            "join[bucket] t2 on s1.v4 = t2.v7 and s1.v2 = t2.v8";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 7: v7\n" +
                    "  |  equal join conjunct: 2: v2 = 8: v8");
        }
        {
            String sql =
                    "select * from ( select * from t0 join[bucket] t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5) s1 " +
                            "join[bucket] t2 on s1.v4 = t2.v7 and s1.v5 = t2.v8";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 7: v7\n" +
                    "  |  equal join conjunct: 5: v5 = 8: v8");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testColocateSingleJoinEqEquivalentPropertyInfo() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            String sql = "select * from ( select * from colocate_t0 " +
                    "join[colocate] colocate_t1 on colocate_t0.v1 = colocate_t1.v4 ) s1 " +
                    "join[bucket] colocate_t2 on s1.v4 = colocate_t2.v7";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 7: v7");
        }
        {
            String sql = "select * from ( select * from colocate_t0 " +
                    "join[colocate] colocate_t1 on colocate_t0.v1 = colocate_t1.v4 ) s1, " +
                    "( select * from colocate_t2 join[colocate] colocate_t3 " +
                    "on colocate_t2.v7 = colocate_t3.v10 ) s2 " +
                    "where s1.v4 = s2.v10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  7:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 10: v10");
        }
        {
            // anti colocate join
            String sql =
                    "select * from (select * from colocate_t0 join[bucket] colocate_t2 on colocate_t0.v1 = colocate_t2.v7) s1 " +
                            "join [colocate] colocate_t3 on s1.v7 = colocate_t3.v10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v7 = 7: v10\n" +
                    "  |  \n" +
                    "  |----5:EXCHANGE\n" +
                    "  |    \n" +
                    "  3:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 4: v7");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testJoinOnPredicateCommutativityNotInnerJoin() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            String sql = "select * from ( select * from t0 left join[bucket] t1 on t0.v1 = t1.v4 ) s1 " +
                    "join[bucket] t2 on s1.v4 = t2.v7";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 7: v7");
        }
        {
            String sql = "select * from ( select * from t0 right join[bucket] t1 on t0.v1 = t1.v4 ) s1 " +
                    "join[bucket] t2 on s1.v4 = t2.v7";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (PARTITIONED)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 7: v7");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testBucketJoinNotEqPropertyInfo() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            String sql =
                    "select * from ( select * from t0 join[bucket] t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5) s1 " +
                            "join[bucket] t2 on s1.v5 = t2.v8";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (PARTITIONED)\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 5: v5 = 8: v8\n" +
                    "  |  \n" +
                    "  |----6:EXCHANGE\n" +
                    "  |    \n" +
                    "  4:EXCHANGE");
        }
        {
            String sql = "select * from ( select * from t5 join[bucket] t1 on t5.v16 = t1.v4 and t5.v17 = t1.v5) s1 " +
                    "join[bucket] t2 on s1.v4 = t2.v7";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  7:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (PARTITIONED)");
        }
        {
            String sql = "select * from t5 join[bucket] ( select * from t2 join[bucket] t1 on t2.v7 = t1.v4) s1 " +
                    "on t5.v16 = s1.v4 and t5.v17 = s1.v5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  6:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testValueNodeJoin() throws Exception {
        String sql = "select count(*) from (select test_all_type.t1c as left_int, " +
                "test_all_type1.t1c as right_int from (select * from test_all_type limit 0) " +
                "test_all_type cross join (select * from test_all_type limit 0) test_all_type1 " +
                "cross join (select * from test_all_type limit 0) test_all_type6) t;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "0:EMPTYSET");
        assertContains(plan, "1:EMPTYSET");
    }

    @Test
    public void testSemiJoinReorderProjections() throws Exception {
        String sql = "WITH with_t_0 as (\n" +
                "  SELECT t1_3.t1b, t1_3.t1d \n" +
                "  FROM test_all_type AS t1_3 \n" +
                "  WHERE ((SELECT t1_3.t1a FROM test_all_type AS t1_3)) < ((SELECT 11))\n" +
                ") \n" +
                "SELECT \n" +
                "  SUM(count) \n" +
                "FROM (SELECT \n" +
                "      CAST(false AS INT) as count \n" +
                "    FROM test_all_type AS t1_3 FULL \n" +
                "      JOIN (\n" +
                "        SELECT with_t_0.t1b \n" +
                "        FROM with_t_0 AS with_t_0 \n" +
                "        WHERE (with_t_0.t1d) IN ((SELECT t1_3.t1d FROM test_all_type AS t1_3))\n" +
                "      ) subwith_t_0 ON t1_3.id_decimal = subwith_t_0.t1b\n" +
                "  ) t;";
        String plan = getFragmentPlan(sql);
        // check no error
        assertContains(plan, "11:ASSERT NUMBER OF ROWS");
    }

    @Test
    public void testSemiOuterJoin() throws Exception {
        String sql = "select * from t0 full outer join t2 on t0.v1 = t2.v7 and t0.v1 > t2.v7 " +
                "where t0.v2 in (select t1.v4 from t1 where false)";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  7:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 7: v4\n" +
                "  |  \n" +
                "  |----6:EXCHANGE\n" +
                "  |    \n" +
                "  4:HASH JOIN\n" +
                "  |  join op: FULL OUTER JOIN (PARTITIONED)");
    }

    @Test
    public void testCrossJoinWithRF() throws Exception {
        // supported
        String sql = "select * from t0 join t2 on t0.v1 < t2.v7";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  3:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  other join predicates: [1: v1, BIGINT, true] < [4: v7, BIGINT, true]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (4: v7), remote = false\n" +
                "  |  cardinality: 1\n");

        sql = "select * from t0 join t2 on t0.v1 + t2.v7 < 2";
        plan = getVerboseExplain(sql);
        assertNotContains(plan, "build runtime filters");

        sql = "select * from t0 join t2 on t0.v1 < t2.v7 + t0.v1 ";
        plan = getVerboseExplain(sql);
        assertNotContains(plan, "build runtime filters");

        sql = "select * from t0 join t2 on t0.v1 < t2.v7 + t2.v8";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  3:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  other join predicates: [1: v1, BIGINT, true] < [4: v7, BIGINT, true] + [5: v8, BIGINT, true]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (4: v7 + 5: v8), remote = false\n" +
                "  |  cardinality: 1\n");

        // avoid push down CrossJoin RF across ExchangeNode
        /*
        String sql = "select * from t1 join [shuffle] t2 on v4 = v7 join t0 on v4 < v1 ";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  1:EXCHANGE\n" +
                "     cardinality: 1\n" +
                "     probe runtime filters:\n" +
                "     - filter_id = 1, probe_expr = (1: v4)");
         */
    }

    @Test
    public void testIsNotNullOnPredicate() throws Exception {
        String sql = "select * from " +
                "t0 left outer join t1 on t0.v3 = t1.v6 " +
                "              join t2 on t1.v5 = t2.v8";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 5: v5 IS NOT NULL");
        assertContains(plan, "PREDICATES: 8: v8 IS NOT NULL");
        assertContains(plan, "PREDICATES: 3: v3 IS NOT NULL");
    }

    @Test
    public void testIsNotNullWherePredicate() throws Exception {
        String sql = "select * from " +
                "t0 left outer join t1 on t0.v3 = t1.v6 " +
                "              ,t2 " +
                "where t1.v5 = t2.v8";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 5: v5 IS NOT NULL");
        assertContains(plan, "PREDICATES: 8: v8 IS NOT NULL");
        assertContains(plan, "PREDICATES: 3: v3 IS NOT NULL");
    }

    @Test
    public void testJoinWithAnalytic() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "SELECT t0.*, cnt\n" +
                "FROM t0\n" +
                "left outer JOIN (\n" +
                "    SELECT *,\n" +
                "    count(*) over(partition by v1) as cnt\n" +
                "    FROM t0 \n" +
                ") b\n" +
                "ON t0.v1 = b.v1;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  4:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (COLOCATE)\n" +
                "  |  colocate: true\n" +
                "  |  equal join conjunct: 1: v1 = 4: v1");
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testComplexExpr() throws Exception {
        String sql = "select count(v1) as c, if(v4 is not null, v4, '未知') as k2 from ( select A.v1, B.v4 from " +
                "( select v1 from t0 where v2 = 1 ) A left join ( select v4, if( v5 = 0, '未知', if( v5 = 1, '广东', " +
                "if( v5 = 2, '广西', if( v5 = 3, '北京', if( v5 = 4, '海南', if( v5 = 5, '福建', if( v5 = 6, '天津', " +
                "if( v5 = 7, '湖南', if( v5 = 8, '湖北', if( v5 = 9, '河南', if( v5 = 10, '河北', if( v5 = 11, '山东', " +
                "if( v5 = 12, '山西', if( v5 = 13, '黑龙江', if( v5 = 14, '辽宁', if( v5 = 15, '上海', if( v5 = 16, '甘肃', " +
                "if( v5 = 17, '青海', if( v5 = 18, '新疆', if( v5 = 19, '西藏', if( v5 = 20, '宁夏', if( v5 = 21, '四川', " +
                "if( v5 = 22, '云南', if( v5 = 23, '吉林', if( v5 = 24, '内蒙古', if( v5 = 25, '陕西', if( v5 = 26, '安徽', " +
                "if( v5 = 27, '贵州', if( v5 = 28, '江苏', if( v5 = 29, '重庆', if( v5 = 30, '浙江', if( v5 = 31, '江西', " +
                "if( v5 = 32, '国外', if( v5 = 33, '台湾', if(v5 = 34, '香港', if(v5 = 35, '澳门', 'Default')) ) ) ) ) ) ) " +
                ") ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) as k from t1 ) B on A.v1 = B.k ) C group by v4;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "if(5: v5 = 0, '未知'");
    }

    @Test
    public void testJoinKeyHasExpr() throws Exception {
        String sql = "select subq_1.c0 from " +
                "(select subq_0.c0 as c0, subq_0.c0 as c4, subq_0.c1 as c5, " +
                "max(cast(subq_0.c0 as INT)) over (partition by ref_1.n_nationkey, subq_0.c0) as c8, ref_1.n_name as c13 " +
                "from ( select ref_0.C_CUSTKEY as c0, ref_0.C_CUSTKEY as c1 from customer as ref_0 ) as subq_0 " +
                "inner join nation as ref_1 on (subq_0.c0 = ref_1.n_name) ) as subq_1 where subq_1.c13 = subq_1.c5;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 11: N_NAME = 16: cast\n" +
                "  |  equal join conjunct: 11: N_NAME = CAST(1: C_CUSTKEY AS VARCHAR(1048576))");
    }

    @Test
    public void testComplexProjectionJoin() throws Exception {
        connectContext.getSessionVariable().disableDPJoinReorder();
        connectContext.getSessionVariable().disableGreedyJoinReorder();
        connectContext.getSessionVariable().setMaxTransformReorderJoins(3);

        String sql = "select * from t4 join (   \n" +
                "    select abs(xx1) as xxx1 from t3 join (       \n" +
                "        select abs(x1) as xx1 from t2 join (          \n" +
                "            select abs(t0.v1) as x1 from t0 join t1 \n" +
                "            on v1 = t1.v4) y1        \n" +
                "        on t2.v7 = x1) y2    \n" +
                "    on t3.v10 = xx1) y3  \n" +
                "on t4.v13 = xxx1";

        String plan = getFragmentPlan(sql);
        connectContext.getSessionVariable().enableDPJoinReorder();
        connectContext.getSessionVariable().enableGreedyJoinReorder();
        connectContext.getSessionVariable().setMaxTransformReorderJoins(4);

        assertContains(plan, "  13:Project\n" +
                "  |  <slot 17> : abs(16: abs)\n" +
                "  |  \n" +
                "  12:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)");
    }

    @Test
    public void testSmallestColInJoin() throws Exception {
        String sql = "select 1 from (select v1, v2, 3, 4, 'a' from t0) t, t1, t2 where t.v1 = t1.v4 and t2.v7 = 1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 7: v4\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode");
        sql = "select 1 from (select v1, v2, 3, 4, 'a' from t0) t, t1, t2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "|----4:EXCHANGE\n" +
                "  |    \n" +
                "  1:Project\n" +
                "  |  <slot 21> : 1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0");
    }

    @Test
    public void testShuffleAgg() throws Exception {
        String sql = "select j0.* \n" +
                "    from t6 j0 join[shuffle] t6 j1 on j0.v2 = j1.v2 and j0.v3 = j1.v3\n" +
                "               join[shuffle] t6 j2 on j0.v2 = j2.v2 and j0.v3 = j2.v3 and j0.v4 = j2.v4\n" +
                "               join[shuffle] (select v4,v2,v3 from t6 group by v4,v2,v3) j4 " +
                "                             on j0.v2 =j4.v2 and j0.v3=j4.v3 and j0.v4 = j4.v4;\n" +
                "\n";

        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String plan = getFragmentPlan(sql);
        connectContext.getSessionVariable().setNewPlanerAggStage(0);

        assertContains(plan, "  15:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 14: v2\n" +
                "  |  equal join conjunct: 3: v3 = 15: v3\n" +
                "  |  equal join conjunct: 4: v4 = 16: v4\n" +
                "  |  \n" +
                "  |----14:EXCHANGE\n" +
                "  |    \n" +
                "  9:Project");
        assertContains(plan, "  PARTITION: HASH_PARTITIONED: 14: v2, 15: v3, 16: v4\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 14\n" +
                "    HASH_PARTITIONED: 14: v2, 15: v3\n" +
                "\n" +
                "  13:AGGREGATE (merge finalize)\n" +
                "  |  group by: 16: v4, 14: v2, 15: v3");
    }

    @Test // shouldn't have exception
    public void testRemoveAggregationFromAggTableRuluWithJoinAssociateRule() throws Exception {
        String sql = "select t0.v1 from t0 inner join (select a.k1, a.k2 from " +
                "(select k1, k2, k3 from test_agg group by k1, k2, k3) as a " +
                "where EXISTS (select a.k2 from " +
                "(select k1, k2, k3 from test_agg group by k1, k2, k3) as a )) " +
                "subv0 on t0.v1 = subv0.k1;";
        getFragmentPlan(sql);
    }

    @Test
    public void testTopNGroupMerge() throws Exception {
        String sql = "with tmp1 as (select * from t0 order by v1 asc), tmp2 as (select v4 from t1 group by v4)" +
                "select count(*) from t0, tmp2, tmp1, t1 " +
                "where 1 in (select v4 from t1);";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "21:NESTLOOP JOIN\n" +
                "  |  join op: LEFT SEMI JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: 19: v4 = 1");
    }

    @Test
    public void testTableFunctionReorder() throws Exception {

        String sql = "select * from (SELECT * FROM tarray, unnest(v3, v3)) t1 LEFT JOIN " +
                "(SELECT * FROM tarray, unnest(v3, v3)) t2 ON t1.v1=t2.v1 join t3 on t1.v1 = t3.v10";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "9:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 6: v1\n" +
                "  |  \n" +
                "  |----8:EXCHANGE\n" +
                "  |    \n" +
                "  5:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 11: v10\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  2:EXCHANGE");
    }
}
