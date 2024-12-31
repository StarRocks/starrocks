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

import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Replica;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import org.junit.Assert;
import org.junit.Test;

public class LimitTest extends PlanTestBase {

    @Test
    public void testLimit() throws Exception {
        String sql = "select v1 from t0 limit 1";
        String planFragment = getFragmentPlan(sql);
        assertContains(planFragment, ("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:1: v1\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  1:EXCHANGE\n" +
                "     limit: 1\n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=1.0\n" +
                "     limit: 1"));
    }

    @Test
    public void testLimitWithHaving() throws Exception {
        String sql = "SELECT v1, sum(v3) as v from t0 where v2 = 0 group by v1 having sum(v3) > 0 limit 10";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("having: 4: sum > 0"));
        Assert.assertTrue(planFragment.contains("limit: 10"));
    }

    @Test
    public void testWindowLimitPushdown() throws Exception {
        String sql = "select lag(v1, 1,1) OVER () from t0 limit 1";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING\n" +
                "  |  limit: 1"));
    }

    @Test
    public void testSelectStarWhereSubQueryLimit1() throws Exception {
        String sql = "SELECT * FROM t0 where v1 = (select v1 from t0 limit 1);";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("ASSERT NUMBER OF ROWS"));
    }

    @Test
    public void testCrossJoinWithLimit() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select * from t0 join t1 on t0.v2 = t1.v4 limit 2";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v4 = 2: v2\n" +
                "  |  limit: 2"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testLimit0WithAgg() throws Exception {
        String queryStr = "select count(*) from t0 limit 0";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:4: count"));
        Assert.assertTrue(explainString.contains("0:EMPTYSET"));

        queryStr = "select count(*) from t0 order by 1 limit 0";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:4: count"));
        Assert.assertTrue(explainString.contains("0:EMPTYSET"));
    }

    @Test
    public void testSubQueryWithLimit0() throws Exception {
        String queryStr = "select v1 from (select * from t0 limit 0) t";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("0:EMPTYSET"));

        queryStr = "select v1 from (select * from t0 order by v1 limit 0) t";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("0:EMPTYSET"));
    }

    @Test
    public void testSortWithLimit0() throws Exception {
        String queryStr = "select v1 from t0 order by v1 limit 0";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("0:EMPTYSET"));
    }

    @Test
    public void testAggSubQueryWithLimit0() throws Exception {
        String queryStr = "select sum(a) from (select v1 as a from t0 limit 0) t";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("0:EMPTYSET"));
        queryStr = "select sum(a) from (select v1 as a from t0 order by 1 limit 0) t";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("0:EMPTYSET"));
    }

    @Test
    public void testExceptLimit() throws Exception {
        String queryStr = "select 1 from (select 1, 3 from t0 except select 2, 3 ) as a limit 3";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("  7:Project\n" +
                "  |  <slot 11> : 1\n" +
                "  |  limit: 3\n" +
                "  |  \n" +
                "  0:EXCEPT\n" +
                "  |  limit: 3"));

        Assert.assertTrue(explainString.contains("  2:Project\n"
                + "  |  <slot 4> : 1\n"
                + "  |  <slot 5> : 3\n"
                + "  |  \n"
                + "  1:OlapScanNode"));
    }

    @Test
    public void testIntersectLimit() throws Exception {
        String queryStr = "select 1 from (select 1, 3 from t0 intersect select 2, 3 ) as a limit 3";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("  7:Project\n" +
                "  |  <slot 11> : 1\n" +
                "  |  limit: 3\n" +
                "  |  \n" +
                "  0:INTERSECT\n" +
                "  |  limit: 3"));

        Assert.assertTrue(explainString.contains("  2:Project\n"
                + "  |  <slot 4> : 1\n"
                + "  |  <slot 5> : 3\n"
                + "  |  \n"
                + "  1:OlapScanNode"));
    }


    @Test
    public void testCountStarWithLimitForOneAggStage() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select count(*) from (select v1 from t0 order by v2 limit 10,20) t;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan, plan.contains("4:AGGREGATE (update finalize)"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testSortWithLimitSubQuery() throws Exception {
        String sql = "select * from (select v1, v2 from t0 limit 10) a order by a.v1";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:EXCHANGE\n" +
                "     limit: 10"));

        sql = "select * from (select v1, v2 from t0 limit 10) a order by a.v1 limit 1000";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:EXCHANGE\n" +
                "     limit: 10"));

        sql = "select * from (select v1, v2 from t0 limit 10) a order by a.v1 limit 1";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:EXCHANGE\n" +
                "     limit: 10"));

        sql = "select * from (select v1, v2 from t0 limit 1) a order by a.v1 limit 10,1";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:EXCHANGE\n" +
                "     limit: 1"));
    }

    @Test
    public void testAggWithLimitSubQuery() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select a.v1 from (select v1, v2 from t0 limit 10) a group by a.v1";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:EXCHANGE\n" +
                "     limit: 10"));

        sql = "select a.v2 from (select v1, v2 from t0 limit 10) a group by a.v2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:EXCHANGE\n" +
                "     limit: 10"));

        sql = "select count(a.v2) from (select v1, v2 from t0 limit 10) a";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:EXCHANGE\n" +
                "     limit: 10"));

        sql = "select count(a.v2) from (select v1, v2 from t0 limit 10) a group by a.v2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:EXCHANGE\n" +
                "     limit: 10"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testWindowWithLimitSubQuery() throws Exception {
        String sql = "select sum(a.v1) over(partition by a.v2) from (select v1, v2 from t0 limit 10) a";
        String plan = getFragmentPlan(sql);

        Assert.assertTrue(plan.contains("  1:EXCHANGE\n" +
                "     limit: 10"));

        sql = "select sum(a.v1) over(partition by a.v2 order by a.v1) from (select v1, v2 from t0 limit 10) a";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:EXCHANGE\n" +
                "     limit: 10"));

        sql = "select sum(a.v1) over() from (select v1, v2 from t0 limit 10) a";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:EXCHANGE\n"
                + "     limit: 10\n"));
    }

    @Test
    public void testJoinWithLimitSubQuery() throws Exception {
        String sql = "select * from (select v1, v2 from t0 limit 10) a join " +
                "(select v1, v2 from t0 limit 1) b";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("    EXCHANGE ID: 03\n" +
                "    UNPARTITIONED"));
        Assert.assertTrue(plan.contains("    EXCHANGE ID: 01\n"
                + "    UNPARTITIONED\n"));
    }

    @Test
    public void testJoinWithLimitSubQuery1() throws Exception {
        String sql = "select * from (select v1, v2 from t0 limit 10) a join [broadcast] " +
                "(select v1, v2 from t0 limit 1) b on a.v1 = b.v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "    EXCHANGE ID: 04\n" +
                "    UNPARTITIONED");
    }

    @Test
    public void testJoinWithLimitSubQuery2() throws Exception {
        String sql = "select * from (select v1, v2 from t0) a join [broadcast] " +
                "(select v1, v2 from t0 limit 1) b on a.v1 = b.v1";
        String plan = getFragmentPlan(sql);

        Assert.assertTrue(plan.contains("    EXCHANGE ID: 02\n" +
                "    UNPARTITIONED"));
    }

    @Test
    public void testJoinWithLimitSubQuery3() throws Exception {
        String sql = "select * from (select v1, v2 from t0 limit 10) a join [shuffle] " +
                "(select v1, v2 from t0 limit 1) b on a.v1 = b.v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, ("join op: INNER JOIN (PARTITIONED)"));
        assertContains(plan, ("  6:SELECT\n" +
                "  |  predicates: 4: v1 IS NOT NULL\n" +
                "  |  \n" +
                "  5:EXCHANGE\n" +
                "     limit: 1"));
        assertContains(plan, ("  2:SELECT\n" +
                "  |  predicates: 1: v1 IS NOT NULL\n" +
                "  |  \n" +
                "  1:EXCHANGE\n" +
                "     limit: 10"));
    }

    @Test
    public void testJoinWithLimitSubQuery4() throws Exception {
        String sql = "select * from (select v1, v2 from t0) a join [shuffle] " +
                "(select v4 from t1 limit 1) b on a.v1 = b.v4";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("join op: INNER JOIN (PARTITIONED)"));
    }

    @Test
    public void testJoinWithLimitSubQuery5() throws Exception {
        String sql = "select * from (select v1, v2 from t0 limit 10) a join [shuffle] " +
                "(select v4 from t1 ) b on a.v1 = b.v4";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("join op: INNER JOIN (PARTITIONED)"));
    }

    @Test
    public void testUnionWithLimitSubQuery() throws Exception {
        String sql = "select v1, v2 from t0 union all " +
                "select v1, v2 from t0 limit 1 ";
        String plan = getFragmentPlan(sql);
        assertContains(plan, ("    EXCHANGE ID: 02\n" +
                "    RANDOM"));
        assertContains(plan, ("    EXCHANGE ID: 05\n" +
                "    UNPARTITIONED"));

        sql = "select v1, v2 from t0 union all " +
                "select a.v1, a.v2 from (select v1, v2 from t0 limit 1) a ";
        plan = getFragmentPlan(sql);
        assertContains(plan, ("    EXCHANGE ID: 04\n" +
                "    UNPARTITIONED"));
    }

    @Test
    public void testJoinLimit() throws Exception {
        String sql;
        String plan;
        sql = "select * from t0 inner join t1 on t0.v1 = t1.v4 limit 10";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: INNER JOIN (BROADCAST)\n"
                + "  |  colocate: false, reason: \n"
                + "  |  equal join conjunct: 1: v1 = 4: v4\n"
                + "  |  limit: 10\n"
                + "  |  \n"
                + "  |----2:EXCHANGE\n"
                + "  |    \n"
                + "  0:OlapScanNode\n"
                + "     TABLE: t0"));

        sql = "select * from t0 left anti join t1 on t0.v1 = t1.v4 limit 10";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: LEFT ANTI JOIN (BROADCAST)\n"
                + "  |  colocate: false, reason: \n"
                + "  |  equal join conjunct: 1: v1 = 4: v4\n"
                + "  |  limit: 10\n"
                + "  |  \n"
                + "  |----2:EXCHANGE\n"
                + "  |    \n"
                + "  0:OlapScanNode\n"
                + "     TABLE: t0\n"));

        sql = "select * from t0 right semi join t1 on t0.v1 = t1.v4 limit 10";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: LEFT SEMI JOIN (BROADCAST)\n"
                + "  |  colocate: false, reason: \n"
                + "  |  equal join conjunct: 4: v4 = 1: v1\n"
                + "  |  limit: 10\n"
                + "  |  \n"
                + "  |----2:EXCHANGE\n"
                + "  |    \n"
                + "  0:OlapScanNode\n"
                + "     TABLE: t1\n"));
    }

    @Test
    public void testJoinLimitLeft() throws Exception {
        String sql = "select * from t0 left outer join t1 on t0.v1 = t1.v4 limit 10";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode"));
        Assert.assertTrue(plan.contains("     TABLE: t0\n"
                + "     PREAGGREGATION: ON\n"
                + "     partitions=0/1\n"
                + "     rollup: t0\n"
                + "     tabletRatio=0/0\n"
                + "     tabletList=\n"
                + "     cardinality=1\n"
                + "     avgRowSize=3.0\n"
                + "     limit: 10"));
    }

    @Test
    public void testJoinLimitFull() throws Exception {
        String sql;
        String plan;
        sql = "select * from t0 full outer join t1 on t0.v1 = t1.v4 limit 10";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  4:HASH JOIN\n"
                + "  |  join op: FULL OUTER JOIN (PARTITIONED)\n"
                + "  |  colocate: false, reason: \n"
                + "  |  equal join conjunct: 1: v1 = 4: v4\n"
                + "  |  limit: 10\n"
                + "  |  \n"
                + "  |----3:EXCHANGE\n"
                + "  |    \n"
                + "  1:EXCHANGE\n"));

        sql = "select * from t0, t1 limit 10";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan, plan.contains("3:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
                "  |       limit: 10\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0"));
    }

    @Test
    public void testSqlSelectLimitSession() throws Exception {
        connectContext.getSessionVariable().setSqlSelectLimit(10);
        String sql = "select * from test_all_type";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("limit: 10"));

        connectContext.getSessionVariable().setSqlSelectLimit(10);
        sql = "select * from test_all_type limit 20000";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("limit: 20000"));

        connectContext.getSessionVariable().setSqlSelectLimit(SessionVariable.DEFAULT_SELECT_LIMIT);
        sql = "select * from test_all_type";
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("limit: 10"));

        connectContext.getSessionVariable().setSqlSelectLimit(8888);
        sql = "select * from (select * from test_all_type limit 10) as a join " +
                "(select * from test_all_type limit 100) as b";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("limit: 8888"));
        connectContext.getSessionVariable().setSqlSelectLimit(SessionVariable.DEFAULT_SELECT_LIMIT);

        connectContext.getSessionVariable().setSqlSelectLimit(0);
        sql = "select * from test_all_type";
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("limit"));

        connectContext.getSessionVariable().setSqlSelectLimit(0);
        sql = "select * from test_all_type";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("EMPTYSET"));
        connectContext.getSessionVariable().setSqlSelectLimit(SessionVariable.DEFAULT_SELECT_LIMIT);
    }

    @Test
    public void testLimitRightJoin() throws Exception {
        String sql = "select v1 from t0 right outer join t1 on t0.v1 = t1.v4 limit 100";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  limit: 100");
        Assert.assertTrue(plan.contains("  |----3:EXCHANGE\n" +
                "  |       limit: 100"));

        sql = "select v1 from t0 full outer join t1 on t0.v1 = t1.v4 limit 100";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("join op: FULL OUTER JOIN (PARTITIONED)"));
    }

    @Test
    public void testLimitLeftJoin() throws Exception {
        String sql = "select v1 from (select * from t0 limit 1) x0 left outer join[shuffle] t1 on x0.v1 = t1.v4";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" 5:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  2:EXCHANGE\n" +
                "     limit: 1"));

        sql = "select v1 from (select * from t0 limit 10) x0 left outer join t1 on x0.v1 = t1.v4 limit 1";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("4:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  limit: 1\n" +
                "  |  \n" +
                "  |----3:EXCHANGE\n" +
                "  |    \n" +
                "  1:EXCHANGE\n" +
                "     limit: 1"));
        Assert.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=1.0\n" +
                "     limit: 1"));

        sql = "select v1 from (select * from t0 limit 10) x0 left outer join[shuffle] t1 on x0.v1 = t1.v4 limit 100";
        plan = getFragmentPlan(sql);
        assertContains(plan, "5:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  limit: 100\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  2:EXCHANGE\n" +
                "     limit: 10");
        assertContains(plan, ("PLAN FRAGMENT 3\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    HASH_PARTITIONED: 1: v1\n" +
                "\n" +
                "  1:EXCHANGE\n" +
                "     limit: 10"));

        sql = "select v1 from (select * from t0 limit 10) x0 left outer " +
                "join (select * from t1 limit 5) x1 on x0.v1 = x1.v4 limit 7";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("5:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  limit: 7\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |       limit: 5\n" +
                "  |    \n" +
                "  1:EXCHANGE\n" +
                "     limit: 7"));
        assertContains(plan, ("PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 04\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  3:EXCHANGE\n" +
                "     limit: 5"));
    }

    @Test
    public void testCrossJoinPushLimit() throws Exception {
        String sql = "select * from t0 cross join t1 on t0.v2 != t1.v5 limit 10";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan, plan.contains("  3:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: 2: v2 != 5: v5\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode"));

        sql = "select * from t0 inner join t1 on t0.v2 != t1.v5 limit 10";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan, plan.contains("3:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: 2: v2 != 5: v5\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode"));
    }

    @Test
    public void testJoinWithLimit() throws Exception {
        String sql = "select t2.v8 from (select v1, v2, v1 as v3 from t0 where v2<> v3 limit 15) as a join t1 " +
                "on a.v3 = t1.v4 join t2 on v4 = v7 join t2 as b" +
                " on a.v1 = b.v7 where b.v8 > t1.v5 limit 10";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "14:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  other join predicates: 11: v8 > 5: v5\n" +
                "  |  limit: 10");
        // check join on predicate which has expression with limit operator
        sql = "select t2.v8 from (select v1, v2, v1 as v3 from t0 where v2<> v3 limit 15) as a join t1 " +
                "on a.v3 + 1 = t1.v4 join t2 on v4 = v7 join t2 as b" +
                " on a.v3 + 2 = b.v7 where b.v8 > t1.v5 limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "15:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 14: add = 4: v4\n" +
                "  |  other join predicates: 11: v8 > 5: v5\n" +
                "  |  limit: 10");
    }

    @Test
    public void testLimitPushDownJoin() throws Exception {
        String sql = "select * from t0 left join[shuffle] t1 on t0.v2 = t1.v5 where t1.v6 is null limit 2";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: LEFT OUTER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 5: v5\n" +
                "  |  other predicates: 6: v6 IS NULL\n" +
                "  |  limit: 2"));
        Assert.assertTrue(plan.contains("     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=3.0\n"));
    }

    @Test
    public void testOffsetWithSubTopN() throws Exception {
        String sql;
        String plan;
        sql = "select v1 from (\n" +
                "  select * from (select v1, v2 from t0 order by v1 asc limit 1000, 600) l\n" +
                "  left join (select null as cx, '1' as c1) r\n" +
                "  on l.v1 =r.cx\n" +
                ") b limit 600;";
        plan = getThriftPlan(sql);
        assertContains(plan, "TExchangeNode(input_row_tuples:[1], sort_info:" +
                "TSortInfo(ordering_exprs:[TExpr(nodes:[TExprNode(node_type:SLOT_REF");

        sql = "select * from (select v1, v2 from t0 order by v1 asc limit 1000, 600) l limit 200, 600";
        plan = getFragmentPlan(sql);
        assertContains(plan, "\n" +
                "  2:MERGING-EXCHANGE\n" +
                "     offset: 1200\n" +
                "     limit: 400");
    }

    @Test
    public void testUnionLimit() throws Exception {
        String queryStr = "select 1 from (select 4, 3 from t0 union all select 2, 3 ) as a limit 3";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("  2:Project\n"
                + "  |  <slot 4> : 4\n"
                + "  |  limit: 3\n"
                + "  |  \n"
                + "  1:OlapScanNode"));
    }

    @Test
    public void testMergeLimitForFilterNode() throws Exception {
        String sql =
                "SELECT CAST(nullif(subq_0.c1, subq_0.c1) AS INTEGER) AS c0, subq_0.c0 AS c1, 42 AS c2, subq_0.c0 AS "
                        + "c3, subq_0.c1 AS c4, subq_0.c0 AS c5, subq_0.c0 AS c6\n" +
                        "FROM (\n" +
                        "\tSELECT ref_2.v8 AS c0, ref_2.v8 AS c1\n" +
                        "\tFROM t2 ref_0\n" +
                        "\t\tRIGHT JOIN t1 ref_1 ON ref_0.v7 = ref_1.v4\n" +
                        "\t\tRIGHT JOIN t2 ref_2 ON ref_1.v4 = ref_2.v7\n" +
                        "\tWHERE ref_1.v4 IS NOT NULL\n" +
                        "\tLIMIT 110\n" +
                        ") subq_0\n" +
                        "WHERE CAST(coalesce(true, true) AS BOOLEAN) < true\n" +
                        "LIMIT 157";
        String plan = getFragmentPlan(sql);
        assertContains(plan, ("EMPTYSET"));
    }

    @Test
    public void testWhereLimitSubquery() throws Exception {
        String sql = "select * from (select * from t0 limit 2) xx where xx.v1 = 1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  2:SELECT\n" +
                "  |  predicates: 1: v1 = 1\n" +
                "  |  \n" +
                "  1:EXCHANGE\n" +
                "     limit: 2");
    }

    @Test
    public void testGroupingSetLimitSubquery() throws Exception {
        String sql = "select v1, v2, v3 from (select * from t0 limit 2) xx group by cube(v1, v2, v3)";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  2:REPEAT_NODE\n" +
                "  |  repeat: repeat 7 lines [[], [1], [2], [1, 2], [3], [1, 3], [2, 3], [1, 2, 3]]\n" +
                "  |  \n" +
                "  1:EXCHANGE\n" +
                "     limit: 2");
    }

    @Test
    public void testProjectLimitSubquery() throws Exception {
        String sql = "select v1 + 1, v2 + 2, v3 + 3 from (select * from t0 limit 2) xx ";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  <slot 4> : 1: v1 + 1\n" +
                "  |  <slot 5> : 2: v2 + 2\n" +
                "  |  <slot 6> : 3: v3 + 3\n" +
                "  |  limit: 2\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0");
    }

    @Test
    public void testSubqueryLimit() throws Exception {
        String sql = "SELECT \n" +
                "  `s0`.`S_NATIONKEY`, \n" +
                "  `s0`.`S_NAME`, \n" +
                "  `s0`.`S_ADDRESS` \n" +
                "FROM \n" +
                "  (\n" +
                "    SELECT \n" +
                "      `S_NAME`, \n" +
                "      `S_ADDRESS`, \n" +
                "      `S_NATIONKEY` \n" +
                "    FROM \n" +
                "      `supplier` \n" +
                "    GROUP BY \n" +
                "      `S_NAME`, \n" +
                "      `S_ADDRESS`, \n" +
                "      `S_NATIONKEY`\n" +
                "  ) AS `s0`, \n" +
                "  (\n" +
                "    SELECT \n" +
                "      * \n" +
                "    FROM \n" +
                "      (\n" +
                "        SELECT \n" +
                "          `S_SUPPKEY`, \n" +
                "          `S_NAME`, \n" +
                "          `S_ADDRESS`, \n" +
                "          `S_NATIONKEY`, \n" +
                "          `S_PHONE`, \n" +
                "          `S_ACCTBAL`, \n" +
                "          `S_COMMENT` \n" +
                "        FROM \n" +
                "          `supplier` \n" +
                "        ORDER BY \n" +
                "          `S_NATIONKEY` IS NULL, \n" +
                "          `S_NATIONKEY` \n" +
                "        LIMIT \n" +
                "          15 OFFSET 6\n" +
                "      ) AS `t0`\n" +
                "  ) AS `t1` \n" +
                "LIMIT \n" +
                "  61202;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "7:MERGING-EXCHANGE\n" +
                "     offset: 6\n" +
                "     limit: 15");
        assertContains(plan, "4:TOP-N\n" +
                "  |  order by: <slot 17> 17: expr ASC, <slot 12> 12: S_NATIONKEY ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 21");

        sql = "SELECT \n" +
                "      * \n" +
                "    FROM \n" +
                "      (\n" +
                "        SELECT \n" +
                "          `S_SUPPKEY`, \n" +
                "          `S_NAME`, \n" +
                "          `S_ADDRESS`, \n" +
                "          `S_NATIONKEY`, \n" +
                "          `S_PHONE`, \n" +
                "          `S_ACCTBAL`, \n" +
                "          `S_COMMENT` \n" +
                "        FROM \n" +
                "          `supplier` \n" +
                "        ORDER BY \n" +
                "          `S_NATIONKEY` IS NULL, \n" +
                "          `S_NATIONKEY` \n" +
                "        LIMIT \n" +
                "          15 OFFSET 6\n" +
                "      ) AS `t0` limit 100";
        plan = getFragmentPlan(sql);
        assertContains(plan, "2:TOP-N\n" +
                "  |  order by: <slot 9> 9: expr ASC, <slot 4> 4: S_NATIONKEY ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 21");
        assertContains(plan, "5:MERGING-EXCHANGE\n" +
                "     offset: 6\n" +
                "     limit: 15");
    }

    // LimitPruneTabletsRule shouldn't prune tablets when totalRow less than limit
    @Test
    public void testLimitPrune() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();

        // We need to let some tablets have data, some tablets don't data
        OlapTable t0 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t0");
        MaterializedIndex index = t0.getPartitions().stream().findFirst().get().getDefaultPhysicalPartition().getBaseIndex();
        LocalTablet tablets = (LocalTablet) index.getTablets().get(0);
        Replica replica = tablets.getSingleReplica();
        new Expectations(replica) {
            {
                replica.getRowCount();
                result = 10;
                minTimes = 0;
            }
        };

        boolean flag = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        String sql = "select * from t0 limit 1000";
        String planFragment = getFragmentPlan(sql);
        // Shouldn't prune tablets
        assertNotContains(planFragment, "tabletRatio=1/3");
        assertContains(planFragment, "tabletRatio=3/3");
        FeConstants.runningUnitTest = flag;
    }

    @Test
    public void testConstantOrderByLimit() throws Exception {
        String sql = "select * from t0 order by 'abc' limit 100, 100 ";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:MERGING-EXCHANGE\n" +
                "     offset: 100\n" +
                "     limit: 100");
    }

    @Test
    public void testOrderByConstant() throws Exception {
        String sql = "select * from t0 limit 100, 100";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:MERGING-EXCHANGE\n" +
                "     offset: 100\n" +
                "     limit: 100");
    }

    @Test
    public void testMergeLimit() throws Exception {
        String sql = "select * from (select * from t0 limit 100, 100) x limit 1, 2";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:MERGING-EXCHANGE\n" +
                "     offset: 101\n" +
                "     limit: 2");

        sql = "select * from (select * from t0 limit 1, 2) x limit 100, 100";
        plan = getFragmentPlan(sql);
        assertContains(plan, "0:EMPTYSET");

        sql = "select * from (select * from t0 limit 1, 5) x limit 1, 100";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:MERGING-EXCHANGE\n" +
                "     offset: 2\n" +
                "     limit: 4");

        sql = "select * from (select * from t0 limit 1, 5) x limit 3";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:MERGING-EXCHANGE\n" +
                "     offset: 1\n" +
                "     limit: 3");

        sql = "select * from (select * from t0 limit 100) x limit 5";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:EXCHANGE\n" +
                "     limit: 5");
    }

    @Test
    public void testLimitAgg() throws Exception {
        String sql = "select count(*) from (select * from t0 limit 50, 20) xx;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  2:MERGING-EXCHANGE\n" +
                "     offset: 50\n" +
                "     limit: 20");
    }

    @Test
    public void testLimitValues() throws Exception {
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(-1);
        String sql = "select count(*) from (select * from TABLE(generate_series(1, 100000)) limit 50000, 10) x;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "4:MERGING-EXCHANGE\n" +
                "     offset: 50000\n" +
                "     limit: 10");
    }

    @Test
    public void testLimitValues2() throws Exception {
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(-1);
        String sql = "select count(*) from (select * from (select * from t0 limit 10) x limit 10, 10) xx;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "0:EMPTYSET");
    }

    @Test
    public void testMergeLimitInCte() throws Exception {
        String sql = "with with_t_0 as (select v1 from t0 where EXISTS (select v4 from t1)) \n" +
                "select distinct 1 from with_t_0, (select v7, v8 from t2) subt right SEMI join t0 on subt.v8 = v2 \n" +
                "union all \n" +
                "select distinct 2 from with_t_0, (select v10, v11 from t3) subt right SEMI join t0 on subt.v11 = v3;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "7:UNION\n" +
                "  |  \n" +
                "  |----33:EXCHANGE\n" +
                "  |       limit: 1\n" +
                "  |    \n" +
                "  20:EXCHANGE\n" +
                "     limit: 1");
    }

    @Test
    public void testTransformGroupByToLimit() throws Exception {
        String sql = "select distinct v1 from (select t0.* from t0 join (select * from t1 where false) t1 " +
                "right join t2 on t0.v1 = t2.v7) t";
        String plan = getFragmentPlan(sql);
        assertCContains(plan, "1:Project\n" +
                "  |  <slot 1> : NULL\n" +
                "  |  limit: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t2",
                "RESULT SINK\n" +
                        "\n" +
                        "  2:EXCHANGE\n" +
                        "     limit: 1");
    }
}
