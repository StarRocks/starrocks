// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.SessionVariable;
import com.starrocks.system.BackendCoreStat;
import com.starrocks.thrift.TExplainLevel;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

public class OrderByTest extends PlanTestBase {

    @Test
    public void testParallelism() throws Exception {
        int numCores = 8;
        int expectDop = numCores / 2;
        new MockUp<BackendCoreStat>() {
            @Mock
            public int getAvgNumOfHardwareCoresOfBe() {
                return 8;
            }
        };

        boolean enablePipeline = true;
        int pipelineDop = 0;
        try {
            enablePipeline = connectContext.getSessionVariable().isEnablePipelineEngine();
            pipelineDop = connectContext.getSessionVariable().getPipelineDop();

            connectContext.getSessionVariable().setEnablePipelineEngine(true);
            connectContext.getSessionVariable().setPipelineDop(0);

            String sql = "select * from t0 order by v1 limit 100";
            ExecPlan plan = getExecPlan(sql);
            PlanFragment fragment1 = plan.getFragments().get(1);
            assertContains(fragment1.getExplainString(TExplainLevel.NORMAL), "TOP-N");
            Assert.assertEquals(1, fragment1.getParallelExecNum());
            Assert.assertEquals(expectDop, fragment1.getPipelineDop());
        } finally {
            connectContext.getSessionVariable().setEnablePipelineEngine(enablePipeline);
            connectContext.getSessionVariable().setPipelineDop(pipelineDop);
        }

    }

    @Test
    public void testSqlSelectLimit() throws Exception {
        connectContext.getSessionVariable().setSqlSelectLimit(200);
        // test order by with project
        String sql;
        String plan;

        sql = "select L_QUANTITY from lineitem order by L_QUANTITY, L_PARTKEY";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:TOP-N\n" +
                "  |  order by: <slot 5> 5: L_QUANTITY ASC, <slot 2> 2: L_PARTKEY ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 200");

        sql = sql + " limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:TOP-N\n" +
                "  |  order by: <slot 5> 5: L_QUANTITY ASC, <slot 2> 2: L_PARTKEY ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 10");
        connectContext.getSessionVariable().setSqlSelectLimit(SessionVariable.DEFAULT_SELECT_LIMIT);
    }

    @Test
    public void testOrderByWithSubquery() throws Exception {
        String sql = "select t0.*, " +
                "(select sum(v5) from t1) as x1, " +
                "(select sum(v7) from t2) as x2 from t0 order by t0.v3";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  11:SORT\n" +
                "  |  order by: <slot 3> 3: v3 ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  10:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  <slot 8> : 8: expr\n" +
                "  |  <slot 13> : 12: sum\n" +
                "  |  \n" +
                "  9:CROSS JOIN\n" +
                "  |  cross join:\n" +
                "  |  predicates is NULL.");
    }

    @Test
    public void tstOrderByNullLiteral() throws Exception {
        String sql;
        String plan;

        sql = "select * from t0 order by null limit 10;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=3.0\n" +
                "     numNodes=0\n" +
                "     limit: 10");

        sql = "select * from (select max(v5) from t1) tmp order by null limit 10;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  output: max(2: v5)\n" +
                "  |  group by: \n" +
                "  |  limit: 10");

        // TODO opt this case
        sql = "select * from (select max(v5) from t1) tmp order by \"\" > null limit 10;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  3:TOP-N\n" +
                "  |  order by: <slot 5> 5: expr ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  2:Project\n" +
                "  |  <slot 4> : 4: max\n" +
                "  |  <slot 5> : NULL");
    }

    @Test
    public void testOrderByGroupByWithSubquery() throws Exception {
        String sql = "select t0.v2, sum(v3) as x3, " +
                "(select sum(v5) from t1) as x1, " +
                "(select sum(v7) from t2) as x2 from t0 group by v2 order by x3";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  12:SORT\n" +
                "  |  order by: <slot 4> 4: sum ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  11:Project\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 4> : 4: sum\n" +
                "  |  <slot 9> : 9: expr\n" +
                "  |  <slot 14> : 13: sum\n" +
                "  |  \n" +
                "  10:CROSS JOIN\n" +
                "  |  cross join:\n" +
                "  |  predicates is NULL." +
                "");
    }

    @Test
    public void testOrderByTransform() throws Exception {
        String sql = "select v1, * from test.t0 order by v2";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 2> 2: v2 ASC");

        sql = "select v1+1 as v from t0 group by v1+1 order by v";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 4> 4: expr ASC");
        assertContains(plan, "<slot 4> : 1: v1 + 1");

        sql = "select distinct v1, v2 from t0 order by v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:SORT\n" +
                "  |  order by: <slot 2> 2: v2 ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  group by: 1: v1, 2: v2");

        sql = "select distinct v1, v2 from t0 order by v1 + 1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "3:SORT\n" +
                "  |  order by: <slot 4> 4: expr ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  2:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 4> : 1: v1 + 1");

        sql = "select * from t0 a join t0 b where a.v1 = b.v1 order by a.v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 2> 2: v2 ASC");

        sql = "select v1 v from t0 order by v";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 1> 1: v1 ASC");

        sql = "select v1 v from t0 order by v1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 1> 1: v1 ASC");

        sql = "select sum(v1) as v, v2, v3 from t0 group by v2, v3 order by v";
        plan = getFragmentPlan(sql);
        assertContains(plan, "2:SORT\n" +
                "  |  order by: <slot 4> 4: sum ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(1: v1)\n" +
                "  |  group by: 2: v2, 3: v3");

        sql = "select sum(v1) as v, v2 from t0 group by v2, v3 order by v";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  3:SORT\n" +
                "  |  order by: <slot 4> 4: sum ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  2:Project\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 4> : 4: sum\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(1: v1)\n" +
                "  |  group by: 2: v2, 3: v3");


        sql = "select abs(t0.v1), abs(t1.v1) from t0, t0_not_null t1 order by t1.v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "5:SORT\n" +
                "  |  order by: <slot 5> 5: v2 ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  4:Project\n" +
                "  |  <slot 5> : 5: v2\n" +
                "  |  <slot 7> : abs(1: v1)\n" +
                "  |  <slot 8> : abs(4: v1)");

        sql = "select abs(t0.v1), abs(t1.v1) from t0, t0_not_null t1 order by t1.v1";
        plan = getFragmentPlan(sql);
        assertContains(plan, " 5:SORT\n" +
                "  |  order by: <slot 4> 4: v1 ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  4:Project\n" +
                "  |  <slot 4> : 4: v1\n" +
                "  |  <slot 7> : abs(1: v1)\n" +
                "  |  <slot 8> : abs(4: v1)");
    }

    @Test
    public void testUDTFWithOrderBy() throws Exception {
        String sql = "select t.* from t0, unnest([1,2,3]) as t order by `unnest`";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 4> 4: unnest ASC");
    }

    @Test
    public void testOrderByWithSameColumnName() throws Exception {
        String sql = "select t0_not_null.*, t0.* from t0, t0_not_null order by t0_not_null.v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 4> 4: v1 ASC");

        sql = "select t0_not_null.*, t0.* from t0, t0_not_null order by t0.v1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 1> 1: v1 ASC");

        sql = "select t0.*, t0_not_null.* from t0, t0_not_null order by t0.v1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 1> 1: v1 ASC");

        sql = "select t0.*, t0_not_null.* from t0, t0_not_null order by t0_not_null.v1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 4> 4: v1 ASC");

        sql = "select t0.v1, t0_not_null.v1 from t0, t0_not_null order by t0_not_null.v1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 4> 4: v1 ASC");
    }

    @Test
    public void testOrderByWithFieldReference() throws Exception {
        String sql = "select * from t0 order by 1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 1> 1: v1 ASC");

        sql = "select * from t0 order by 3";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 3> 3: v3 ASC");

        sql = "select *, v1 from t0 order by 3";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 3> 3: v3 ASC");

        sql = "select *, v1 from t0 order by 4";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 1> 1: v1 ASC");

        sql = "select v1, v1 from t0 order by 1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 1> 1: v1 ASC");

        sql = "select v1, v1 from t0 order by 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 1> 1: v1 ASC");

        sql = "select v1 as v, v1 as v from t0 order by 1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 1> 1: v1 ASC");

        sql = "select v1 as v, v1 as v from t0 order by 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 1> 1: v1 ASC");

        sql = "select v1 as v, *, v2 + 1 as vv from t0 order by 5";
        plan = getFragmentPlan(sql);
        assertContains(plan, "2:SORT\n" +
                "  |  order by: <slot 4> 4: expr ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  <slot 4> : 2: v2 + 1");
    }
}