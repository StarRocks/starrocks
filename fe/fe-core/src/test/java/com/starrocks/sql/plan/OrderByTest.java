// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
    public void testExistOrderBy() throws Exception {
        String sql = "SELECT * \n" +
                "FROM   emp \n" +
                "WHERE  EXISTS (SELECT dept.dept_id \n" +
                "               FROM   dept \n" +
                "               WHERE  emp.dept_id = dept.dept_id \n" +
                "               ORDER  BY state) \n" +
                "ORDER  BY hiredate";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("LEFT SEMI JOIN"));
    }

    @Test
    public void testSort() throws Exception {
        String sql = "select count(*) from (select L_QUANTITY, L_PARTKEY, L_ORDERKEY from lineitem " +
                "order by L_QUANTITY, L_PARTKEY, L_ORDERKEY limit 5000, 10000) as a;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("2:MERGING-EXCHANGE"));
    }

    @Test
    public void testPruneSortColumns() throws Exception {
        String sql = "select count(v1) from (select v1 from t0 order by v2 limit 10) t";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:Project\n" +
                "  |  <slot 1> : 1: v1"));
    }

    @Test
    public void testSortProject() throws Exception {
        String sql = "select avg(null) over (order by ref_0.v1) as c2 "
                + "from t0 as ref_0 left join t1 as ref_1 on (ref_0.v1 = ref_1.v4 );";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains(
                "sort_tuple_slot_exprs:[TExpr(nodes:[TExprNode(node_type:SLOT_REF, type:TTypeDesc(types:[TTypeNode"
                        + "(type:SCALAR, scalar_type:TScalarType(type:BIGINT))]), num_children:0, slot_ref:TSlotRef"
                        + "(slot_id:1, tuple_id:2), output_scale:-1, output_column:-1, "
                        + "has_nullable_child:false, is_nullable:true, is_monotonic:true)])]"));
    }

    @Test
    public void testTopNOffsetError() throws Exception {
        long limit = connectContext.getSessionVariable().getSqlSelectLimit();
        connectContext.getSessionVariable().setSqlSelectLimit(200);
        String sql = "select * from (select * from t0 order by v1 limit 5) as a left join t1 on a.v1 = t1.v4";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:TOP-N\n" +
                "  |  order by: <slot 1> 1: v1 ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 5"));
        connectContext.getSessionVariable().setSqlSelectLimit(limit);
    }

    @Test
    public void testOrderBySameColumnDiffOrder() throws Exception {
        String sql = "select v1 from t0 order by v1 desc, v1 asc";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:SORT\n" +
                "  |  order by: <slot 1> 1: v1 DESC"));
    }

    @Test
    public void testUnionOrderByDuplicateColumn() throws Exception {
        String sql = "select * from t0 union all select * from t1 order by v1, v2, v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 7> 7: v1 ASC, <slot 8> 8: v2 ASC");
    }

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
        assertContains(plan, "  15:SORT\n" +
                "  |  order by: <slot 3> 3: v3 ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  14:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  <slot 8> : 8: sum\n" +
                "  |  <slot 13> : 12: sum\n" +
                "  |  \n" +
                "  13:NESTLOOP JOIN");
    }

    @Test
    public void testOrderByGroupByWithSubquery() throws Exception {
        String sql = "select t0.v2, sum(v3) as x3, " +
                "(select sum(v5) from t1) as x1, " +
                "(select sum(v7) from t2) as x2 from t0 group by v2 order by x3";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  16:SORT\n" +
                "  |  order by: <slot 4> 4: sum ASC\n" +
                "  |  offset: 0\n" +
                "  |  \n" +
                "  15:Project\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 4> : 4: sum\n" +
                "  |  <slot 9> : 9: sum\n" +
                "  |  <slot 14> : 13: sum\n" +
                "  |  \n" +
                "  14:NESTLOOP JOIN\n");
    }
}
