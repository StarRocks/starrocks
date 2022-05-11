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
}
