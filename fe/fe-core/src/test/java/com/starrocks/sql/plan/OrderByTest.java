// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.planner.PlanFragment;
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
}
