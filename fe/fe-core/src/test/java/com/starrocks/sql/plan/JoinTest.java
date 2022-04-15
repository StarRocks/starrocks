// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.common.FeConstants;
import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.SessionVariable;
import com.starrocks.system.BackendCoreStat;
import com.starrocks.thrift.TExplainLevel;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

public class JoinTest extends PlanTestBase {
    @Test
    public void testParallelism() throws Exception {
        int numCores = 8;
        int expectedParallelism = numCores / 2;
        new MockUp<BackendCoreStat>() {
            @Mock
            public int getAvgNumOfHardwareCoresOfBe() {
                return numCores;
            }
        };

        SessionVariable sessionVariable = connectContext.getSessionVariable();
        boolean enablePipeline = sessionVariable.isEnablePipelineEngine();
        int pipelineDop = sessionVariable.getPipelineDop();
        int parallelExecInstanceNum = sessionVariable.getParallelExecInstanceNum();

        try {
            // Enable DopAutoEstimate.
            sessionVariable.setEnablePipelineEngine(true);
            sessionVariable.setPipelineDop(0);
            sessionVariable.setParallelExecInstanceNum(1);
            FeConstants.runningUnitTest = true;

            // Case 1: local bucket shuffle join should use fragment instance parallel.
            String sql = "select a.v1 from t0 a join [bucket] t0 b on a.v1 = b.v2 and a.v2 = b.v1";
            ExecPlan plan = getExecPlan(sql);
            PlanFragment fragment = plan.getFragments().get(1);
            assertContains(fragment.getExplainString(TExplainLevel.NORMAL), "join op: INNER JOIN (BUCKET_SHUFFLE)");
            Assert.assertEquals(expectedParallelism, fragment.getParallelExecNum());
            Assert.assertEquals(1, fragment.getPipelineDop());

            // Case 2: colocate join should use fragment instance parallel.
            sql = "SELECT * from t0 join t0 as b on t0.v1 = b.v1;";
            plan = getExecPlan(sql);
            fragment = plan.getFragments().get(1);
            assertContains(fragment.getExplainString(TExplainLevel.NORMAL), "join op: INNER JOIN (COLOCATE)");
            Assert.assertEquals(expectedParallelism, fragment.getParallelExecNum());
            Assert.assertEquals(1, fragment.getPipelineDop());

            // Case 3: broadcast join should use pipeline parallel.
            sql = "select a.v1 from t0 a join [broadcast] t0 b on a.v1 = b.v2 and a.v2 = b.v1";
            plan = getExecPlan(sql);
            fragment = plan.getFragments().get(1);
            assertContains(fragment.getExplainString(TExplainLevel.NORMAL), "join op: INNER JOIN (BROADCAST)");
            Assert.assertEquals(1, fragment.getParallelExecNum());
            Assert.assertEquals(expectedParallelism, fragment.getPipelineDop());

            // Case 4: local bucket shuffle join succeeded by broadcast should use fragment instance parallel.
            sql = "select a.v1 from t0 a " +
                    "join [bucket] t0 b on a.v1 = b.v2 and a.v2 = b.v1 " +
                    "join [broadcast] t0 c on a.v1 = c.v2";
            plan = getExecPlan(sql);
            fragment = plan.getFragments().get(1);
            String fragmentString = fragment.getExplainString(TExplainLevel.NORMAL);
            assertContains(fragmentString, "join op: INNER JOIN (BROADCAST)");
            assertContains(fragmentString, "join op: INNER JOIN (BUCKET_SHUFFLE)");
            Assert.assertEquals(expectedParallelism, fragment.getParallelExecNum());
            Assert.assertEquals(1, fragment.getPipelineDop());
        } finally {
            sessionVariable.setEnablePipelineEngine(enablePipeline);
            sessionVariable.setPipelineDop(pipelineDop);
            sessionVariable.setParallelExecInstanceNum(parallelExecInstanceNum);
            FeConstants.runningUnitTest = false;
        }
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
}
