// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.starrocks.common.FeConstants;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class EnumeratePlanTest extends DistributedEnvPlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        DistributedEnvPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setMaxTransformReorderJoins(4);
        connectContext.getSessionVariable().setCboPruneShuffleColumnRate(0);
    }

    @After
    public void after() {
        connectContext.getSessionVariable().setUseNthExecPlan(0);
    }

    @Test
    public void testThreeTableJoinEnumPlan() {
        runFileUnitTest("enumerate-plan/three-join");
    }

    @Test
    public void testTPCHQ1EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q1");
    }

    @Test
    public void testTPCHQ2EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q2");
    }

    @Test
    public void testTPCHQ3EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q3");
    }

    @Test
    public void testTPCHQ4EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q4");
    }

    @Test
    public void testTPCHQ5EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q5", true);
    }

    @Test
    public void testTPCHQ6EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q6");
    }

    @Test
    public void testTPCHQ7EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q7");
    }

    @Test
    public void testTPCHQ8EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q8");
    }

    @Test
    public void testTPCHQ9EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q9", true);
    }

    @Test
    public void testTPCHQ10EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q10", true);
    }

    @Test
    public void testTPCHQ11EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q11");
    }

    @Test
    public void testTPCHQ12EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q12");
    }

    @Test
    public void testTPCHQ13EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q13");
    }

    @Test
    public void testTPCHQ14EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q14");
    }

    @Test
    public void testTPCHQ15EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q15");
    }

    @Test
    public void testTPCHQ16EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q16");
    }

    @Test
    public void testTPCHQ17EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q17");
    }

    @Test
    public void testTPCHQ18EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q18", true);
    }

    @Test
    public void testTPCHQ19EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q19");
    }

    @Test
    public void testTPCHQ20EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q20");
    }

    @Test
    public void testTPCHQ21EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q21", true);
    }

    @Test
    public void testTPCHQ22EnumPlan() {
        runFileUnitTest("enumerate-plan/tpch-q22");
    }
}
