// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.starrocks.common.FeConstants;
import org.junit.BeforeClass;
import org.junit.Test;

public class AggregatePushDownTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setNewPlanerAggStage(1);
        connectContext.getSessionVariable().setCboPushDownAggregateMode(1);
    }

    @Test
    public void testPushDown() {
        runFileUnitTest("optimized-plan/agg-pushdown", false);
    }
}
