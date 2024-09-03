package com.starrocks.qe.feedback.guide;

import com.starrocks.common.FeConstants;
import com.starrocks.sql.plan.DistributedEnvPlanTestBase;
import org.junit.jupiter.api.BeforeAll;

class RightChildEstimationErrorTuningGuideTest extends DistributedEnvPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        DistributedEnvPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }

}