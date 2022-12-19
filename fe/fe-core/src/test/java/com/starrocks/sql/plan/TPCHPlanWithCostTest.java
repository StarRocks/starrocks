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

import com.starrocks.common.FeConstants;
import org.junit.BeforeClass;
import org.junit.Test;

public class TPCHPlanWithCostTest extends DistributedEnvPlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        DistributedEnvPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setEnableGlobalRuntimeFilter(true);
        connectContext.getSessionVariable().setEnableMultiColumnsOnGlobbalRuntimeFilter(true);
    }

    @Test
    public void testTPCH1() {
        runFileUnitTest("tpchcost/q1");
    }

    @Test
    public void testTPCH2() {
        runFileUnitTest("tpchcost/q2");
    }

    @Test
    public void testTPCH3() {
        runFileUnitTest("tpchcost/q3");
    }

    @Test
    public void testTPCH4() {
        runFileUnitTest("tpchcost/q4");
    }

    @Test
    public void testTPCH5() {
        runFileUnitTest("tpchcost/q5");
    }

    @Test
    public void testTPCH6() {
        runFileUnitTest("tpchcost/q6");
    }

    @Test
    public void testTPCH7() {
        runFileUnitTest("tpchcost/q7");
    }

    @Test
    public void testTPCH8() {
        runFileUnitTest("tpchcost/q8");
    }

    @Test
    public void testTPCH9() {
        runFileUnitTest("tpchcost/q9");
    }

    @Test
    public void testTPCH10() {
        runFileUnitTest("tpchcost/q10");
    }

    @Test
    public void testTPCH11() {
        runFileUnitTest("tpchcost/q11");
    }

    @Test
    public void testTPCH12() {
        runFileUnitTest("tpchcost/q12");
    }

    @Test
    public void testTPCH13() {
        runFileUnitTest("tpchcost/q13");
    }

    @Test
    public void testTPCH14() {
        runFileUnitTest("tpchcost/q14");
    }

    @Test
    public void testTPCH15() {
        runFileUnitTest("tpchcost/q15");
    }

    @Test
    public void testTPCH16() {
        runFileUnitTest("tpchcost/q16");
    }

    @Test
    public void testTPCH17() {
        runFileUnitTest("tpchcost/q17");
    }

    @Test
    public void testTPCH18() {
        runFileUnitTest("tpchcost/q18");
    }

    @Test
    public void testTPCH19() {
        runFileUnitTest("tpchcost/q19");
    }

    @Test
    public void testTPCH20() {
        runFileUnitTest("tpchcost/q20");
    }

    @Test
    public void testTPCH21() {
        runFileUnitTest("tpchcost/q21");
    }

    @Test
    public void testTPCH22() {
        runFileUnitTest("tpchcost/q22");
    }
}
