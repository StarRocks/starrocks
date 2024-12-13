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

<<<<<<< HEAD
import com.starrocks.common.FeConstants;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TPCHPlanWithCostTest extends DistributedEnvPlanTestBase {

    @BeforeClass
=======
import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.planner.TpchSQL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class TPCHPlanWithCostTest extends DistributedEnvPlanTestBase {

    @BeforeAll
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public static void beforeClass() throws Exception {
        DistributedEnvPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        FeConstants.showScanNodeLocalShuffleColumnsInExplain = false;
        connectContext.getSessionVariable().setEnableGlobalRuntimeFilter(true);
        connectContext.getSessionVariable().setEnableMultiColumnsOnGlobbalRuntimeFilter(true);
        connectContext.getSessionVariable().setEnableQueryDump(true);
<<<<<<< HEAD
    }

    @AfterClass
=======
        connectContext.getSessionVariable().setEnableStatsToOptimizeSkewJoin(true);
    }

    @AfterAll
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public static void afterClass() {
        FeConstants.showScanNodeLocalShuffleColumnsInExplain = true;
    }

<<<<<<< HEAD
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
=======
    @ParameterizedTest(name = "Tpch.{0}")
    @MethodSource("tpchSource")
    public void testTPCH(String name, String sql, String resultFile) {
        runFileUnitTest(sql, resultFile);
    }

    private static Stream<Arguments> tpchSource() {
        List<Arguments> cases = Lists.newArrayList();
        for (Map.Entry<String, String> entry : TpchSQL.getAllSQL().entrySet()) {
            cases.add(Arguments.of(entry.getKey(), entry.getValue(), "tpchcost/" + entry.getKey()));
        }
        return cases.stream();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }
}
