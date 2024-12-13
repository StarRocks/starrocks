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

package com.starrocks.connector.parser.trino;

<<<<<<< HEAD
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.MockTpchStatisticStorage;
import org.junit.BeforeClass;
import org.junit.Test;

public class TrinoTPCHTest extends TrinoTestBase {
    @BeforeClass
=======
import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.planner.TpchSQL;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.MockTpchStatisticStorage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class TrinoTPCHTest extends TrinoTestBase {
    @BeforeAll
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public static void beforeClass() throws Exception {
        TrinoTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        connectContext.getGlobalStateMgr().setStatisticStorage(new MockTpchStatisticStorage(connectContext, 1));
<<<<<<< HEAD
        GlobalStateMgr.getCurrentAnalyzeMgr().getBasicStatsMetaMap().clear();
=======
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().getBasicStatsMetaMap().clear();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        connectContext.getSessionVariable().setMaxTransformReorderJoins(8);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000);
        connectContext.getSessionVariable().setEnableLocalShuffleAgg(false);
    }

<<<<<<< HEAD
    @Test
    public void testTPCH1() {
        runFileUnitTest("tpch/q1");
    }

    @Test
    public void testTPCH2() {
        runFileUnitTest("tpch/q2");
    }

    @Test
    public void testTPCH3() {
        runFileUnitTest("tpch/q3");
    }

    @Test
    public void testTPCH4() {
        runFileUnitTest("tpch/q4");
=======
    @ParameterizedTest(name = "Tpch.{0}")
    @MethodSource("tpchSource")
    public void testTPCH(String name, String sql, String resultFile) {
        runFileUnitTest(sql, resultFile);
    }

    private static Stream<Arguments> tpchSource() {
        List<Arguments> cases = Lists.newArrayList();
        for (Map.Entry<String, String> entry : TpchSQL.getAllSQL().entrySet()) {
            cases.add(Arguments.of(entry.getKey(), entry.getValue(), "tpch/" + entry.getKey()));
        }
        return cases.stream();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Test
    public void testTPCH4_2() {
        runFileUnitTest("tpch/q4-2");
    }

    @Test
<<<<<<< HEAD
    public void testTPCH5() {
        runFileUnitTest("tpch/q5");
    }

    @Test
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public void testTPCH5_2() {
        runFileUnitTest("tpch/q5-2");
    }

    @Test
<<<<<<< HEAD
    public void testTPCH6() {
        runFileUnitTest("tpch/q6");
    }

    @Test
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public void testTPCH6_2() {
        runFileUnitTest("tpch/q6-2");
    }

    @Test
<<<<<<< HEAD
    public void testTPCH7() {
        runFileUnitTest("tpch/q7");
    }

    @Test
    public void testTPCH8() {
        runFileUnitTest("tpch/q8");
    }

    @Test
    public void testTPCH9() {
        runFileUnitTest("tpch/q9");
    }

    @Test
    public void testTPCH10() {
        runFileUnitTest("tpch/q10");
    }

    @Test
    public void testTPCH11() {
        runFileUnitTest("tpch/q11");
    }

    @Test
    public void testTPCH12() {
        runFileUnitTest("tpch/q12");
    }

    @Test
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public void testTPCH12_2() {
        runFileUnitTest("tpch/q12-2");
    }

    @Test
<<<<<<< HEAD
    public void testTPCH13() {
        runFileUnitTest("tpch/q13");
    }

    @Test
    public void testTPCH14() {
        runFileUnitTest("tpch/q14");
    }

    @Test
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public void testTPCH14_2() {
        runFileUnitTest("tpch/q14-2");
    }

    @Test
<<<<<<< HEAD
    public void testTPCH15() {
        runFileUnitTest("tpch/q15");
    }

    @Test
    public void testTPCH16() {
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
        runFileUnitTest("tpch/q16");
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
    }

    @Test
    public void testTPCH17() {
        runFileUnitTest("tpch/q17");
    }

    @Test
    public void testTPCH18() {
        runFileUnitTest("tpch/q18");
    }

    @Test
    public void testTPCH19() {
        runFileUnitTest("tpch/q19");
    }

    @Test
    public void testTPCH20() {
        runFileUnitTest("tpch/q20");
    }

    @Test
    public void testTPCH20_2() {
        runFileUnitTest("tpch/q20-2");
    }

    @Test
    public void testTPCH21() {
        runFileUnitTest("tpch/q21");
    }

    @Test
    public void testTPCH22() {
        runFileUnitTest("tpch/q22");
    }
=======
    public void testTPCH20_2() {
        runFileUnitTest("tpch/q20-2");
    }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
}
