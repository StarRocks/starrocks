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
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.MockHistogramStatisticStorage;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TPCHPlanWithHistogramCostTest extends DistributedEnvPlanTestBase {

    @BeforeClass
=======
import com.google.common.collect.Lists;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.planner.TpchSQL;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.MockHistogramStatisticStorage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class TPCHPlanWithHistogramCostTest extends DistributedEnvPlanTestBase {

    @BeforeAll
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public static void beforeClass() throws Exception {
        DistributedEnvPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        FeConstants.showScanNodeLocalShuffleColumnsInExplain = false;
        connectContext.getSessionVariable().setEnableGlobalRuntimeFilter(true);

        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        int scale = 100;
        connectContext.getGlobalStateMgr().setStatisticStorage(new MockHistogramStatisticStorage(scale));
        connectContext.getSessionVariable().setEnableStatsToOptimizeSkewJoin(true);

<<<<<<< HEAD
        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("region");
        setTableStatistics(t0, 5);

        OlapTable t5 = (OlapTable) globalStateMgr.getDb("test").getTable("nation");
        setTableStatistics(t5, 25);

        OlapTable t1 = (OlapTable) globalStateMgr.getDb("test").getTable("supplier");
        setTableStatistics(t1, 10000 * scale);

        OlapTable t4 = (OlapTable) globalStateMgr.getDb("test").getTable("customer");
        setTableStatistics(t4, 150000 * scale);

        OlapTable t6 = (OlapTable) globalStateMgr.getDb("test").getTable("part");
        setTableStatistics(t6, 200000 * scale);

        OlapTable t2 = (OlapTable) globalStateMgr.getDb("test").getTable("partsupp");
        setTableStatistics(t2, 800000 * scale);

        OlapTable t3 = (OlapTable) globalStateMgr.getDb("test").getTable("orders");
        setTableStatistics(t3, 1500000 * scale);

        OlapTable t7 = (OlapTable) globalStateMgr.getDb("test").getTable("lineitem");
        setTableStatistics(t7, 6000000 * scale);
    }

    @AfterClass
=======
        OlapTable t0 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("region");
        setTableStatistics(t0, 5);

        OlapTable t5 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("nation");
        setTableStatistics(t5, 25);

        OlapTable t1 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("supplier");
        setTableStatistics(t1, 10000 * scale);

        OlapTable t4 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("customer");
        setTableStatistics(t4, 150000 * scale);

        OlapTable t6 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("part");
        setTableStatistics(t6, 200000 * scale);

        OlapTable t2 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("partsupp");
        setTableStatistics(t2, 800000 * scale);

        OlapTable t3 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("orders");
        setTableStatistics(t3, 1500000 * scale);

        OlapTable t7 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("lineitem");
        setTableStatistics(t7, 6000000 * scale);
    }

    @AfterAll
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public static void afterClass() {
        FeConstants.showScanNodeLocalShuffleColumnsInExplain = true;
    }

<<<<<<< HEAD
    @Test
    public void testTPCH1() {
        runFileUnitTest("tpch-histogram-cost/q1");
    }

    @Test
    public void testTPCH2() {
        runFileUnitTest("tpch-histogram-cost/q2");
    }

    @Test
    public void testTPCH3() {
        runFileUnitTest("tpch-histogram-cost/q3");
    }

    @Test
    public void testTPCH4() {
        runFileUnitTest("tpch-histogram-cost/q4");
    }

    @Test
    public void testTPCH5() {
        runFileUnitTest("tpch-histogram-cost/q5");
    }

    @Test
    public void testTPCH6() {
        runFileUnitTest("tpch-histogram-cost/q6");
    }

    @Test
    public void testTPCH7() {
        runFileUnitTest("tpch-histogram-cost/q7");
    }

    @Test
    public void testTPCH8() {
        runFileUnitTest("tpch-histogram-cost/q8");
    }

    @Test
    public void testTPCH9() {
        runFileUnitTest("tpch-histogram-cost/q9");
    }

    @Test
    public void testTPCH10() {
        runFileUnitTest("tpch-histogram-cost/q10");
    }

    @Test
    public void testTPCH11() {
        runFileUnitTest("tpch-histogram-cost/q11");
    }

    @Test
    public void testTPCH12() {
        runFileUnitTest("tpch-histogram-cost/q12");
    }

    @Test
    public void testTPCH13() {
        runFileUnitTest("tpch-histogram-cost/q13");
    }

    @Test
    public void testTPCH14() {
        runFileUnitTest("tpch-histogram-cost/q14");
    }

    @Test
    public void testTPCH15() {
        runFileUnitTest("tpch-histogram-cost/q15");
    }

    @Test
    public void testTPCH16() {
        runFileUnitTest("tpch-histogram-cost/q16");
    }

    @Test
    public void testTPCH17() {
        runFileUnitTest("tpch-histogram-cost/q17");
    }

    @Test
    public void testTPCH18() {
        runFileUnitTest("tpch-histogram-cost/q18");
    }

    @Test
    public void testTPCH19() {
        runFileUnitTest("tpch-histogram-cost/q19");
    }

    @Test
    public void testTPCH20() {
        runFileUnitTest("tpch-histogram-cost/q20");
    }

    @Test
    public void testTPCH21() {
        runFileUnitTest("tpch-histogram-cost/q21");
    }

    @Test
    public void testTPCH22() {
        runFileUnitTest("tpch-histogram-cost/q22");
    }
=======
    @ParameterizedTest(name = "Tpch.{0}")
    @MethodSource("tpchSource")
    public void testTPCH(String name, String sql, String resultFile) {
        runFileUnitTest(sql, resultFile);
    }

    private static Stream<Arguments> tpchSource() {
        List<Arguments> cases = Lists.newArrayList();
        for (Map.Entry<String, String> entry : TpchSQL.getAllSQL().entrySet()) {
            cases.add(Arguments.of(entry.getKey(), entry.getValue(), "tpch-histogram-cost/" + entry.getKey()));
        }
        return cases.stream();
    }

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
}
