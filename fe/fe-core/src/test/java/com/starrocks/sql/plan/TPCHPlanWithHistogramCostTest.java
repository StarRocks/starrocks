// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.MockTPCHHistogramStatisticStorage;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TPCHPlanWithHistogramCostTest extends DistributedEnvPlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        DistributedEnvPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        FeConstants.showLocalShuffleColumnsInExplain = false;
        connectContext.getSessionVariable().setEnableGlobalRuntimeFilter(true);

        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        int scale = 100;
        connectContext.getGlobalStateMgr().setStatisticStorage(new MockTPCHHistogramStatisticStorage(scale));
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
    public static void afterClass() {
        FeConstants.showLocalShuffleColumnsInExplain = true;
    }

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
}
