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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class MaterializedViewTPCHTest extends PlanTestBase {
    private static final String MATERIALIZED_DB_NAME = "test";

    @BeforeClass
    public static void setUp() throws Exception {
        PlanTestBase.beforeClass();

        FeConstants.runningUnitTest = true;
        Config.enable_experimental_mv = true;
        UtFrameUtils.createMinStarRocksCluster();

        connectContext= UtFrameUtils.createDefaultCtx();
        connectContext.getSessionVariable().setEnablePipelineEngine(true);
        connectContext.getSessionVariable().setEnableQueryCache(true);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(300000);
        connectContext.getSessionVariable().setEnableOptimizerTraceLog(true);
        FeConstants.runningUnitTest = true;
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.useDatabase(MATERIALIZED_DB_NAME);

        // create tpch relative mv
        createMaterializedViews("sql/materialized-view/tpch/", Lists.newArrayList("customer_order_mv",
                "customer_mv", "lineitem_mv", "v_partsupp_mv"));
    }

    @Ignore
    @Test
    public void testQuery1() {
        runFileUnitTest("materialized-view/tpch/q1");
    }


    @Ignore
    @Test
    public void testQuery2() {
        runFileUnitTest("materialized-view/tpch/q2");
    }


    @Ignore
    @Test
    public void testQuery3() {
        runFileUnitTest("materialized-view/tpch/q3");
    }


    @Ignore
    @Test
    public void testQuery4() {
        runFileUnitTest("materialized-view/tpch/q4");
    }


    @Ignore
    @Test
    public void testQuery5() {
        runFileUnitTest("materialized-view/tpch/q5");
    }


    @Ignore
    @Test
    public void testQuery6() {
        runFileUnitTest("materialized-view/tpch/q6");
    }


    @Ignore
    @Test
    public void testQuery7() {
        runFileUnitTest("materialized-view/tpch/q7");
    }


    @Ignore
    @Test
    public void testQuery8() {
        runFileUnitTest("materialized-view/tpch/q8");
    }


    @Ignore
    @Test
    public void testQuery9() {
        runFileUnitTest("materialized-view/tpch/q9");
    }


    @Ignore
    @Test
    public void testQuery10() {
        runFileUnitTest("materialized-view/tpch/q10");
    }


    @Ignore
    @Test
    public void testQuery11() {
        runFileUnitTest("materialized-view/tpch/q11");
    }


    @Ignore
    @Test
    public void testQuery12() {
        runFileUnitTest("materialized-view/tpch/q12");
    }


    @Test
    public void testQuery13() {
        runFileUnitTest("materialized-view/tpch/q13");
    }


    @Ignore
    @Test
    public void testQuery14() {
        runFileUnitTest("materialized-view/tpch/q14");
    }


    @Ignore
    @Test
    public void testQuery15() {
        runFileUnitTest("materialized-view/tpch/q15");
    }


    @Ignore
    @Test
    public void testQuery16() {
        runFileUnitTest("materialized-view/tpch/q16");
    }

    @Ignore
    @Test
    public void testQuery17() {
        runFileUnitTest("materialized-view/tpch/q17");
    }

    @Ignore
    @Test
    public void testQuery18() {
        runFileUnitTest("materialized-view/tpch/q18");
    }

    @Ignore
    @Test
    public void testQuery19() {
        runFileUnitTest("materialized-view/tpch/q19");
    }

    @Ignore
    @Test
    public void testQuery20() {
        runFileUnitTest("materialized-view/tpch/q20");
    }


    @Ignore
    @Test
    public void testQuery21() {
        runFileUnitTest("materialized-view/tpch/q21");
    }

    @Ignore
    @Test
    public void testQuery22() {
        runFileUnitTest("materialized-view/tpch/q22");
    }
}
