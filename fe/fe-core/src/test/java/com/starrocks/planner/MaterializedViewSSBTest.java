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

public class MaterializedViewSSBTest extends PlanTestBase {
    private static final String MATERIALIZED_DB_NAME = "test_mv";

    @BeforeClass
    public static void setUp() throws Exception {
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
        starRocksAssert.withDatabase(MATERIALIZED_DB_NAME)
                .useDatabase(MATERIALIZED_DB_NAME);

        // create SSB tables
        createTables("sql/ssb/", Lists.newArrayList("lineorder", "customer", "dates", "supplier", "part"));

        // create lineorder_flat_mv
        createMaterializedViews("sql/materialized-view/ssb/", Lists.newArrayList("lineorder_flat_mv"));
    }
    @Ignore
    @Test
    public void testQuery1_1() {
        runFileUnitTest("materialized-view/ssb/q1-1");
    }

    @Ignore
    @Test
    public void testQuery1_2() {
        runFileUnitTest("materialized-view/ssb/q1-2");
    }

    @Ignore
    @Test
    public void testQuery1_3() {
        runFileUnitTest("materialized-view/ssb/q1-3");
    }

    @Ignore
    @Test
    public void testQuery2_1() {
        runFileUnitTest("materialized-view/ssb/q2-1");
    }

    @Ignore
    @Test
    public void testQuery2_2() {
        runFileUnitTest("materialized-view/ssb/q2-2");
    }

    @Ignore
    @Test
    public void testQuery2_3() {
        runFileUnitTest("materialized-view/ssb/q2-3");
    }

    @Ignore
    @Test
    public void testQuery3_1() {
        runFileUnitTest("materialized-view/ssb/q3-1");
    }

    @Ignore
    @Test
    public void testQuery3_2() {
        runFileUnitTest("materialized-view/ssb/q3-2");
    }

    @Ignore
    @Test
    public void testQuery3_3() {
        runFileUnitTest("materialized-view/ssb/q3-3");
    }

    @Ignore
    @Test
    public void testQuery3_4() {
        runFileUnitTest("materialized-view/ssb/q3-4");
    }

    @Test
    public void testQuery4_1() {
        runFileUnitTest("materialized-view/ssb/q4-1");
    }

    @Test
    public void testQuery4_2() {
        runFileUnitTest("materialized-view/ssb/q4-2");
    }

    @Test
    public void testQuery4_3() {
        runFileUnitTest("materialized-view/ssb/q4-3");
    }
}
