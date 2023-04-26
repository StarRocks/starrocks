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

import com.google.common.collect.ImmutableMap;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.util.Map;

public class TPCDS1TTestBase extends TPCDSPlanTestBase {
    private static final Map<String, Long> ROW_COUNT_MAP = ImmutableMap.<String, Long>builder()
            .put("call_center", 42L)
            .put("catalog_page", 30000L)
            .put("catalog_returns", 143996756L)
            .put("catalog_sales", 1439980416L)
            .put("customer", 12000000L)
            .put("customer_address", 6000000L)
            .put("customer_demographics", 1920800L)
            .put("date_dim", 73049L)
            .put("date_dim_varchar", 73049L)
            .put("household_demographics", 7200L)
            .put("income_band", 20L)
            .put("inventory", 783000000L)
            .put("item", 300000L)
            .put("promotion", 1500L)
            .put("reason", 65L)
            .put("ship_mode", 20L)
            .put("store", 1002L)
            .put("store_returns", 287999764L)
            .put("store_sales", 2879987999L)
            .put("time_dim", 86400L)
            .put("warehouse", 20L)
            .put("web_page", 3000L)
            .put("web_returns", 71997522L)
            .put("web_sales", 720000376L)
            .put("web_site", 54L)
            .build();

    private StatisticStorage origin;

    @BeforeAll
    public static void beforeClass() throws Exception {
        TPCDSPlanTestBase.beforeClass();
        connectContext.getSessionVariable().setCboCteReuse(true);
        connectContext.getSessionVariable().setEnablePipelineEngine(true);
    }

    @AfterAll
    public static void afterClass() {
        TPCDSPlanTestBase.afterClass();
    }

    @BeforeEach
    public void setUp() throws Exception {
        origin = GlobalStateMgr.getCurrentStatisticStorage();
        connectContext.getGlobalStateMgr().setStatisticStorage(new MockTPCDSStatisticStorage());
        setTPCDSTableStats(ROW_COUNT_MAP);
        FeConstants.runningUnitTest = true;
    }

    @AfterEach
    public void tearDown() throws Exception {
        FeConstants.runningUnitTest = false;
        connectContext.getGlobalStateMgr().setStatisticStorage(origin);
    }
}
