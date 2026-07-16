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

import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.RunMode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

// Covers the FE-side per-scan decision that turns on the lake prepared-physical-split scan
// (PlanFragmentBuilder.visitPhysicalOlapScan) plus the EXPLAIN marker it drives. The decision
// is gated on: the (INVISIBLE) session switch, query cache being off (mutual exclusion by
// design), and a cloud-native (lake) table. All tables here are cloud-native because the
// cluster runs in SHARED_DATA mode.
public class LakePreparedPhysicalSplitScanPlanTest {

    private static final String MARKER = "Prepared Physical Split Scan: true";

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test_prepared_split").useDatabase("test_prepared_split");
        starRocksAssert.withTable("CREATE TABLE lake_t0 (\n" +
                "  k1 bigint,\n" +
                "  k2 bigint,\n" +
                "  k3 bigint\n" +
                ")\n" +
                "DUPLICATE KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 3");
    }

    @AfterEach
    public void resetSessionVariable() {
        connectContext.getSessionVariable().setEnableLakePreparedPhysicalSplitScan(false);
        connectContext.getSessionVariable().setEnableQueryCache(false);
    }

    private String getFragmentPlan(String sql) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        return UtFrameUtils.getPlanAndFragment(connectContext, sql).second
                .getExplainString(TExplainLevel.NORMAL);
    }

    // Flag on + query cache off + lake table -> the decision fires and the marker appears.
    @Test
    public void testEnabledForLakeTableWhenFlagOn() throws Exception {
        connectContext.getSessionVariable().setEnableLakePreparedPhysicalSplitScan(true);
        connectContext.getSessionVariable().setEnableQueryCache(false);
        String plan = getFragmentPlan("SELECT k1 FROM lake_t0");
        Assertions.assertTrue(plan.contains(MARKER), plan);
    }

    // The switch defaults to off (INVISIBLE), so a plain query must not enable the scan.
    @Test
    public void testDisabledByDefault() throws Exception {
        String plan = getFragmentPlan("SELECT k1 FROM lake_t0");
        Assertions.assertFalse(plan.contains(MARKER), plan);
    }

    // Query cache is mutually exclusive with the optimization by design: even with the switch
    // on, enabling query cache must fall back to the non-split scan (no marker).
    @Test
    public void testDisabledWhenQueryCacheOn() throws Exception {
        connectContext.getSessionVariable().setEnableLakePreparedPhysicalSplitScan(true);
        connectContext.getSessionVariable().setEnableQueryCache(true);
        String plan = getFragmentPlan("SELECT k1 FROM lake_t0");
        Assertions.assertFalse(plan.contains(MARKER), plan);
    }
}
