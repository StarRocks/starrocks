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
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class CacheStatsScanTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test_cache_stats").useDatabase("test_cache_stats");

        starRocksAssert.withTable("CREATE TABLE lake_t0 (\n" +
                "  k1 bigint,\n" +
                "  k2 bigint,\n" +
                "  k3 bigint\n" +
                ")\n" +
                "DUPLICATE KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 3");

        starRocksAssert.withTable("CREATE TABLE lake_part_t0 (\n" +
                "  k1 date,\n" +
                "  k2 bigint,\n" +
                "  k3 bigint\n" +
                ")\n" +
                "PARTITION BY RANGE(k1) (\n" +
                "  PARTITION p1 VALUES [('2024-01-01'), ('2024-02-01')),\n" +
                "  PARTITION p2 VALUES [('2024-02-01'), ('2024-03-01'))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3");
    }

    private String getFragmentPlan(String sql) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        return UtFrameUtils.getPlanAndFragment(connectContext, sql).second
                .getExplainString(TExplainLevel.NORMAL);
    }

    private ExecPlan getExecPlan(String sql) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        return UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
    }

    @Test
    public void testCacheStatsScanPlan() throws Exception {
        String sql = "SELECT * FROM lake_t0 [_CACHE_STATS_]";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("CacheStatsScan"), plan);
        Assertions.assertTrue(plan.contains("Table: lake_t0"), plan);
        Assertions.assertTrue(plan.contains("CacheStats Query"), plan);
    }

    @Test
    public void testCacheStatsScanSelectColumns() throws Exception {
        String sql = "SELECT tablet_id, cached_bytes FROM lake_t0 [_CACHE_STATS_]";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("CacheStatsScan"), plan);
    }

    @Test
    public void testCacheStatsScanWithPartition() throws Exception {
        String sql = "SELECT * FROM lake_part_t0 PARTITION(p1) [_CACHE_STATS_]";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("CacheStatsScan"), plan);
        Assertions.assertTrue(plan.contains("Table: lake_part_t0"), plan);
    }

    @Test
    public void testCacheStatsScanWithFilter() throws Exception {
        String sql = "SELECT * FROM lake_t0 [_CACHE_STATS_] WHERE tablet_id > 0";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("CacheStatsScan"), plan);
    }

    @Test
    public void testCacheStatsScanWithAggregation() throws Exception {
        String sql = "SELECT sum(cached_bytes), sum(total_bytes) FROM lake_t0 [_CACHE_STATS_]";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("CacheStatsScan"), plan);
        Assertions.assertTrue(plan.contains("AGGREGATE"), plan);
    }

    @Test
    public void testCacheStatsScanExecPlanHasScanNode() throws Exception {
        String sql = "SELECT * FROM lake_t0 [_CACHE_STATS_]";
        ExecPlan execPlan = getExecPlan(sql);
        boolean hasCacheStatsScanNode = execPlan.getScanNodes().stream()
                .anyMatch(n -> n instanceof CacheStatsScanNode);
        Assertions.assertTrue(hasCacheStatsScanNode);
    }
}
