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

package com.starrocks.statistic;

import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.system.SystemInfoService;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class StatisticUtilsTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        UtFrameUtils.createMinStarRocksCluster();
        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }
        UtFrameUtils.addMockBackend(123);
        UtFrameUtils.addMockBackend(124);
    }

    @Test
    void alterSystemTableReplicationNumIfNecessary() {
        // 1. Has sufficient backends
        new MockUp<SystemInfoService>() {
            @Mock
            public int getRetainedBackendNumber() {
                return 100;
            }
        };
        final String tableName = "column_statistics";
        Assertions.assertTrue(StatisticUtils.alterSystemTableReplicationNumIfNecessary(tableName));
        Assertions.assertFalse(StatisticUtils.alterSystemTableReplicationNumIfNecessary(tableName));
        Assertions.assertEquals("3",
                starRocksAssert.getTable(StatsConstants.STATISTICS_DB_NAME, tableName).getProperties().get(
                        "replication_num"));

        // 2. change default_replication_num
        Config.default_replication_num = 1;
        Assertions.assertTrue(StatisticUtils.alterSystemTableReplicationNumIfNecessary(tableName));
        Assertions.assertFalse(StatisticUtils.alterSystemTableReplicationNumIfNecessary(tableName));
        Assertions.assertEquals("1",
                starRocksAssert.getTable(StatsConstants.STATISTICS_DB_NAME, tableName).getProperties().get(
                        "replication_num"));
        Config.default_replication_num = 3;
        Assertions.assertTrue(StatisticUtils.alterSystemTableReplicationNumIfNecessary(tableName));

        // 3. Has no sufficient backends
        new MockUp<SystemInfoService>() {
            @Mock
            public int getRetainedBackendNumber() {
                return 1;
            }
        };
        Assertions.assertTrue(StatisticUtils.alterSystemTableReplicationNumIfNecessary(tableName));
        Assertions.assertFalse(StatisticUtils.alterSystemTableReplicationNumIfNecessary(tableName));
        Assertions.assertEquals("1",
                starRocksAssert.getTable(StatsConstants.STATISTICS_DB_NAME, tableName).getProperties().get(
                        "replication_num"));
    }

    @Test
    void buildConnectContextHonorsOverridesUnderGlobalEnableProfile() throws Exception {
        // Reproduces the bug where SET GLOBAL enable_profile=true leaked into background
        // syncer queries because StatisticUtils.buildConnectContext applied overrides BEFORE
        // setCurrentWarehouse() replaced the entire sessionVariable with a fresh clone of
        // defaultSessionVariable.
        SessionVariable globalDefault = com.starrocks.server.GlobalStateMgr.getCurrentState()
                .getVariableMgr().getDefaultSessionVariable();
        boolean savedEnableProfile = globalDefault.isEnableProfile();
        long savedBigQueryThresholdMs = globalDefault.getBigQueryProfileMilliSecondThreshold();
        try {
            globalDefault.setEnableProfile(true);
            globalDefault.setBigQueryProfileThreshold("30s");

            ConnectContext ctx = StatisticUtils.buildConnectContext();
            Assertions.assertFalse(ctx.getSessionVariable().isEnableProfile(),
                    "enable_profile must remain disabled for statistics-infrastructure context");
            Assertions.assertFalse(ctx.getSessionVariable().isEnableLoadProfile());
            Assertions.assertFalse(ctx.getSessionVariable().isEnableBigQueryProfile(),
                    "big_query_profile_threshold must be 0s for statistics-infrastructure context");
            // Other StatisticUtils overrides should also survive the warehouse switch.
            Assertions.assertEquals(1, ctx.getSessionVariable().getParallelExecInstanceNum());
            // isStatisticsContext is reflected through isStatisticsJob().
            Assertions.assertTrue(ctx.isStatisticsJob(),
                    "buildConnectContext must mark the context as statistics infrastructure");
            // MV rewrite is intentionally disabled for inner queries; this must survive
            // the warehouse switch that setCurrentWarehouse performs internally.
            Assertions.assertFalse(ctx.getSessionVariable().isEnableMaterializedViewRewrite());
        } finally {
            globalDefault.setEnableProfile(savedEnableProfile);
            globalDefault.setBigQueryProfileThreshold(savedBigQueryThresholdMs + "ms");
        }
    }

    @Test
    void hashExternalPartitionName() {
        String longPartitionName = "tenant_id=hash-2c933fdc";
        String hashed = StatisticUtils.hashExternalPartitionName(longPartitionName);

        Assertions.assertTrue(hashed.startsWith(StatsConstants.EXTERNAL_PARTITION_NAME_HASH_PREFIX));
        // prefix + 16 fixed hex chars, regardless of input length
        Assertions.assertEquals(StatsConstants.EXTERNAL_PARTITION_NAME_HASH_PREFIX.length() + 16, hashed.length());
        // deterministic
        Assertions.assertEquals(hashed, StatisticUtils.hashExternalPartitionName(longPartitionName));
        // different input -> different hash
        Assertions.assertNotEquals(hashed, StatisticUtils.hashExternalPartitionName("tenant_id=hash-deadbeef"));
        // fixed length even for a very long, arbitrarily-formatted partition path
        String veryLongPartitionName = "tenant_id=" + "a".repeat(200);
        Assertions.assertEquals(hashed.length(), StatisticUtils.hashExternalPartitionName(veryLongPartitionName).length());
    }
}