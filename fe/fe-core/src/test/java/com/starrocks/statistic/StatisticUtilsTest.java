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
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.system.SystemInfoService;
import com.starrocks.type.ScalarType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

class StatisticUtilsTest extends PlanTestBase {

    private static void assertVarcharLength(List<ColumnDef> columnDefs, String columnName, int expectedLength) {
        ColumnDef columnDef = columnDefs.stream()
                .filter(column -> columnName.equals(column.getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("missing statistics column: " + columnName));
        Assertions.assertInstanceOf(ScalarType.class, columnDef.getType());
        Assertions.assertEquals(expectedLength, ((ScalarType) columnDef.getType()).getLength(), columnName);
    }

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
    void buildStatsColumnDefUsesConfiguredOlapVarcharLengthOnlyForPayloadColumns() {
        int previousMaxVarcharLength = Config.max_varchar_length;
        int configuredMaxVarcharLength = 1_234_567;
        try {
            Config.max_varchar_length = configuredMaxVarcharLength;

            List<ColumnDef> sampleColumns =
                    StatisticUtils.buildStatsColumnDef(StatsConstants.SAMPLE_STATISTICS_TABLE_NAME);
            assertVarcharLength(sampleColumns, "max", configuredMaxVarcharLength);
            assertVarcharLength(sampleColumns, "min", configuredMaxVarcharLength);
            assertVarcharLength(sampleColumns, "column_name", 65530);
            assertVarcharLength(sampleColumns, "table_name", 65530);
            assertVarcharLength(sampleColumns, "db_name", 65530);

            List<ColumnDef> fullColumns =
                    StatisticUtils.buildStatsColumnDef(StatsConstants.FULL_STATISTICS_TABLE_NAME);
            assertVarcharLength(fullColumns, "max", configuredMaxVarcharLength);
            assertVarcharLength(fullColumns, "min", configuredMaxVarcharLength);
            assertVarcharLength(fullColumns, "partition_name", 65530);

            List<ColumnDef> externalFullColumns =
                    StatisticUtils.buildStatsColumnDef(StatsConstants.EXTERNAL_FULL_STATISTICS_TABLE_NAME);
            assertVarcharLength(externalFullColumns, "max", configuredMaxVarcharLength);
            assertVarcharLength(externalFullColumns, "min", configuredMaxVarcharLength);
            assertVarcharLength(externalFullColumns, "table_uuid", 65530);
            assertVarcharLength(externalFullColumns, "catalog_name", 65530);

            List<ColumnDef> histogramColumns =
                    StatisticUtils.buildStatsColumnDef(StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME);
            assertVarcharLength(histogramColumns, "buckets", configuredMaxVarcharLength);
            assertVarcharLength(histogramColumns, "mcv", configuredMaxVarcharLength);
            assertVarcharLength(histogramColumns, "column_name", 65530);

            List<ColumnDef> externalHistogramColumns =
                    StatisticUtils.buildStatsColumnDef(StatsConstants.EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME);
            assertVarcharLength(externalHistogramColumns, "buckets", configuredMaxVarcharLength);
            assertVarcharLength(externalHistogramColumns, "mcv", configuredMaxVarcharLength);
            assertVarcharLength(externalHistogramColumns, "table_uuid", 65530);

            List<ColumnDef> multiColumnColumns =
                    StatisticUtils.buildStatsColumnDef(StatsConstants.MULTI_COLUMN_STATISTICS_TABLE_NAME);
            assertVarcharLength(multiColumnColumns, "column_ids", 65530);
            assertVarcharLength(multiColumnColumns, "column_names", 65530);
        } finally {
            Config.max_varchar_length = previousMaxVarcharLength;
        }
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
    void hashTableUuidForPkStorageIsDeterministicAndFixedLength() {
        String tableUuid = "iceberg.udp_abx_etl_db1_datawarehouse.tenant.account_buying_group." +
                "d6cfa1ed-0000-0000-0000-000000000000";
        String hash1 = StatisticUtils.hashTableUuidForPkStorage(tableUuid);
        String hash2 = StatisticUtils.hashTableUuidForPkStorage(tableUuid);
        Assertions.assertEquals(hash1, hash2, "hash must be deterministic for the same input");
        Assertions.assertEquals(32, hash1.length(), "murmur3_128 hex-encoded hash must be 32 chars");
        Assertions.assertTrue(hash1.length() < tableUuid.length(),
                "hash must be shorter than a realistic long Iceberg table_uuid");

        String otherTableUuid = "iceberg.udp_abx_etl_db1_datawarehouse.tenant.buying_group_member." +
                "d6cfa1ed-0000-0000-0000-000000000000";
        Assertions.assertNotEquals(hash1, StatisticUtils.hashTableUuidForPkStorage(otherTableUuid),
                "different table_uuid values must not collide for realistic inputs");
    }
}
