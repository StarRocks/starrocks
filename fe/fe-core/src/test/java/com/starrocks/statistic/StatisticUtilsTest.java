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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.system.SystemInfoService;
import com.starrocks.type.BooleanType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;

class StatisticUtilsTest extends PlanTestBase {

    private static StructType namedStruct(StructField... fields) {
        return new StructType(Arrays.asList(fields), true);
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
    void testGetQueryStatisticsColumnTypeSimple() {
        Table table = Mockito.mock(Table.class);
        Column col = Mockito.mock(Column.class);
        Mockito.when(col.getType()).thenReturn(IntegerType.INT);
        Mockito.when(table.getColumn("id")).thenReturn(col);

        Type result = StatisticUtils.getQueryStatisticsColumnType(table, "id");
        Assertions.assertEquals(IntegerType.INT, result);
    }

    @Test
    void testGetQueryStatisticsColumnTypeWithDotInName() {
        Table table = Mockito.mock(Table.class);
        Column col = Mockito.mock(Column.class);
        Mockito.when(col.getType()).thenReturn(BooleanType.BOOLEAN);
        Mockito.when(table.getColumn("customer.is_verified_email")).thenReturn(col);
        Mockito.when(table.getColumn("customer")).thenReturn(null);

        Type result = StatisticUtils.getQueryStatisticsColumnType(table, "customer.is_verified_email");
        Assertions.assertEquals(BooleanType.BOOLEAN, result);
    }

    @Test
    void testGetQueryStatisticsColumnTypeStructField() {
        Table table = Mockito.mock(Table.class);
        StructType structType = namedStruct(
                new StructField("city", VarcharType.VARCHAR)
        );
        Column structCol = Mockito.mock(Column.class);
        Mockito.when(structCol.getType()).thenReturn(structType);
        Mockito.when(table.getColumn("address.city")).thenReturn(null);
        Mockito.when(table.getColumn("address")).thenReturn(structCol);

        Type result = StatisticUtils.getQueryStatisticsColumnType(table, "address.city");
        Assertions.assertEquals(VarcharType.VARCHAR, result);
    }

    @Test
    void testGetQueryStatisticsColumnTypeLiteralWinsOverStruct() {
        Table table = Mockito.mock(Table.class);
        Column literalCol = Mockito.mock(Column.class);
        Mockito.when(literalCol.getType()).thenReturn(BooleanType.BOOLEAN);

        StructType structType = namedStruct(
                new StructField("b", VarcharType.VARCHAR)
        );
        Column structCol = Mockito.mock(Column.class);
        Mockito.when(structCol.getType()).thenReturn(structType);

        Mockito.when(table.getColumn("a.b")).thenReturn(literalCol);
        Mockito.when(table.getColumn("a")).thenReturn(structCol);

        Type result = StatisticUtils.getQueryStatisticsColumnType(table, "a.b");
        Assertions.assertEquals(BooleanType.BOOLEAN, result);
    }

    @Test
    void testGetQueryStatisticsColumnTypeDottedStructColumn() {
        Table table = Mockito.mock(Table.class);
        StructType structType = namedStruct(
                new StructField("city", VarcharType.VARCHAR)
        );
        Column structCol = Mockito.mock(Column.class);
        Mockito.when(structCol.getType()).thenReturn(structType);
        Mockito.when(table.getColumn("customer.profile.city")).thenReturn(null);
        Mockito.when(table.getColumn("customer")).thenReturn(null);
        Mockito.when(table.getColumn("customer.profile")).thenReturn(structCol);

        Type result = StatisticUtils.getQueryStatisticsColumnType(table, "customer.profile.city");
        Assertions.assertEquals(VarcharType.VARCHAR, result);
    }

    @Test
    void testGetQueryStatisticsColumnTypeDottedNestedStructColumn() {
        Table table = Mockito.mock(Table.class);
        StructType innerStruct = namedStruct(
                new StructField("zip", VarcharType.VARCHAR)
        );
        StructType outerStruct = namedStruct(
                new StructField("address", innerStruct)
        );
        Column structCol = Mockito.mock(Column.class);
        Mockito.when(structCol.getType()).thenReturn(outerStruct);
        Mockito.when(table.getColumn("customer.profile.address.zip")).thenReturn(null);
        Mockito.when(table.getColumn("customer")).thenReturn(null);
        Mockito.when(table.getColumn("customer.profile")).thenReturn(structCol);
        Mockito.when(table.getColumn("customer.profile.address")).thenReturn(null);

        Type result = StatisticUtils.getQueryStatisticsColumnType(table, "customer.profile.address.zip");
        Assertions.assertEquals(VarcharType.VARCHAR, result);
    }

    @Test
    void testGetQueryStatisticsColumnTypeAmbiguousPrefixLongestWins() {
        // Both "customer" and "customer.profile" exist as top-level columns and
        // both could resolve "customer.profile.city". The longest base column
        // ("customer.profile") must win so we return its field type, not the type
        // reached by traversing the shorter "customer" struct.
        Table table = Mockito.mock(Table.class);

        StructType shortBaseStruct = namedStruct(
                new StructField("profile", namedStruct(
                        new StructField("city", IntegerType.INT)
                ))
        );
        Column shortBaseCol = Mockito.mock(Column.class);
        Mockito.when(shortBaseCol.getType()).thenReturn(shortBaseStruct);

        StructType longBaseStruct = namedStruct(
                new StructField("city", VarcharType.VARCHAR)
        );
        Column longBaseCol = Mockito.mock(Column.class);
        Mockito.when(longBaseCol.getType()).thenReturn(longBaseStruct);

        Mockito.when(table.getColumn("customer.profile.city")).thenReturn(null);
        Mockito.when(table.getColumn("customer.profile")).thenReturn(longBaseCol);
        Mockito.when(table.getColumn("customer")).thenReturn(shortBaseCol);

        Type result = StatisticUtils.getQueryStatisticsColumnType(table, "customer.profile.city");
        Assertions.assertEquals(VarcharType.VARCHAR, result);
    }

    @Test
    void testGetQueryStatisticsColumnTypeFallbackToShorterPrefix() {
        // The longest prefix "customer.profile" exists but cannot resolve the
        // remaining "city" subfield, so resolution must fall back to the shorter
        // base column "customer" which does resolve "profile.city".
        Table table = Mockito.mock(Table.class);

        StructType customerStruct = namedStruct(
                new StructField("profile", namedStruct(
                        new StructField("city", VarcharType.VARCHAR)
                ))
        );
        Column customerCol = Mockito.mock(Column.class);
        Mockito.when(customerCol.getType()).thenReturn(customerStruct);

        // "customer.profile" exists but is a scalar, so "city" cannot resolve.
        Column scalarProfileCol = Mockito.mock(Column.class);
        Mockito.when(scalarProfileCol.getType()).thenReturn(IntegerType.INT);

        Mockito.when(table.getColumn("customer.profile.city")).thenReturn(null);
        Mockito.when(table.getColumn("customer.profile")).thenReturn(scalarProfileCol);
        Mockito.when(table.getColumn("customer")).thenReturn(customerCol);

        Type result = StatisticUtils.getQueryStatisticsColumnType(table, "customer.profile.city");
        Assertions.assertEquals(VarcharType.VARCHAR, result);
    }

    @Test
    void testGetQueryStatisticsColumnTypeMissingSubfieldThrows() {
        // Base column "address" resolves as a struct, but the requested subfield
        // "zipcode" does not exist. Should report a SemanticException rather than
        // throwing a NullPointerException.
        Table table = Mockito.mock(Table.class);
        StructType structType = namedStruct(
                new StructField("city", VarcharType.VARCHAR)
        );
        Column structCol = Mockito.mock(Column.class);
        Mockito.when(structCol.getType()).thenReturn(structType);
        Mockito.when(table.getColumn("address.zipcode")).thenReturn(null);
        Mockito.when(table.getColumn("address")).thenReturn(structCol);
        Mockito.when(table.getName()).thenReturn("test_table");

        Assertions.assertThrows(SemanticException.class,
                () -> StatisticUtils.getQueryStatisticsColumnType(table, "address.zipcode"));
    }

    @Test
    void testGetQueryStatisticsColumnTypeNotFound() {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.getColumn(Mockito.anyString())).thenReturn(null);
        Mockito.when(table.getName()).thenReturn("test_table");

        Assertions.assertThrows(SemanticException.class,
                () -> StatisticUtils.getQueryStatisticsColumnType(table, "nonexistent"));
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
}