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

package com.starrocks.statistic.columns;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

class PredicateColumnsMgrTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StatisticsMetaManager statistic = new StatisticsMetaManager();
        statistic.createStatisticsTablesForTest();
        TableKeeper keeper = ExternalPredicateColumnsStorage.createKeeper();
        keeper.run();
        FeConstants.runningUnitTest = true;
    }

    @BeforeEach
    public void before() {
        PredicateColumnsMgr.getInstance().reset();
    }

    private IcebergTable mockExternalTable(String uuid, String catalog, String db, String table) {
        IcebergTable t = Mockito.mock(IcebergTable.class);
        Mockito.when(t.getUUID()).thenReturn(uuid);
        Mockito.when(t.getCatalogName()).thenReturn(catalog);
        Mockito.when(t.getCatalogDBName()).thenReturn(db);
        Mockito.when(t.getCatalogTableName()).thenReturn(table);
        Mockito.when(t.getName()).thenReturn(table);
        Mockito.when(t.isNativeTableOrMaterializedView()).thenReturn(false);
        Mockito.when(t.isTemporaryTable()).thenReturn(false);
        return t;
    }

    @Test
    public void testExternalColumnRecordedAndQueryable() {
        IcebergTable table = mockExternalTable("iceberg_catalog.db1.t1.uuid-1", "iceberg_catalog", "db1", "t1");
        Column column = new Column("c1", IntegerType.INT);
        PredicateColumnsMgr mgr = PredicateColumnsMgr.getInstance();

        mgr.recordColumnUsageForTest(table, column, ColumnUsage.UseCase.PREDICATE);

        List<ExternalColumnUsage> result = mgr.queryExternalPredicateColumns(table);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("c1", result.get(0).getColumnName());
        Assertions.assertEquals("iceberg_catalog", result.get(0).getCatalogName());
    }

    @Test
    public void testNativeTableStillRecordsIntoInternalPath() {
        PredicateColumnsMgr mgr = PredicateColumnsMgr.getInstance();
        Table t0 = starRocksAssert.getTable(connectContext.getDatabase(), "t0");
        Column v1 = t0.getColumn("v1");

        mgr.recordColumnUsageForTest(t0, v1, ColumnUsage.UseCase.PREDICATE);

        TableName tableName = new TableName(connectContext.getDatabase(), "t0");
        List<ColumnUsage> result = mgr.queryPredicateColumns(tableName);
        Assertions.assertEquals(1, result.size());

        // and must not leak into the external map
        IcebergTable unrelated = mockExternalTable("catalog.db.other", "catalog", "db", "other");
        Assertions.assertTrue(mgr.queryExternalPredicateColumns(unrelated).isEmpty());
    }

    @Test
    public void testExternalCollectionDisabledByConfig() {
        boolean defaultValue = Config.enable_external_predicate_columns_collection;
        Config.enable_external_predicate_columns_collection = false;
        try {
            IcebergTable table = mockExternalTable("iceberg_catalog.db1.t2.uuid-2", "iceberg_catalog", "db1", "t2");
            Column column = new Column("c1", IntegerType.INT);
            PredicateColumnsMgr.getInstance().recordColumnUsageForTest(table, column, ColumnUsage.UseCase.PREDICATE);

            Assertions.assertTrue(PredicateColumnsMgr.getInstance().queryExternalPredicateColumns(table).isEmpty());
        } finally {
            Config.enable_external_predicate_columns_collection = defaultValue;
        }
    }

    @Test
    public void testOverlongColumnNameSkipped() {
        IcebergTable table = mockExternalTable("iceberg_catalog.db1.t3.uuid-3", "iceberg_catalog", "db1", "t3");
        Column column = new Column("c".repeat(200), IntegerType.INT);

        PredicateColumnsMgr.getInstance().recordColumnUsageForTest(table, column, ColumnUsage.UseCase.PREDICATE);

        Assertions.assertTrue(PredicateColumnsMgr.getInstance().queryExternalPredicateColumns(table).isEmpty());
    }

    @Test
    public void testResetClearsExternalState() {
        IcebergTable table = mockExternalTable("iceberg_catalog.db1.t4.uuid-4", "iceberg_catalog", "db1", "t4");
        Column column = new Column("c1", IntegerType.INT);
        PredicateColumnsMgr mgr = PredicateColumnsMgr.getInstance();

        mgr.recordColumnUsageForTest(table, column, ColumnUsage.UseCase.PREDICATE);
        Assertions.assertEquals(1, mgr.queryExternalPredicateColumns(table).size());

        mgr.reset();
        Assertions.assertTrue(mgr.queryExternalPredicateColumns(table).isEmpty());
    }

    @Test
    public void testVacuumExternalSkipsWhenTtlDisabled() {
        IcebergTable table = mockExternalTable("iceberg_catalog.db1.t5.uuid-5", "iceberg_catalog", "db1", "t5");
        Column column = new Column("c1", IntegerType.INT);
        PredicateColumnsMgr mgr = PredicateColumnsMgr.getInstance();
        mgr.recordColumnUsageForTest(table, column, ColumnUsage.UseCase.PREDICATE);
        Assertions.assertEquals(1, mgr.queryExternalPredicateColumns(table).size());

        long beforeValue = Config.statistic_external_predicate_columns_ttl_hours;
        Config.statistic_external_predicate_columns_ttl_hours = -1;
        try {
            mgr.vacuumExternal();
            Assertions.assertEquals(1, mgr.queryExternalPredicateColumns(table).size());
        } finally {
            Config.statistic_external_predicate_columns_ttl_hours = beforeValue;
        }
    }
}
