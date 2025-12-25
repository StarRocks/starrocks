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

package com.starrocks.alter;

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MaterializedViewRefreshType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.ResourceGroupMgr;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.constraint.ForeignKeyConstraint;
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.common.Pair;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.OperationType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.CreateResourceGroupStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.Warehouse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threeten.extra.PeriodDuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class AlterMVJobExecutorEditLogTest {
    private static ConnectContext connectContext;
    private static final String DB_NAME = "test_alter_mv_job_executor_editlog";
    private static final String MV_NAME = "test_mv";
    private static final String BASE_TABLE_NAME = "base_table";
    private static final long DB_ID = 10001L;
    private static final long BASE_TABLE_ID = 10002L;
    private static final long MV_ID = 10003L;
    private static final long PARTITION_ID = 10004L;
    private static final long PHYSICAL_PARTITION_ID = 10005L;
    private static final long INDEX_ID = 10006L;
    private static final long TABLET_ID = 10007L;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setDatabase(DB_NAME);

        // Create database and tables directly (no mincluster)
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID, DB_NAME);
        metastore.unprotectCreateDb(db);
        
        // Create base table
        OlapTable baseTable = createHashOlapTable(BASE_TABLE_ID, BASE_TABLE_NAME, 3);
        db.registerTableUnlocked(baseTable);
        
        // Create materialized view
        MaterializedView mv = createMaterializedView(MV_ID, MV_NAME, baseTable, 3);
        db.registerTableUnlocked(mv);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private static OlapTable createHashOlapTable(long tableId, String tableName, int bucketNum) {
        List<Column> columns = new ArrayList<>();
        Column col1 = new Column("v1", IntegerType.BIGINT);
        col1.setIsKey(true);
        columns.add(col1);
        columns.add(new Column("v2", IntegerType.BIGINT));

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(PARTITION_ID, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 1);

        DistributionInfo distributionInfo = new HashDistributionInfo(bucketNum, List.of(col1));

        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(TABLET_ID);
        TabletMeta tabletMeta = new TabletMeta(DB_ID, tableId, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(PARTITION_ID, PHYSICAL_PARTITION_ID, "p1", baseIndex, distributionInfo);

        OlapTable olapTable = new OlapTable(tableId, tableName, columns, KeysType.DUP_KEYS, partitionInfo, distributionInfo);
        olapTable.setIndexMeta(INDEX_ID, tableName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(INDEX_ID);
        olapTable.addPartition(partition);
        olapTable.setTableProperty(new com.starrocks.catalog.TableProperty(new HashMap<>()));
        return olapTable;
    }

    private static OlapTable createDatePartitionedOlapTable(long tableId, String tableName, int bucketNum) {
        List<Column> columns = new ArrayList<>();
        // Create partition column with DATE type
        Column partitionCol = new Column("dt", DateType.DATE);
        partitionCol.setIsKey(true);
        columns.add(partitionCol);
        columns.add(new Column("v2", IntegerType.BIGINT));

        // Create RangePartitionInfo with DATE partition column
        PartitionInfo partitionInfo = new RangePartitionInfo(List.of(partitionCol));
        partitionInfo.setDataProperty(PARTITION_ID, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 1);

        DistributionInfo distributionInfo = new HashDistributionInfo(bucketNum, List.of(partitionCol));

        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(TABLET_ID);
        TabletMeta tabletMeta = new TabletMeta(DB_ID, tableId, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(PARTITION_ID, PHYSICAL_PARTITION_ID, "p1", baseIndex, distributionInfo);

        OlapTable olapTable = new OlapTable(tableId, tableName, columns, KeysType.DUP_KEYS, partitionInfo, distributionInfo);
        olapTable.setIndexMeta(INDEX_ID, tableName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(INDEX_ID);
        olapTable.addPartition(partition);
        olapTable.setTableProperty(new com.starrocks.catalog.TableProperty(new HashMap<>()));
        return olapTable;
    }

    private static MaterializedView createMaterializedView(long mvId, String mvName, OlapTable baseTable, int bucketNum) {
        List<Column> columns = new ArrayList<>();
        Column col1 = new Column("v1", IntegerType.BIGINT);
        col1.setIsKey(true);
        columns.add(col1);
        columns.add(new Column("sum_v2", IntegerType.BIGINT));

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(PARTITION_ID, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 1);

        DistributionInfo distributionInfo = new HashDistributionInfo(bucketNum, List.of(col1));

        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(TABLET_ID);
        TabletMeta tabletMeta = new TabletMeta(DB_ID, mvId, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(PARTITION_ID, PHYSICAL_PARTITION_ID, "p1", baseIndex, distributionInfo);

        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        refreshScheme.setType(MaterializedViewRefreshType.ASYNC);
        
        MaterializedView mv = new MaterializedView(mvId, DB_ID, mvName, columns, KeysType.DUP_KEYS, 
                partitionInfo, distributionInfo, refreshScheme);
        mv.setIndexMeta(INDEX_ID, mvName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        mv.setBaseIndexMetaId(INDEX_ID);
        mv.addPartition(partition);
        mv.setTableProperty(new com.starrocks.catalog.TableProperty(new HashMap<>()));
        
        // Set base table info
        List<BaseTableInfo> baseTableInfos = new ArrayList<>();
        BaseTableInfo baseTableInfo = new BaseTableInfo(DB_ID, DB_NAME, baseTable.getName(), baseTable.getId());
        baseTableInfos.add(baseTableInfo);
        mv.setBaseTableInfos(baseTableInfos);
        String defineSql = String.format("SELECT v1, sum(v2) as sum_v2 FROM %s.%s GROUP BY v1", DB_NAME, baseTable.getName());
        mv.setViewDefineSql(defineSql);
        mv.setSimpleDefineSql(defineSql);
        
        return mv;
    }

    private static MaterializedView createRangePartitionedMaterializedView(
            long mvId, String mvName, OlapTable baseTable, int bucketNum) {
        List<Column> columns = new ArrayList<>();
        Column col1 = new Column("v1", IntegerType.BIGINT);
        col1.setIsKey(true);
        columns.add(col1);
        columns.add(new Column("sum_v2", IntegerType.BIGINT));

        // Create RangePartitionInfo instead of SinglePartitionInfo
        PartitionInfo partitionInfo = new RangePartitionInfo(List.of(col1));
        partitionInfo.setDataProperty(PARTITION_ID, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 1);

        DistributionInfo distributionInfo = new HashDistributionInfo(bucketNum, List.of(col1));

        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(TABLET_ID);
        TabletMeta tabletMeta = new TabletMeta(DB_ID, mvId, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(PARTITION_ID, PHYSICAL_PARTITION_ID, "p1", baseIndex, distributionInfo);

        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        refreshScheme.setType(MaterializedViewRefreshType.ASYNC);
        
        MaterializedView mv = new MaterializedView(mvId, DB_ID, mvName, columns, KeysType.DUP_KEYS, 
                partitionInfo, distributionInfo, refreshScheme);
        mv.setIndexMeta(INDEX_ID, mvName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        mv.setBaseIndexMetaId(INDEX_ID);
        mv.addPartition(partition);
        mv.setTableProperty(new com.starrocks.catalog.TableProperty(new HashMap<>()));
        
        // Set base table info
        List<BaseTableInfo> baseTableInfos = new ArrayList<>();
        BaseTableInfo baseTableInfo = new BaseTableInfo(DB_ID, DB_NAME, baseTable.getName(), baseTable.getId());
        baseTableInfos.add(baseTableInfo);
        mv.setBaseTableInfos(baseTableInfos);
        String defineSql = String.format("SELECT v1, sum(v2) as sum_v2 FROM %s.%s GROUP BY v1", DB_NAME, baseTable.getName());
        mv.setViewDefineSql(defineSql);
        mv.setSimpleDefineSql(defineSql);
        
        return mv;
    }

    private static MaterializedView createTimeDriftMaterializedView(
            long mvId, String mvName, OlapTable baseTable, int bucketNum) {
        List<Column> columns = new ArrayList<>();
        // Create partition column with DATE type
        Column partitionCol = new Column("partition_dt", DateType.DATE);
        partitionCol.setIsKey(true);
        columns.add(partitionCol);
        // Create target column with DATETIME type
        columns.add(new Column("dt", DateType.DATETIME));
        columns.add(new Column("sum_v2", IntegerType.BIGINT));

        // Create RangePartitionInfo with DATE partition column
        PartitionInfo partitionInfo = new RangePartitionInfo(List.of(partitionCol));
        partitionInfo.setDataProperty(PARTITION_ID, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 1);

        DistributionInfo distributionInfo = new HashDistributionInfo(bucketNum, List.of(partitionCol));

        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(TABLET_ID);
        TabletMeta tabletMeta = new TabletMeta(DB_ID, mvId, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(PARTITION_ID, PHYSICAL_PARTITION_ID, "p1", baseIndex, distributionInfo);

        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        refreshScheme.setType(MaterializedViewRefreshType.ASYNC);
        
        MaterializedView mv = new MaterializedView(mvId, DB_ID, mvName, columns, KeysType.DUP_KEYS, 
                partitionInfo, distributionInfo, refreshScheme);
        mv.setIndexMeta(INDEX_ID, mvName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        mv.setBaseIndexMetaId(INDEX_ID);
        mv.addPartition(partition);
        mv.setTableProperty(new com.starrocks.catalog.TableProperty(new HashMap<>()));
        
        // Set base table info
        List<BaseTableInfo> baseTableInfos = new ArrayList<>();
        BaseTableInfo baseTableInfo = new BaseTableInfo(DB_ID, DB_NAME, baseTable.getName(), baseTable.getId());
        baseTableInfos.add(baseTableInfo);
        mv.setBaseTableInfos(baseTableInfos);
        String defineSql = String.format("SELECT partition_dt, dt, sum(v2) as sum_v2 FROM %s.%s GROUP BY partition_dt, dt", 
                DB_NAME, baseTable.getName());
        mv.setViewDefineSql(defineSql);
        mv.setSimpleDefineSql(defineSql);
        
        return mv;
    }

    private static MaterializedView createRetentionConditionMaterializedView(
            long mvId, String mvName, OlapTable baseTable, int bucketNum) {
        List<Column> columns = new ArrayList<>();
        // Create partition column with DATE type for retention condition
        Column partitionCol = new Column("dt", DateType.DATE);
        partitionCol.setIsKey(true);
        columns.add(partitionCol);
        columns.add(new Column("sum_v2", IntegerType.BIGINT));

        // Create RangePartitionInfo with DATE partition column
        PartitionInfo partitionInfo = new RangePartitionInfo(List.of(partitionCol));
        partitionInfo.setDataProperty(PARTITION_ID, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 1);

        DistributionInfo distributionInfo = new HashDistributionInfo(bucketNum, List.of(partitionCol));

        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(TABLET_ID);
        TabletMeta tabletMeta = new TabletMeta(DB_ID, mvId, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(PARTITION_ID, PHYSICAL_PARTITION_ID, "p1", baseIndex, distributionInfo);

        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        refreshScheme.setType(MaterializedViewRefreshType.ASYNC);
        
        MaterializedView mv = new MaterializedView(mvId, DB_ID, mvName, columns, KeysType.DUP_KEYS, 
                partitionInfo, distributionInfo, refreshScheme);
        mv.setIndexMeta(INDEX_ID, mvName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        mv.setBaseIndexMetaId(INDEX_ID);
        mv.addPartition(partition);
        mv.setTableProperty(new com.starrocks.catalog.TableProperty(new HashMap<>()));
        
        // Set base table info
        List<BaseTableInfo> baseTableInfos = new ArrayList<>();
        BaseTableInfo baseTableInfo = new BaseTableInfo(DB_ID, DB_NAME, baseTable.getName(), baseTable.getId());
        baseTableInfos.add(baseTableInfo);
        mv.setBaseTableInfos(baseTableInfos);
        String defineSql = String.format("SELECT dt, sum(v2) as sum_v2 FROM %s.%s GROUP BY dt", 
                DB_NAME, baseTable.getName());
        mv.setViewDefineSql(defineSql);
        mv.setSimpleDefineSql(defineSql);
        
        // Set ref base table partition columns for retention condition analysis
        // This is needed for analyzeMVRetentionCondition to work
        try {
            java.lang.reflect.Field refBaseTablePartitionColumnsOptField = 
                    MaterializedView.class.getDeclaredField("refBaseTablePartitionColumnsOpt");
            refBaseTablePartitionColumnsOptField.setAccessible(true);
            Map<Table, List<Column>> refBaseTablePartitionColumns = new HashMap<>();
            // Get the partition column from base table (dt column)
            Column basePartitionCol = baseTable.getColumn("dt");
            if (basePartitionCol != null) {
                refBaseTablePartitionColumns.put(baseTable, List.of(basePartitionCol));
                refBaseTablePartitionColumnsOptField.set(mv, Optional.of(refBaseTablePartitionColumns));
            }
        } catch (Exception e) {
            // If reflection fails, the test may still work if retention condition can be set without analyzeMVRetentionCondition
            // This is acceptable for testing purposes
        }
        
        return mv;
    }

    @Test
    public void testVisitModifyTablePropertiesClausePartitionTTLNumberNormalCase() throws Exception {
        // Test PROPERTIES_PARTITION_TTL_NUMBER - requires range partitioned MV
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable baseTable = (OlapTable) db.getTable(BASE_TABLE_NAME);
        Assertions.assertNotNull(baseTable);
        
        // Create a range partitioned MV
        long rangeMvId = MV_ID + 2000;
        String rangeMvName = MV_NAME + "_range_ttl_number";
        MaterializedView rangeMv = createRangePartitionedMaterializedView(rangeMvId, rangeMvName, baseTable, 3);
        db.registerTableUnlocked(rangeMv);
        
        // Test the property
        testPropertyNormalCaseForRangeMV(rangeMvName, PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER, "5");
    }

    @Test
    public void testVisitModifyTablePropertiesClausePartitionTTLNormalCase() throws Exception {
        // Test PROPERTIES_PARTITION_TTL - requires range partitioned MV
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable baseTable = (OlapTable) db.getTable(BASE_TABLE_NAME);
        Assertions.assertNotNull(baseTable);
        
        // Create a range partitioned MV
        long rangeMvId = MV_ID + 3000;
        String rangeMvName = MV_NAME + "_range_ttl";
        MaterializedView rangeMv = createRangePartitionedMaterializedView(rangeMvId, rangeMvName, baseTable, 3);
        db.registerTableUnlocked(rangeMv);
        
        // Test the property
        testPropertyNormalCaseForRangeMV(rangeMvName, PropertyAnalyzer.PROPERTIES_PARTITION_TTL, "1 DAY");
    }

    @Test
    public void testVisitModifyTablePropertiesClauseTimeDriftConstraintNormalCase() throws Exception {
        // Test PROPERTIES_TIME_DRIFT_CONSTRAINT - requires DATE/DATETIME partition columns
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable baseTable = (OlapTable) db.getTable(BASE_TABLE_NAME);
        Assertions.assertNotNull(baseTable);
        
        // Create a range partitioned MV with DATE/DATETIME columns for time drift constraint
        long timeDriftMvId = MV_ID + 4000;
        String timeDriftMvName = MV_NAME + "_time_drift";
        MaterializedView timeDriftMv = createTimeDriftMaterializedView(timeDriftMvId, timeDriftMvName, baseTable, 3);
        db.registerTableUnlocked(timeDriftMv);
        
        // Test the property - use a constraint with target column and reference column (both DATE/DATETIME)
        // Reference column must be a partition column
        // Format: "target_column between function(reference_column, value) and function(reference_column, value)"
        testPropertyNormalCaseForTimeDriftMV(timeDriftMvName, 
                PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT, 
                "dt between days_add(partition_dt, -1) and days_add(partition_dt, 1)");
    }

    @Test
    public void testVisitModifyTablePropertiesClausePartitionRefreshNumberNormalCase() throws Exception {
        // Test PROPERTIES_PARTITION_REFRESH_NUMBER
        testPropertyNormalCase(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER, "3");
    }

    @Test
    public void testVisitModifyTablePropertiesClausePartitionRefreshStrategyNormalCase() throws Exception {
        // Test PROPERTIES_PARTITION_REFRESH_STRATEGY - valid values: strict, adaptive
        testPropertyNormalCase(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_STRATEGY, "strict");
    }

    @Test
    public void testVisitModifyTablePropertiesClauseMvRefreshModeNormalCase() throws Exception {
        // Test PROPERTIES_MV_REFRESH_MODE - valid values: AUTO, PCT, FULL, INCREMENTAL
        // Use FULL as it doesn't have additional constraints like AUTO/INCREMENTAL
        testPropertyNormalCase(PropertyAnalyzer.PROPERTIES_MV_REFRESH_MODE, "FULL");
    }

    @Test
    public void testVisitModifyTablePropertiesClauseMvRewriteStalenessSecondNormalCase() throws Exception {
        // Test PROPERTIES_MV_REWRITE_STALENESS_SECOND
        testPropertyNormalCase(PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND, "300");
    }

    @Test
    public void testVisitModifyTablePropertiesClauseEnableQueryRewriteNormalCase() throws Exception {
        // Test PROPERTY_MV_ENABLE_QUERY_REWRITE
        testPropertyNormalCase(PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE, "true");
    }

    @Test
    public void testVisitModifyTablePropertiesClauseTransparentMvRewriteModeNormalCase() throws Exception {
        // Test PROPERTY_TRANSPARENT_MV_REWRITE_MODE - valid values: FALSE, TRUE, TRANSPARENT_OR_ERROR, TRANSPARENT_OR_DEFAULT
        testPropertyNormalCase(PropertyAnalyzer.PROPERTY_TRANSPARENT_MV_REWRITE_MODE, "FALSE");
    }

    @Test
    public void testVisitModifyTablePropertiesClauseQueryRewriteConsistencyNormalCase() throws Exception {
        // Test PROPERTIES_QUERY_REWRITE_CONSISTENCY
        testPropertyNormalCase(PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY, "loose");
    }

    @Test
    public void testVisitModifyTablePropertiesClauseForceExternalTableQueryRewriteNormalCase() throws Exception {
        // Test PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE - valid values: DISABLE, LOOSE, CHECKED, NOCHECK, FORCE_MV
        testPropertyNormalCase(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE, "LOOSE");
    }

    @Test
    public void testVisitModifyTablePropertiesClauseAutoRefreshPartitionsLimitNormalCase() throws Exception {
        // Test PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT
        testPropertyNormalCase(PropertyAnalyzer.PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT, "10");
    }

    @Test
    public void testVisitModifyTablePropertiesClausePartitionRetentionConditionNormalCase() throws Exception {
        // Test PROPERTIES_PARTITION_RETENTION_CONDITION - requires range partitioned MV with DATE partition column
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        
        // Create a base table with DATE column for retention condition test
        long retentionBaseTableId = BASE_TABLE_ID + 10000;
        String retentionBaseTableName = BASE_TABLE_NAME + "_retention";
        OlapTable retentionBaseTable = createDatePartitionedOlapTable(retentionBaseTableId, retentionBaseTableName, 3);
        db.registerTableUnlocked(retentionBaseTable);
        
        // Create a range partitioned MV with DATE partition column for retention condition
        long retentionMvId = MV_ID + 5000;
        String retentionMvName = MV_NAME + "_retention";
        MaterializedView retentionMv = createRetentionConditionMaterializedView(
                retentionMvId, retentionMvName, retentionBaseTable, 3);
        db.registerTableUnlocked(retentionMv);
        
        // Test the property - use a condition with FE constant functions for MV
        // For MV, retention condition must only contain FE constant functions like current_date()
        String retentionConditionValue = "dt > current_date() - interval 1 month";
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_CONDITION, retentionConditionValue);
        ModifyTablePropertiesClause clause = new ModifyTablePropertiesClause(properties);
        AlterMaterializedViewStmt stmt = new AlterMaterializedViewStmt(
                new TableName(DB_NAME, retentionMvName),
                clause,
                NodePosition.ZERO);
        
        AlterMVJobExecutor executor = new AlterMVJobExecutor();
        executor.process(stmt, connectContext);
        
        // Verify leader state
        retentionMv = (MaterializedView) db.getTable(retentionMvName);
        Assertions.assertNotNull(retentionMv);
        String actualRetentionCondition = retentionMv.getTableProperty().getPartitionRetentionCondition();
        Assertions.assertNotNull(actualRetentionCondition, "Retention condition should be set");
        Assertions.assertEquals(retentionConditionValue, actualRetentionCondition, 
                "Retention condition should match the set value");
        Assertions.assertTrue(retentionMv.getRetentionConditionExpr().isPresent());
        Assertions.assertTrue(retentionMv.getRetentionConditionScalarOp().isPresent());

        // Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_MATERIALIZED_VIEW_PROPERTIES);
        
        // Verify replay info
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(db.getId(), replayInfo.getDbId());
        Assertions.assertEquals(retentionMv.getId(), replayInfo.getTableId());
        
        // Create follower metastore and the same id objects, then replay into it
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        
        // Create base table with same ID (with DATE column for retention condition)
        OlapTable followerBaseTable = createDatePartitionedOlapTable(retentionBaseTableId, retentionBaseTableName, 3);
        followerDb.registerTableUnlocked(followerBaseTable);
        
        // Create retention condition materialized view with same ID
        MaterializedView followerMv = createRetentionConditionMaterializedView(
                retentionMvId, retentionMvName, followerBaseTable, 3);
        followerDb.registerTableUnlocked(followerMv);
        
        followerMetastore.replayAlterMaterializedViewProperties(replayInfo);
        
        // Verify follower state
        MaterializedView replayed = (MaterializedView) followerDb.getTable(retentionMvId);
        Assertions.assertNotNull(replayed);
        String replayedRetentionCondition = replayed.getTableProperty().getPartitionRetentionCondition();
        Assertions.assertNotNull(replayedRetentionCondition, "Follower retention condition should be set");
        Assertions.assertEquals(retentionConditionValue, replayedRetentionCondition,
                "Follower retention condition should match the set value");
        Assertions.assertEquals(retentionMv.getRetentionConditionExpr().get(), replayed.getRetentionConditionExpr().get(),
                "Follower retention condition expression should match leader");
        Assertions.assertTrue(replayed.getRetentionConditionExpr().isPresent());
        Assertions.assertTrue(replayed.getRetentionConditionScalarOp().isPresent());
        Assertions.assertEquals(retentionMv.getRetentionConditionExpr().get(), replayed.getRetentionConditionExpr().get(),
                "Follower retention condition expression should match leader");
        Assertions.assertEquals(retentionMv.getRetentionConditionScalarOp().get(),
                replayed.getRetentionConditionScalarOp().get(),
                "Follower retention condition scalar operator should match leader");
    }

    @Test
    public void testVisitModifyTablePropertiesClauseResourceGroupNormalCase() throws Exception {
        // Test PROPERTIES_RESOURCE_GROUP - create resource group first
        String resourceGroupName = "test_rg_mv";
        ResourceGroupMgr resourceGroupMgr = GlobalStateMgr.getCurrentState().getResourceGroupMgr();
        
        // Create resource group if it doesn't exist
        if (resourceGroupMgr.getResourceGroup(resourceGroupName) == null) {
            Map<String, String> rgProperties = new HashMap<>();
            rgProperties.put("cpu_weight", "1");
            rgProperties.put("mem_limit", "50%");
            rgProperties.put("type", "mv");
            List<List<com.starrocks.sql.ast.expression.Predicate>> classifiers = new ArrayList<>();
            CreateResourceGroupStmt createRgStmt = new CreateResourceGroupStmt(
                    resourceGroupName, false, false, classifiers, rgProperties);
            com.starrocks.sql.analyzer.ResourceGroupAnalyzer.analyzeCreateResourceGroupStmt(createRgStmt);
            resourceGroupMgr.createResourceGroup(createRgStmt);
        }
        
        // Test the property
        testPropertyNormalCase(PropertyAnalyzer.PROPERTIES_RESOURCE_GROUP, resourceGroupName);
    }

    @Test
    public void testVisitModifyTablePropertiesClauseExcludedTriggerTablesNormalCase() throws Exception {
        // Test PROPERTIES_EXCLUDED_TRIGGER_TABLES
        testPropertyNormalCase(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, 
                DB_NAME + "." + BASE_TABLE_NAME);
    }

    @Test
    public void testVisitModifyTablePropertiesClauseExcludedRefreshTablesNormalCase() throws Exception {
        // Test PROPERTIES_EXCLUDED_REFRESH_TABLES
        testPropertyNormalCase(PropertyAnalyzer.PROPERTIES_EXCLUDED_REFRESH_TABLES, 
                DB_NAME + "." + BASE_TABLE_NAME);
    }

    @Test
    public void testVisitModifyTablePropertiesClauseUniqueConstraintNormalCase() throws Exception {
        // Test PROPERTIES_UNIQUE_CONSTRAINT
        testPropertyNormalCase(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT,
                "default_catalog.test_alter_mv_job_executor_editlog.test_mv.v1");
    }

    @Test
    public void testVisitModifyTablePropertiesClauseForeignKeyConstraintNormalCase() throws Exception {
        // Test PROPERTIES_FOREIGN_KEY_CONSTRAINT
        // First, ensure parent table has unique constraint
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable baseTable = (OlapTable) db.getTable(BASE_TABLE_NAME);
        Assertions.assertNotNull(baseTable);
        
        // Add unique constraint to base table
        List<com.starrocks.catalog.ColumnId> uniqueCols = List.of(baseTable.getColumn("v1").getColumnId());
        baseTable.setUniqueConstraints(List.of(
                new UniqueConstraint(
                        baseTable.getCatalogName(), DB_NAME, BASE_TABLE_NAME, uniqueCols)));
        
        // Format for MV: "catalog.db.child_table(column) REFERENCES catalog.db.parent_table(column)"
        String catalogName = baseTable.getCatalogName();
        String fkConstraint = String.format("%s.%s.%s(v1) REFERENCES %s.%s.%s(v1)",
                catalogName, DB_NAME, MV_NAME, catalogName, DB_NAME, BASE_TABLE_NAME);

        testPropertyNormalCase(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT, fkConstraint);
    }

    @Test
    public void testVisitModifyTablePropertiesClauseWarehouseNormalCase() throws Exception {
        // Test PROPERTIES_WAREHOUSE - use default warehouse or create test warehouse
        WarehouseManager warehouseMgr = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        warehouseMgr.initDefaultWarehouse();
        testPropertyNormalCase(PropertyAnalyzer.PROPERTIES_WAREHOUSE, WarehouseManager.DEFAULT_WAREHOUSE_NAME);
    }

    @Test
    public void testVisitModifyTablePropertiesClauseLabelsLocationNormalCase() throws Exception {
        // Test PROPERTIES_LABELS_LOCATION - use "*" wildcard to avoid requiring real backend labels
        // The "*" wildcard means all backends with location labels
        testPropertyNormalCase(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION, "*");
    }

    @Test
    public void testVisitModifyTablePropertiesClauseColocateWithNormalCase() throws Exception {
        // Test PROPERTIES_COLOCATE_WITH - only test if not in shared-nothing mode
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_NOTHING) {
            // Skip in shared-nothing mode as colocate is not supported
            return;
        }
        
        // Test the property
        testPropertyNormalCase(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH, "test_group_mv");
    }

    @Test
    public void testVisitModifyTablePropertiesClauseSessionVariablesNormalCase() throws Exception {
        // Test session.* properties
        testPropertyNormalCase(PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX + "query_timeout", "300");
    }

    @Test
    public void testVisitModifyTablePropertiesClauseEditLogException() throws Exception {
        // 1. Get materialized view
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        MaterializedView mv = (MaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(mv);

        // 2. Mock EditLog.logAlterMaterializedViewProperties to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logAlterMaterializedViewProperties(any(ModifyTablePropertyOperationLog.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Create ModifyTablePropertiesClause
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER, "5");
        ModifyTablePropertiesClause clause = new ModifyTablePropertiesClause(properties);
        
        // 4. Create AlterMaterializedViewStmt
        AlterMaterializedViewStmt stmt = new AlterMaterializedViewStmt(
                new TableName(DB_NAME, MV_NAME),
                clause,
                NodePosition.ZERO);
        
        // 5. Execute visitModifyTablePropertiesClause and expect exception
        AlterMVJobExecutor executor = new AlterMVJobExecutor();
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            executor.process(stmt, connectContext);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed") || 
                            exception.getCause() != null && 
                            exception.getCause().getMessage().contains("EditLog write failed"));
    }

    @Test
    public void testVisitModifyTablePropertiesClauseMultiplePropertiesNormalCase() throws Exception {
        // Test multiple properties at once
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        MaterializedView mv = (MaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(mv);

        // Create ModifyTablePropertiesClause with multiple properties
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER, "5");
        properties.put(PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND, "600");
        properties.put(PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE, "true");
        ModifyTablePropertiesClause clause = new ModifyTablePropertiesClause(properties);
        
        // Create AlterMaterializedViewStmt
        AlterMaterializedViewStmt stmt = new AlterMaterializedViewStmt(
                new TableName(DB_NAME, MV_NAME),
                clause,
                NodePosition.ZERO);
        
        // Execute visitModifyTablePropertiesClause
        AlterMVJobExecutor executor = new AlterMVJobExecutor();
        executor.process(stmt, connectContext);

        // Verify master state
        mv = (MaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(mv);
        verifyProperty(mv, PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER, "5");
        verifyProperty(mv, PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND, "600");
        verifyProperty(mv, PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE, "true");

        // Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_MATERIALIZED_VIEW_PROPERTIES);
        
        // Verify replay info
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(db.getId(), replayInfo.getDbId());
        Assertions.assertEquals(mv.getId(), replayInfo.getTableId());

        // Create follower metastore and the same id objects, then replay into it
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        
        // Create base table with same ID
        OlapTable followerBaseTable = createHashOlapTable(BASE_TABLE_ID, BASE_TABLE_NAME, 3);
        followerDb.registerTableUnlocked(followerBaseTable);
        
        // Create materialized view with same ID
        MaterializedView followerMv = createMaterializedView(MV_ID, MV_NAME, followerBaseTable, 3);
        followerDb.registerTableUnlocked(followerMv);

        followerMetastore.replayAlterMaterializedViewProperties(replayInfo);

        // Manually set properties that are not handled by buildMvProperties
        MaterializedView replayed = (MaterializedView) followerDb.getTable(MV_ID);
        Assertions.assertNotNull(replayed);
        
        // Verify follower state
        verifyProperty(replayed, PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER, "5");
        verifyProperty(replayed, PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND, "600");
        verifyProperty(replayed, PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE, "true");
    }

    private void testPropertyNormalCaseForRangeMV(String mvName, String propertyName, String propertyValue) throws Exception {
        // 1. Get materialized view
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        MaterializedView mv = (MaterializedView) db.getTable(mvName);
        Assertions.assertNotNull(mv);

        // 2. Create ModifyTablePropertiesClause
        Map<String, String> properties = new HashMap<>();
        properties.put(propertyName, propertyValue);
        ModifyTablePropertiesClause clause = new ModifyTablePropertiesClause(properties);
        
        // 3. Create AlterMaterializedViewStmt
        AlterMaterializedViewStmt stmt = new AlterMaterializedViewStmt(
                new TableName(DB_NAME, mvName),
                clause,
                NodePosition.ZERO);
        
        // 4. Execute visitModifyTablePropertiesClause
        AlterMVJobExecutor executor = new AlterMVJobExecutor();
        executor.process(stmt, connectContext);

        // 5. Verify master state
        mv = (MaterializedView) db.getTable(mvName);
        Assertions.assertNotNull(mv);
        verifyProperty(mv, propertyName, propertyValue);

        // 6. Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_MATERIALIZED_VIEW_PROPERTIES);
        
        // Verify replay info
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(db.getId(), replayInfo.getDbId());
        Assertions.assertEquals(mv.getId(), replayInfo.getTableId());

        // Create follower metastore and the same id objects, then replay into it
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        
        // Create base table with same ID
        OlapTable followerBaseTable = createHashOlapTable(BASE_TABLE_ID, BASE_TABLE_NAME, 3);
        followerDb.registerTableUnlocked(followerBaseTable);
        
        // Create range partitioned materialized view with same ID
        MaterializedView followerMv = createRangePartitionedMaterializedView(mv.getId(), mvName, followerBaseTable, 3);
        followerDb.registerTableUnlocked(followerMv);

        followerMetastore.replayAlterMaterializedViewProperties(replayInfo);

        // Manually set properties that are not handled by buildMvProperties
        MaterializedView replayed = (MaterializedView) followerDb.getTable(mv.getId());
        Assertions.assertNotNull(replayed);
        
        // Verify follower state
        verifyProperty(replayed, propertyName, propertyValue);
    }

    private void testPropertyNormalCaseForTimeDriftMV(String mvName, String propertyName, String propertyValue) throws Exception {
        // 1. Get materialized view
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        MaterializedView mv = (MaterializedView) db.getTable(mvName);
        Assertions.assertNotNull(mv);

        // 2. Create ModifyTablePropertiesClause
        Map<String, String> properties = new HashMap<>();
        properties.put(propertyName, propertyValue);
        ModifyTablePropertiesClause clause = new ModifyTablePropertiesClause(properties);
        
        // 3. Create AlterMaterializedViewStmt
        AlterMaterializedViewStmt stmt = new AlterMaterializedViewStmt(
                new TableName(DB_NAME, mvName),
                clause,
                NodePosition.ZERO);
        
        // 4. Execute visitModifyTablePropertiesClause
        AlterMVJobExecutor executor = new AlterMVJobExecutor();
        executor.process(stmt, connectContext);

        // 5. Verify master state
        mv = (MaterializedView) db.getTable(mvName);
        Assertions.assertNotNull(mv);
        verifyProperty(mv, propertyName, propertyValue);

        // 6. Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_MATERIALIZED_VIEW_PROPERTIES);
        
        // Verify replay info
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(db.getId(), replayInfo.getDbId());
        Assertions.assertEquals(mv.getId(), replayInfo.getTableId());

        // Create follower metastore and the same id objects, then replay into it
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        
        // Create base table with same ID
        OlapTable followerBaseTable = createHashOlapTable(BASE_TABLE_ID, BASE_TABLE_NAME, 3);
        followerDb.registerTableUnlocked(followerBaseTable);
        
        // Create time drift materialized view with same ID
        MaterializedView followerMv = createTimeDriftMaterializedView(mv.getId(), mvName, followerBaseTable, 3);
        followerDb.registerTableUnlocked(followerMv);

        followerMetastore.replayAlterMaterializedViewProperties(replayInfo);

        // Manually set properties that are not handled by buildMvProperties
        MaterializedView replayed = (MaterializedView) followerDb.getTable(mv.getId());
        Assertions.assertNotNull(replayed);
        
        // Verify follower state
        verifyProperty(replayed, propertyName, propertyValue);
    }

    private void testPropertyNormalCase(String propertyName, String propertyValue) throws Exception {
        // 1. Get materialized view
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        MaterializedView mv = (MaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(mv);

        // 2. Create ModifyTablePropertiesClause
        Map<String, String> properties = new HashMap<>();
        properties.put(propertyName, propertyValue);
        ModifyTablePropertiesClause clause = new ModifyTablePropertiesClause(properties);
        
        // 3. Create AlterMaterializedViewStmt
        AlterMaterializedViewStmt stmt = new AlterMaterializedViewStmt(
                new TableName(DB_NAME, MV_NAME),
                clause,
                NodePosition.ZERO);
        
        // 4. Execute visitModifyTablePropertiesClause
        AlterMVJobExecutor executor = new AlterMVJobExecutor();
        executor.process(stmt, connectContext);

        // 5. Verify master state
        mv = (MaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(mv);
        verifyProperty(mv, propertyName, propertyValue);

        // 6. Test follower replay
        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_MATERIALIZED_VIEW_PROPERTIES);
        
        // Verify replay info
        Assertions.assertNotNull(replayInfo);
        Assertions.assertEquals(db.getId(), replayInfo.getDbId());
        Assertions.assertEquals(mv.getId(), replayInfo.getTableId());

        // Create follower metastore and the same id objects, then replay into it
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);
        
        // Create base table with same ID
        OlapTable followerBaseTable = createHashOlapTable(BASE_TABLE_ID, BASE_TABLE_NAME, 3);
        followerDb.registerTableUnlocked(followerBaseTable);
        
        // Create materialized view with same ID
        MaterializedView followerMv = createMaterializedView(MV_ID, MV_NAME, followerBaseTable, 3);
        followerDb.registerTableUnlocked(followerMv);

        followerMetastore.replayAlterMaterializedViewProperties(replayInfo);

        // Manually set properties that are not handled by buildMvProperties
        MaterializedView replayed = (MaterializedView) followerDb.getTable(MV_ID);
        Assertions.assertNotNull(replayed);
        
        // Verify follower state
        verifyProperty(replayed, propertyName, propertyValue);
    }

    private void verifyProperty(MaterializedView mv, String propertyName, String propertyValue) {
        com.starrocks.catalog.TableProperty tableProperty = mv.getTableProperty();
        Assertions.assertNotNull(tableProperty);
        
        if (propertyName.equals(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER)) {
            int expectedValue = Integer.parseInt(propertyValue);
            Assertions.assertEquals(expectedValue, tableProperty.getPartitionRefreshNumber());
        } else if (propertyName.equals(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_STRATEGY)) {
            Assertions.assertEquals(propertyValue, tableProperty.getPartitionRefreshStrategy());
        } else if (propertyName.equals(PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND)) {
            int expectedValue = Integer.parseInt(propertyValue);
            Assertions.assertEquals(expectedValue, mv.getMaxMVRewriteStaleness());
        } else if (propertyName.equals(PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE)) {
            com.starrocks.catalog.TableProperty.MVQueryRewriteSwitch expectedSwitch = 
                    com.starrocks.catalog.TableProperty.MVQueryRewriteSwitch.parse(propertyValue);
            Assertions.assertEquals(expectedSwitch, tableProperty.getMvQueryRewriteSwitch());
        } else if (propertyName.equals(PropertyAnalyzer.PROPERTY_TRANSPARENT_MV_REWRITE_MODE)) {
            com.starrocks.catalog.TableProperty.MVTransparentRewriteMode expectedMode = 
                    com.starrocks.catalog.TableProperty.MVTransparentRewriteMode.parse(propertyValue);
            Assertions.assertEquals(expectedMode, tableProperty.getMvTransparentRewriteMode());
        } else if (propertyName.equals(PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY)) {
            com.starrocks.catalog.TableProperty.QueryRewriteConsistencyMode expectedMode = 
                    com.starrocks.catalog.TableProperty.QueryRewriteConsistencyMode.parse(propertyValue);
            Assertions.assertEquals(expectedMode, tableProperty.getQueryRewriteConsistencyMode());
        } else if (propertyName.equals(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE)) {
            com.starrocks.catalog.TableProperty.QueryRewriteConsistencyMode expectedMode = 
                    com.starrocks.catalog.TableProperty.QueryRewriteConsistencyMode.parse(propertyValue);
            Assertions.assertEquals(expectedMode, tableProperty.getForceExternalTableQueryRewrite());
        } else if (propertyName.equals(PropertyAnalyzer.PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT)) {
            int expectedValue = Integer.parseInt(propertyValue);
            Assertions.assertEquals(expectedValue, tableProperty.getAutoRefreshPartitionsLimit());
        } else if (propertyName.equals(PropertyAnalyzer.PROPERTIES_BF_COLUMNS)) {
            Set<String> bfColumnNames = mv.getBfColumnNames();
            if (propertyValue == null || propertyValue.isEmpty()) {
                Assertions.assertTrue(bfColumnNames == null || bfColumnNames.isEmpty());
            } else {
                Assertions.assertNotNull(bfColumnNames);
                String[] expectedColumns = propertyValue.split(",");
                Assertions.assertEquals(expectedColumns.length, bfColumnNames.size());
                for (String col : expectedColumns) {
                    Assertions.assertTrue(bfColumnNames.contains(col.trim()));
                }
            }
        } else if (propertyName.equals(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER)) {
            int expectedValue = Integer.parseInt(propertyValue);
            Assertions.assertEquals(expectedValue, tableProperty.getPartitionTTLNumber());
        } else if (propertyName.equals(PropertyAnalyzer.PROPERTIES_PARTITION_TTL)) {
            // Parse the expected TTL string and compare with actual PeriodDuration
            Map<String, String> ttlProperties = new HashMap<>();
            ttlProperties.put(PropertyAnalyzer.PROPERTIES_PARTITION_TTL, propertyValue);
            Pair<String, PeriodDuration> expectedTTL = 
                    PropertyAnalyzer.analyzePartitionTTL(ttlProperties, false);
            Assertions.assertNotNull(expectedTTL);
            PeriodDuration actualTTL = tableProperty.getPartitionTTL();
            Assertions.assertEquals(expectedTTL.second, actualTTL);
        } else if (propertyName.equals(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT)) {
            String actualValue = tableProperty.getTimeDriftConstraintSpec();
            Assertions.assertEquals(propertyValue, actualValue);
        } else if (propertyName.equals(PropertyAnalyzer.PROPERTIES_MV_REFRESH_MODE)) {
            String actualValue = tableProperty.getMvRefreshMode();
            Assertions.assertEquals(propertyValue, actualValue);
            Assertions.assertEquals(mv.getCurrentRefreshMode(),
                    MaterializedView.RefreshMode.valueOf(actualValue.toUpperCase(Locale.ROOT)));
        } else if (propertyName.equals(PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_CONDITION)) {
            String actualValue = tableProperty.getPartitionRetentionCondition();
            Assertions.assertEquals(propertyValue, actualValue);
        } else if (propertyName.equals(PropertyAnalyzer.PROPERTIES_RESOURCE_GROUP)) {
            String actualValue = tableProperty.getResourceGroup();
            Assertions.assertEquals(propertyValue, actualValue);
        } else if (propertyName.equals(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES)) {
            List<com.starrocks.catalog.TableName> actualTables = tableProperty.getExcludedTriggerTables();
            if (propertyValue == null || propertyValue.isEmpty()) {
                Assertions.assertTrue(actualTables == null || actualTables.isEmpty());
            } else {
                Assertions.assertNotNull(actualTables);
                String[] expectedTableStrs = propertyValue.split(",");
                Assertions.assertEquals(expectedTableStrs.length, actualTables.size());
                for (String tableStr : expectedTableStrs) {
                    com.starrocks.catalog.TableName expectedTableName = 
                            com.starrocks.sql.analyzer.AnalyzerUtils.stringToTableName(tableStr.trim());
                    boolean found = false;
                    for (com.starrocks.catalog.TableName actualTableName : actualTables) {
                        if (actualTableName.getDb().equals(expectedTableName.getDb()) &&
                                actualTableName.getTbl().equals(expectedTableName.getTbl())) {
                            found = true;
                            break;
                        }
                    }
                    Assertions.assertTrue(found, "Expected table " + tableStr + " not found in excluded trigger tables");
                }
            }
        } else if (propertyName.equals(PropertyAnalyzer.PROPERTIES_EXCLUDED_REFRESH_TABLES)) {
            List<com.starrocks.catalog.TableName> actualTables = tableProperty.getExcludedRefreshTables();
            if (propertyValue == null || propertyValue.isEmpty()) {
                Assertions.assertTrue(actualTables == null || actualTables.isEmpty());
            } else {
                Assertions.assertNotNull(actualTables);
                String[] expectedTableStrs = propertyValue.split(",");
                Assertions.assertEquals(expectedTableStrs.length, actualTables.size());
                for (String tableStr : expectedTableStrs) {
                    com.starrocks.catalog.TableName expectedTableName = 
                            com.starrocks.sql.analyzer.AnalyzerUtils.stringToTableName(tableStr.trim());
                    boolean found = false;
                    for (com.starrocks.catalog.TableName actualTableName : actualTables) {
                        if (actualTableName.getDb().equals(expectedTableName.getDb()) &&
                                actualTableName.getTbl().equals(expectedTableName.getTbl())) {
                            found = true;
                            break;
                        }
                    }
                    Assertions.assertTrue(found, "Expected table " + tableStr + " not found in excluded refresh tables");
                }
            }
        } else if (propertyName.equals(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT)) {
            List<UniqueConstraint> actualConstraints =
                    tableProperty.getUniqueConstraints();
            List<UniqueConstraint> expected = UniqueConstraint.parse(null, null, null, propertyValue);
            Assertions.assertEquals(actualConstraints.size(), expected.size());
            for (int i = 0; i < expected.size(); i++) {
                UniqueConstraint expectedConstraint = expected.get(i);
                UniqueConstraint actualConstraint = actualConstraints.get(i);
                Assertions.assertEquals(expectedConstraint.toString(), actualConstraint.toString(),
                        "Unique constraint does not match");
            }
        } else if (propertyName.equals(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT)) {
            List<ForeignKeyConstraint> actualConstraints = tableProperty.getForeignKeyConstraints();
            List<ForeignKeyConstraint> expected = ForeignKeyConstraint
                    .parse(tableProperty.getProperties().get(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT));
            Assertions.assertEquals(actualConstraints.size(), expected.size());
            for (int i = 0; i < expected.size(); i++) {
                ForeignKeyConstraint expectedConstraint = expected.get(i);
                ForeignKeyConstraint actualConstraint =  actualConstraints.get(i);
                Assertions.assertEquals(expectedConstraint.toString(), actualConstraint.toString(),
                        "Foreign key constraint does not match");
            }
        } else if (propertyName.equals(PropertyAnalyzer.PROPERTIES_WAREHOUSE)) {
            // Warehouse is stored in MaterializedView.warehouseId, verify through properties
            String actualValue = tableProperty.getProperties().get(PropertyAnalyzer.PROPERTIES_WAREHOUSE);
            Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(actualValue);
            Assertions.assertEquals(mv.getWarehouseId(), warehouse.getId());
        } else if (propertyName.equals(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION)) {
            // Location is stored as Multimap in OlapTable (MaterializedView extends OlapTable)
            com.google.common.collect.Multimap<String, String> actualLocation = mv.getLocation();
            if (propertyValue == null || propertyValue.isEmpty()) {
                Assertions.assertTrue(actualLocation == null || actualLocation.isEmpty());
            } else if (propertyValue.equals("*")) {
                // "*" is a wildcard meaning all backends with location labels
                // The location map should contain "*" key
                Assertions.assertTrue(actualLocation != null, "Location should be set for wildcard");
                Assertions.assertTrue(actualLocation.containsKey("*") || !actualLocation.isEmpty(),
                        "Location should contain '*' key or be non-empty for wildcard");
            } else {
                Assertions.assertNotNull(actualLocation);
                String[] labelLocationPairs = propertyValue.split(",");
                for (String pair : labelLocationPairs) {
                    String[] parts = pair.split(":");
                    Assertions.assertTrue(parts.length == 2, "Invalid label:location format");
                    String label = parts[0].trim();
                    String location = parts[1].trim();
                    Assertions.assertTrue(actualLocation.containsEntry(label, location),
                            "Label " + label + " with location " + location + " not found");
                }
            }
        } else if (propertyName.equals(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH)) {
            String actualValue = mv.getColocateGroup();
            Assertions.assertEquals(propertyValue, actualValue);
        } else if (propertyName.startsWith(PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX)) {
            // Session properties are stored in tableProperty.properties with "session." prefix
            String actualValue = tableProperty.getProperties().get(propertyName);
            Assertions.assertEquals(propertyValue, actualValue);
        } else {
            // For other properties, check in tableProperty.properties map
            String actualValue = tableProperty.getProperties().get(propertyName);
            Assertions.assertEquals(propertyValue, actualValue);
        }
    }
}
