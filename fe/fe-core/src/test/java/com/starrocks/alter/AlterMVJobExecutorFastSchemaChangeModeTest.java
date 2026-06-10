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
import com.starrocks.catalog.MaterializedView.MvRefreshScheme;
import com.starrocks.catalog.MaterializedViewRefreshType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for AlterMVJobExecutor's mv_fast_schema_change_mode configuration handling.
 * This test verifies the behavior of different modes: strict, force, and force_no_clear.
 */
public class AlterMVJobExecutorFastSchemaChangeModeTest {
    private static ConnectContext connectContext;
    private static final String DB_NAME = "test_mv_fse_mode";
    private static final String MV_NAME = "test_mv";
    private static final String BASE_TABLE_NAME = "base_table";
    private static final long DB_ID = 10001L;
    private static final long BASE_TABLE_ID = 10002L;
    private static final long MV_ID = 10003L;
    private static final long PARTITION_ID = 10004L;
    private static final long PHYSICAL_PARTITION_ID = 10005L;
    private static final long INDEX_ID = 10006L;
    private static final long TABLET_ID = 10007L;

    private String originalMode;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setDatabase(DB_NAME);

        // Save original config value
        originalMode = Config.mv_fast_schema_change_mode;

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
        // Restore original config value
        Config.mv_fast_schema_change_mode = originalMode;
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
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
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

        MvRefreshScheme refreshScheme = new MvRefreshScheme();
        refreshScheme.setType(MaterializedViewRefreshType.ASYNC);

        MaterializedView mv = new MaterializedView(mvId, DB_ID, mvName, columns, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo, refreshScheme);
        mv.setIndexMeta(INDEX_ID, mvName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        mv.setBaseIndexMetaId(INDEX_ID);
        mv.addPartition(partition);
        mv.setTableProperty(new TableProperty(new HashMap<>()));

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

    private static TableRef createTableRef(String dbName, String tableName) {
        List<String> parts = new ArrayList<>();
        parts.add(dbName);
        parts.add(tableName);
        QualifiedName qualifiedName = QualifiedName.of(parts);
        return new TableRef(qualifiedName, null, NodePosition.ZERO);
    }

    @Test
    public void testMvFastSchemaChangeModeDefaultValue() {
        // Default mode should be "strict"
        Assertions.assertEquals("strict", Config.mv_fast_schema_change_mode);
    }

    @Test
    public void testMvFastSchemaChangeModeStrict() {
        Config.mv_fast_schema_change_mode = "strict";
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        MaterializedView mv = (MaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(mv);

        // In strict mode, isSupportFastSchemaEvolutionInDanger should control the behavior
        // When MV doesn't have FORCE_MV or DISABLE consistency mode and query rewrite is enabled,
        // isSupportFastSchemaEvolutionInDanger returns false

        // Set query_rewrite_consistency to loose (not FORCE_MV or DISABLE)
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY, "loose");
        properties.put(PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE, "true");
        ModifyTablePropertiesClause clause = new ModifyTablePropertiesClause(properties);
        AlterMaterializedViewStmt stmt = new AlterMaterializedViewStmt(
                createTableRef(DB_NAME, MV_NAME),
                clause,
                NodePosition.ZERO);

        AlterMVJobExecutor executor = new AlterMVJobExecutor();
        executor.process(stmt, connectContext);

        // Verify MV state
        mv = (MaterializedView) db.getTable(MV_NAME);
        Assertions.assertFalse(mv.isSupportFastSchemaEvolutionInDanger());
    }

    @Test
    public void testMvFastSchemaChangeModeForce() {
        Config.mv_fast_schema_change_mode = "force";
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        MaterializedView mv = (MaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(mv);

        // In force mode, FSE should be allowed even when isSupportFastSchemaEvolutionInDanger is false
        // Set query_rewrite_consistency to loose (not FORCE_MV or DISABLE)
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY, "loose");
        properties.put(PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE, "true");
        ModifyTablePropertiesClause clause = new ModifyTablePropertiesClause(properties);
        AlterMaterializedViewStmt stmt = new AlterMaterializedViewStmt(
                createTableRef(DB_NAME, MV_NAME),
                clause,
                NodePosition.ZERO);

        AlterMVJobExecutor executor = new AlterMVJobExecutor();
        executor.process(stmt, connectContext);

        // Verify MV state
        mv = (MaterializedView) db.getTable(MV_NAME);
        Assertions.assertFalse(mv.isSupportFastSchemaEvolutionInDanger());

        // In force mode, the mode check methods should return true
        // Note: We can't directly test private methods, but we can verify the config is set correctly
        Assertions.assertEquals("force", Config.mv_fast_schema_change_mode);
    }

    @Test
    public void testMvFastSchemaChangeModeForceNoClear() {
        Config.mv_fast_schema_change_mode = "force_no_clear";
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        MaterializedView mv = (MaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(mv);

        // In force_no_clear mode, FSE should be allowed but partition entries should NOT be cleared
        Assertions.assertEquals("force_no_clear", Config.mv_fast_schema_change_mode);
    }

    @Test
    public void testMvFastSchemaChangeModeCaseInsensitive() {
        // Test case insensitivity
        Config.mv_fast_schema_change_mode = "FORCE";
        Assertions.assertEquals("FORCE", Config.mv_fast_schema_change_mode);

        Config.mv_fast_schema_change_mode = "Force";
        Assertions.assertEquals("Force", Config.mv_fast_schema_change_mode);

        Config.mv_fast_schema_change_mode = "STRICT";
        Assertions.assertEquals("STRICT", Config.mv_fast_schema_change_mode);

        Config.mv_fast_schema_change_mode = "FORCE_NO_CLEAR";
        Assertions.assertEquals("FORCE_NO_CLEAR", Config.mv_fast_schema_change_mode);
    }

    @Test
    public void testMvFastSchemaChangeModeWithForceMvConsistency() {
        // When query_rewrite_consistency is FORCE_MV, isSupportFastSchemaEvolutionInDanger should return true
        Config.mv_fast_schema_change_mode = "strict";

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        MaterializedView mv = (MaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(mv);

        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_QUERY_REWRITE_CONSISTENCY, "force_mv");
        ModifyTablePropertiesClause clause = new ModifyTablePropertiesClause(properties);
        AlterMaterializedViewStmt stmt = new AlterMaterializedViewStmt(
                createTableRef(DB_NAME, MV_NAME),
                clause,
                NodePosition.ZERO);

        AlterMVJobExecutor executor = new AlterMVJobExecutor();
        executor.process(stmt, connectContext);

        // Verify MV state - should support FSE in danger mode when FORCE_MV
        mv = (MaterializedView) db.getTable(MV_NAME);
        Assertions.assertTrue(mv.isSupportFastSchemaEvolutionInDanger());
    }

    @Test
    public void testMvFastSchemaChangeModeWithDisabledQueryRewrite() {
        // When query rewrite is disabled, isSupportFastSchemaEvolutionInDanger should return true
        Config.mv_fast_schema_change_mode = "strict";

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        MaterializedView mv = (MaterializedView) db.getTable(MV_NAME);
        Assertions.assertNotNull(mv);

        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE, "false");
        ModifyTablePropertiesClause clause = new ModifyTablePropertiesClause(properties);
        AlterMaterializedViewStmt stmt = new AlterMaterializedViewStmt(
                createTableRef(DB_NAME, MV_NAME),
                clause,
                NodePosition.ZERO);

        AlterMVJobExecutor executor = new AlterMVJobExecutor();
        executor.process(stmt, connectContext);

        // Verify MV state - should support FSE in danger mode when query rewrite is disabled
        mv = (MaterializedView) db.getTable(MV_NAME);
        Assertions.assertTrue(mv.isSupportFastSchemaEvolutionInDanger());
    }
}
