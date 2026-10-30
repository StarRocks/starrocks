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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/OlapTableTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.alter.SchemaChangeJobV2;
import com.starrocks.backup.RestoreJob;
import com.starrocks.backup.mv.MvRestoreContext;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UnitTestUtil;
import com.starrocks.persist.OriginStatementInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.LiteralExprFactory;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TPrimaryKeyEncodingType;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.threeten.extra.PeriodDuration;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OlapTableTest {
    private static boolean definitionJournalStarted;

    @AfterAll
    public static void tearDownDefinitionJournal() {
        if (definitionJournalStarted) {
            UtFrameUtils.tearDownForPersisTest();
        }
    }


    @Test
    public void testShadowValueWideningPreservesDefinition() throws Exception {
        OlapTable table = definitionTable("definition_widen");
        MaterializedIndexMeta original = table.getIndexMetaByMetaId(100);
        List<Column> target = original.getSchema().stream().map(Column::new).collect(Collectors.toList());
        Column value = target.get(1);
        value.setName("__starrocks_shadow_projected");
        value.setType(IntegerType.BIGINT);
        value.setDefineExpr(null);
        MaterializedIndexMeta shadow = new MaterializedIndexMeta(101, target, 1, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS, null);
        new DefinitionSchemaChangeJob().reconstruct(original, shadow);
        Assertions.assertEquals(original.getDbId(), shadow.getDbId());
        Assertions.assertEquals(73, shadow.getSyncMvRequiredTxnId());
        MaterializedIndexMeta recovered = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(shadow), MaterializedIndexMeta.class);
        table.analyzeRollupIndexMeta(recovered);
        Assertions.assertEquals(IntegerType.BIGINT, recovered.getSchema().get(1).getType());
        Assertions.assertEquals(original.getSchema().get(1).getUniqueId(), recovered.getSchema().get(1).getUniqueId());
        Assertions.assertNotNull(recovered.getSchema().get(1).getDefineExpr());
        Assertions.assertEquals(AstToSQLBuilder.toSQL(original.getSchema().get(1).getDefineExpr()),
                AstToSQLBuilder.toSQL(recovered.getSchema().get(1).getDefineExpr()));
        Assertions.assertNotNull(recovered.getWhereClause());
    }

    @Test
    public void testDropProjectionValuePreservesDefinitionAfterReload() throws Exception {
        OlapTable table = definitionTable("definition_drop");
        MaterializedIndexMeta original = table.getIndexMetaByMetaId(100);
        int keyUid = original.getSchema().get(0).getUniqueId();
        int valueUid = original.getSchema().get(1).getUniqueId();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("definition_catalog");
        ConnectContext context = UtFrameUtils.createDefaultCtx();
        AlterTableStmt drop = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "ALTER TABLE definition_catalog.definition_drop DROP COLUMN dropped FROM definition_mv", context);
        DDLStmtExecutor.execute(drop, context);
        long changedSchemaId = table.getIndexMetaByMetaId(100).getSchemaId();
        Assertions.assertNotEquals(original.getSchemaId(), changedSchemaId);
        OlapTable recovered = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(table), OlapTable.class);
        recovered.onReload();
        MaterializedIndexMeta meta = recovered.getIndexMetaByMetaId(100);
        Assertions.assertEquals(2, meta.getSchema().size());
        Assertions.assertEquals(keyUid, meta.getSchema().get(0).getUniqueId());
        Assertions.assertEquals(valueUid, meta.getSchema().get(1).getUniqueId());
        Assertions.assertEquals(73, meta.getSyncMvRequiredTxnId());
        Assertions.assertEquals(changedSchemaId, meta.getSchemaId());
        Assertions.assertNotNull(meta.getWhereClause());
        Assertions.assertEquals(AstToSQLBuilder.toSQL(original.getSchema().get(1).getDefineExpr()),
                AstToSQLBuilder.toSQL(meta.getSchema().get(1).getDefineExpr()));
        db.dropTable(table.getName());
        db.registerTableUnlocked(recovered);
        Assertions.assertNotNull(UtFrameUtils.getPlanAndFragment(UtFrameUtils.createDefaultCtx(),
                "INSERT INTO definition_catalog.definition_drop VALUES (1, 2)").second);
    }

    @Test
    public void testAddedSourceColumnSurvivesCatalogUpdate() throws Exception {
        OlapTable table = definitionTable("definition_add");
        Column added = new Column("new_source", IntegerType.INT, false, null, "7", "");
        added.setUniqueId(table.incAndGetMaxColUniqueId());
        List<Column> newBase = new ArrayList<>(table.getBaseSchema());
        newBase.add(added);
        List<Column> newMv = new ArrayList<>(table.getIndexMetaByMetaId(100).getSchema());
        newMv.add(new Column(added));
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("definition_catalog");
        GlobalStateMgr.getCurrentState().getSchemaChangeHandler().applyFastSchemaEvolutionMetaChange(
                db, table, Map.of(table.getBaseIndexMetaId(), newBase, 100L, newMv), table.getCopiedIndexes(),
                GlobalStateMgr.getCurrentState().getNextId(), Map.of(table.getBaseIndexMetaId(), 201L, 100L, 202L));
        OlapTable recovered = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(table), OlapTable.class);
        recovered.onReload();
        Column recoveredSource = recovered.getIndexMetaByMetaId(100).getSchema().get(3);
        Assertions.assertEquals("new_source", recoveredSource.getName());
        Assertions.assertNull(recoveredSource.getDefineExpr(), "the default must not become a permanent MV expression");
        Assertions.assertEquals("7", recoveredSource.getDefaultValue());
        Assertions.assertEquals(added.getUniqueId(), recoveredSource.getUniqueId());
        Assertions.assertEquals(73, recovered.getIndexMetaByMetaId(100).getSyncMvRequiredTxnId());
    }

    @Test
    public void testVisibleMvKeepsBaseTypesWhileShadowTargetUsesShadowTypes() throws Exception {
        OlapTable table = definitionTable("definition_scope");
        MaterializedIndexMeta original = table.getIndexMetaByMetaId(100);
        original.getSchema().get(1).setDefineExpr(SqlParser.parseSqlToExpr("ifnull(v, cast(0 as int))",
                SqlModeHelper.MODE_DEFAULT));
        original.getSchema().get(2).setDefineExpr(SqlParser.parseSqlToExpr("abs(v)", SqlModeHelper.MODE_DEFAULT));
        original.getSchema().get(2).setType(IntegerType.BIGINT);
        original.setSyncMvDefinition(SyncMaterializedViewDefinition.capture(original.getSchema(),
                SqlParser.parseSqlToExpr("k > 0", SqlModeHelper.MODE_DEFAULT)));
        table.analyzeRollupIndexMeta(original);
        assertDefinitionSourceType(original, IntegerType.INT);

        List<Column> targetBase = table.getBaseSchema().stream().map(Column::new).collect(Collectors.toList());
        targetBase.get(1).setName("__starrocks_shadow_v");
        targetBase.get(1).setType(IntegerType.BIGINT);
        table.setIndexMeta(102, "__starrocks_shadow_" + table.getName(), targetBase, 1, 0,
                (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS, null);
        List<Column> target = original.getSchema().stream().map(Column::new).collect(Collectors.toList());
        target.get(1).setName("__starrocks_shadow_projected");
        target.get(1).setType(IntegerType.BIGINT);
        target.get(2).setType(IntegerType.LARGEINT);
        table.setIndexMeta(101, "__starrocks_shadow_definition_mv", target, 1, 0,
                (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS, null);
        new DefinitionSchemaChangeJob().reconstruct(original, table.getIndexMetaByMetaId(101));

        List<Column> legacySchema = original.getSchema().stream().map(Column::new).collect(Collectors.toList());
        legacySchema.get(1).setName("mv_projected");
        legacySchema.get(2).setName("mv_dropped");
        table.setIndexMeta(103, "legacy_mv", legacySchema,
                0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS,
                new OriginStatementInfo("CREATE MATERIALIZED VIEW legacy_mv AS SELECT k, " +
                        "ifnull(v, cast(0 as int)) AS projected, abs(v) AS dropped " +
                        "FROM definition_scope WHERE k > 0", 0));
        OlapTable recovered = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(table), OlapTable.class);
        recovered.onReload();
        assertDefinitionSourceType(recovered.getIndexMetaByMetaId(100), IntegerType.INT);
        assertDefinitionSourceType(recovered.getIndexMetaByMetaId(103), IntegerType.INT);
        assertDefinitionSourceType(recovered.getIndexMetaByMetaId(101), IntegerType.BIGINT);

        // Cancellation removes the shadows; it must not leave the surviving expressions rebound to future types.
        recovered.deleteIndexInfo("__starrocks_shadow_definition_mv");
        recovered.deleteIndexInfo("__starrocks_shadow_" + recovered.getName());
        assertDefinitionSourceType(recovered.getIndexMetaByMetaId(100), IntegerType.INT);
        assertDefinitionSourceType(recovered.getIndexMetaByMetaId(103), IntegerType.INT);
        recovered.onReload();
        assertDefinitionSourceType(recovered.getIndexMetaByMetaId(100), IntegerType.INT);
        assertDefinitionSourceType(recovered.getIndexMetaByMetaId(103), IntegerType.INT);
    }

    private static void assertDefinitionSourceType(MaterializedIndexMeta meta, Type expected) {
        Expr expr = meta.getSchema().get(1).getDefineExpr();
        Assertions.assertEquals(expected, expr.getType());
        List<SlotRef> slots = new ArrayList<>();
        expr.collect(SlotRef.class, slots);
        Assertions.assertEquals(1, slots.size());
        Assertions.assertEquals(expected, slots.get(0).getType());
        Expr abs = meta.getSchema().get(2).getDefineExpr();
        Assertions.assertEquals(expected.equals(IntegerType.INT) ? IntegerType.BIGINT : IntegerType.LARGEINT, abs.getType());
        slots.clear();
        abs.collect(SlotRef.class, slots);
        Assertions.assertEquals(1, slots.size());
        Assertions.assertEquals(expected, slots.get(0).getType());
        slots.clear();
        meta.getWhereClause().collect(SlotRef.class, slots);
        Assertions.assertEquals(1, slots.size());
        Assertions.assertEquals(IntegerType.INT, slots.get(0).getType());
    }

    @Test
    public void testStrictRecoveryUsesRegisteredBaseShadowSchema() throws Exception {
        OlapTable table = definitionTable("definition_shadow_source");
        List<Column> targetBase = new ArrayList<>(table.getBaseSchema());
        targetBase.add(new Column("__starrocks_shadow_new_source", IntegerType.INT));
        table.setIndexMeta(102, "__starrocks_shadow_" + table.getName(), targetBase, 1, 0,
                (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS, null);
        MaterializedIndexMeta original = table.getIndexMetaByMetaId(100);
        List<Column> schema = new ArrayList<>(original.getSchema());
        schema.add(new Column("__starrocks_shadow_new_source", IntegerType.INT));
        MaterializedIndexMeta visibleCopy = original.shallowCopy();
        visibleCopy.setSchema(schema);
        Assertions.assertThrows(SemanticException.class, () -> table.analyzeRollupIndexMeta(visibleCopy),
                "a future base-shadow source must not be accepted in an original MV");
        table.setIndexMeta(101, "__starrocks_shadow_definition_mv", schema, 1, 0,
                (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS, null);
        MaterializedIndexMeta meta = table.getIndexMetaByMetaId(101);
        new DefinitionSchemaChangeJob().reconstruct(original, meta);
        table.analyzeRollupIndexMeta(meta);
        Assertions.assertNull(meta.getSchema().get(3).getDefineExpr());
        table.deleteIndexInfo("__starrocks_shadow_" + table.getName());
        Assertions.assertThrows(SemanticException.class, () -> table.analyzeRollupIndexMeta(meta));
    }

    @Test
    public void testNewDefinitionAnalysisFailurePropagatesOnReload() throws Exception {
        OlapTable table = definitionTable("definition_strict");
        MaterializedIndexMeta meta = table.getIndexMetaByMetaId(100);
        meta.getSchema().get(1).setDefineExpr(SqlParser.parseSqlToExpr("missing + 1", SqlModeHelper.MODE_DEFAULT));
        meta.setSyncMvDefinition(SyncMaterializedViewDefinition.capture(meta.getSchema(), null));
        OlapTable recovered = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(table), OlapTable.class);
        Assertions.assertThrows(SemanticException.class, recovered::onReload);
        // Legacy definitions retain the previous best-effort reload policy.
        meta.setSyncMvDefinition(null);
        Assertions.assertDoesNotThrow(table::onReload);
    }

    private static OlapTable definitionTable(String tableName) throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        if (!definitionJournalStarted) {
            UtFrameUtils.setUpForPersistTest();
            definitionJournalStarted = true;
        }
        ConnectContext context = UtFrameUtils.createDefaultCtx();
        new StarRocksAssert(context).withDatabase("definition_catalog").useDatabase("definition_catalog")
                .withTable("CREATE TABLE " + tableName + " (k INT, v INT) DUPLICATE KEY(k) " +
                        "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1','fast_schema_evolution'='true')");
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("definition_catalog");
        OlapTable table = (OlapTable) db.getTable(tableName);
        Column projected = new Column("projected", IntegerType.INT);
        projected.setUniqueId(table.incAndGetMaxColUniqueId());
        projected.setDefineExpr(SqlParser.parseSqlToExpr("v + 1", SqlModeHelper.MODE_DEFAULT));
        Column dropped = new Column("dropped", IntegerType.INT);
        dropped.setUniqueId(table.incAndGetMaxColUniqueId());
        dropped.setDefineExpr(SqlParser.parseSqlToExpr("v + 2", SqlModeHelper.MODE_DEFAULT));
        table.setIndexMeta(100, "definition_mv", Lists.newArrayList(new Column(table.getBaseSchema().get(0)), projected, dropped),
                0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS, null);
        MaterializedIndexMeta meta = table.getIndexMetaByMetaId(100);
        meta.setSortKeyIdxes(Lists.newArrayList(0));
        meta.setSortKeyUniqueIds(Lists.newArrayList(meta.getSchema().get(0).getUniqueId()));
        meta.setDbId(db.getId());
        meta.setSyncMvRequiredTxnId(73);
        Expr where = SqlParser.parseSqlToExpr("v > 0", SqlModeHelper.MODE_DEFAULT);
        meta.setSyncMvDefinition(SyncMaterializedViewDefinition.capture(meta.getSchema(), where));
        table.analyzeRollupIndexMeta(meta);
        for (Partition partition : table.getPartitions()) {
            for (PhysicalPartition physical : partition.getSubPartitions()) {
                MaterializedIndex index = new MaterializedIndex(100, MaterializedIndex.IndexState.NORMAL);
                physical.getLatestBaseIndex().getTablets().forEach(tablet -> index.addTablet(tablet, null, false));
                physical.createRollupIndex(index);
            }
        }
        table.rebuildFullSchema();
        return table;
    }

    private static class DefinitionSchemaChangeJob extends SchemaChangeJobV2 {
        DefinitionSchemaChangeJob() {
            super(0, 0, 0, "definition", 1000);
        }

        public void reconstruct(MaterializedIndexMeta original, MaterializedIndexMeta target) {
            rebuildMaterializedIndexMeta(original, target);
        }
    }

    @Test
    public void testSetIdForRestore() {
        Database db = UnitTestUtil.createDb(1, 2, 3, 4, 5, 6, 7, KeysType.AGG_KEYS);
        List<Table> tables = db.getTables();
        final long id = 0;
        new MockUp<GlobalStateMgr>() {
            @Mock
            long getNextId() {
                return id;
            }
        };

        for (Table table : tables) {
            if (table.getType() != TableType.OLAP) {
                continue;
            }
            RestoreJob restoreJob = new RestoreJob();
            restoreJob.resetIdsForRestore(GlobalStateMgr.getCurrentState(), (OlapTable) table, db, 3, new MvRestoreContext());
        }
    }

    @Test
    public void testTableWithLocalTablet() throws IOException {
        new MockUp<GlobalStateMgr>() {
            @Mock
            int getCurrentStateJournalVersion() {
                return FeConstants.META_VERSION;
            }
        };

        Database db = UnitTestUtil.createDb(1, 2, 3, 4, 5, 6, 7, KeysType.AGG_KEYS);
        List<Table> tables = db.getTables();

        for (Table table : tables) {
            if (table.getType() != TableType.OLAP) {
                continue;
            }
            OlapTable tbl = (OlapTable) table;
            tbl.setIndexes(Lists.newArrayList(new Index("index", Lists.newArrayList(ColumnId.create("col")),
                    IndexDef.IndexType.BITMAP, "xxxxxx")));
            System.out.println("orig table id: " + tbl.getId());
            MvId mvId1 = new MvId(db.getId(), 10L);
            tbl.addRelatedMaterializedView(mvId1);
            MvId mvId2 = new MvId(db.getId(), 20L);
            tbl.addRelatedMaterializedView(mvId2);
            MvId mvId3 = new MvId(db.getId(), 30L);
            tbl.addRelatedMaterializedView(mvId3);
            Assertions.assertEquals(Sets.newHashSet(10L, 20L, 30L),
                    tbl.getRelatedMaterializedViews().stream().map(mvId -> mvId.getId()).collect(Collectors.toSet()));
            tbl.removeRelatedMaterializedView(mvId1);
            tbl.removeRelatedMaterializedView(mvId2);
            Assertions.assertEquals(Sets.newHashSet(30L),
                    tbl.getRelatedMaterializedViews().stream().map(mvId -> mvId.getId()).collect(Collectors.toSet()));
            tbl.removeRelatedMaterializedView(mvId3);
            Assertions.assertEquals(Sets.newHashSet(), tbl.getRelatedMaterializedViews());
        }
    }

    @Test
    public void testCopyOnlyForQuery() {
        OlapTable olapTable = new OlapTable();
        olapTable.setHasDelete();

        OlapTable copied = new OlapTable();
        olapTable.copyOnlyForQuery(copied);

        Assertions.assertEquals(olapTable.hasDelete(), copied.hasDelete());
        Assertions.assertEquals(olapTable.hasForbiddenGlobalDict(), copied.hasForbiddenGlobalDict());
        Assertions.assertEquals(olapTable, copied);
    }

    @Test
    public void testFilePathInfo() {
        OlapTable olapTable = new OlapTable();
        Assertions.assertNull(olapTable.getDefaultFilePathInfo());
        Assertions.assertNull(olapTable.getPartitionFilePathInfo(10));
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        Assertions.assertNull(olapTable.getDefaultFilePathInfo());
        Assertions.assertNull(olapTable.getPartitionFilePathInfo(10));
    }

    @Test
    public void testMVPartitionDurationTimeUintMismatch1() throws AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.DATE), true, null, "", "");
        List<Column> partitionColumns = new LinkedList<Column>();
        partitionColumns.add(k1);

        RangePartitionInfo rangePartitionInfo = new RangePartitionInfo(partitionColumns);
        PeriodDuration duration1 = TimeUtils.parseHumanReadablePeriodOrDuration("2 day");

        PartitionKey p1 = new PartitionKey();
        p1.pushColumn(LiteralExprFactory.create(LocalDate.now().minus(duration1).toString(), DateType.DATE),
                PrimitiveType.DATE);

        PartitionKey p2 = new PartitionKey();
        p2.pushColumn(LiteralExprFactory.create(LocalDate.now().toString(), DateType.DATE), PrimitiveType.DATE);
        rangePartitionInfo.setRange(1, false, Range.openClosed(p1, p2));

        OlapTable olapTable = new OlapTable(1, "test", partitionColumns, KeysType.AGG_KEYS,
                (PartitionInfo) rangePartitionInfo, null);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("25 hour"));

        Partition partition = new Partition(1, 11, "p1", new MaterializedIndex(), null);
        Assertions.assertTrue(olapTable.isEnableFillDataCache(partition));

        new MockUp<Range<PartitionKey>>() {
            @Mock
            boolean isConnected(Range<PartitionKey> range) throws Exception {
                throw new Exception("Error");
            }
        };

        Assertions.assertFalse(olapTable.isEnableFillDataCache(partition));
    }

    @Test
    public void testMVPartitionDurationTimeUintMismatch2() throws AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.DATE), true, null, "", "");
        List<Column> partitionColumns = new LinkedList<Column>();
        partitionColumns.add(k1);

        RangePartitionInfo rangePartitionInfo = new RangePartitionInfo(partitionColumns);
        PeriodDuration duration1 = TimeUtils.parseHumanReadablePeriodOrDuration("4 day");
        PeriodDuration duration2 = TimeUtils.parseHumanReadablePeriodOrDuration("2 day");

        PartitionKey p1 = new PartitionKey();
        p1.pushColumn(LiteralExprFactory.create(LocalDate.now().minus(duration1).toString(), DateType.DATE),
                PrimitiveType.DATE);

        PartitionKey p2 = new PartitionKey();
        p2.pushColumn(LiteralExprFactory.create(LocalDate.now().minus(duration2).toString(), DateType.DATE),
                PrimitiveType.DATE);
        rangePartitionInfo.setRange(1, false, Range.openClosed(p1, p2));

        OlapTable olapTable = new OlapTable(1, "test", partitionColumns, KeysType.AGG_KEYS,
                (PartitionInfo) rangePartitionInfo, null);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("25 hour"));

        Partition partition = new Partition(1, 11, "p1", new MaterializedIndex(), null);
        Assertions.assertFalse(olapTable.isEnableFillDataCache(partition));
    }

    @Test
    public void testNullDataCachePartitionDuration() {
        OlapTable olapTable = new OlapTable();
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        Assertions.assertNull(olapTable.getTableProperty() == null ? null :
                olapTable.getTableProperty().getDataCachePartitionDuration());
    }

    @Test
    public void testListPartitionSupportPeriodDurationTestDateColumn() throws AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.VARCHAR), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.DATE), true, null, "", "");
        Column k3 = new Column("k3", new ScalarType(PrimitiveType.DATETIME), true, null, "", "");
        List<Column> partitionColumns = new LinkedList<Column>();
        partitionColumns.add(k1);
        partitionColumns.add(k2);
        partitionColumns.add(k3);

        ListPartitionInfo listPartitionInfo = new ListPartitionInfo(PartitionType.LIST, partitionColumns);
        List<String> multiValues1 = new ArrayList<>(Arrays.asList("abcd", "2023-01-01", "2024-01-01"));
        List<List<String>> multiValuesList1 = new ArrayList<>(Arrays.asList(multiValues1));
        listPartitionInfo.setMultiValues(1L, multiValuesList1);
        OlapTable olapTable = new OlapTable(1L, "tb1", partitionColumns, null, (PartitionInfo) listPartitionInfo, null);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        Partition partition1 = new Partition(1L, 11, "p1", new MaterializedIndex(), null);

        // Datacache.partition_duration is not set, cache is valid
        Assertions.assertTrue(olapTable.isEnableFillDataCache(partition1));

        // cache is invalid, because we only take k2 into consideration
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("25 hour"));
        Assertions.assertFalse(olapTable.isEnableFillDataCache(partition1));

        List<String> multiValues2 = new ArrayList<>(Arrays.asList("abcd", LocalDate.now().toString(), "2024-01-01"));
        List<List<String>> multiValuesList2 = new ArrayList<>(Arrays.asList(multiValues2));
        listPartitionInfo.setMultiValues(2L, multiValuesList2);
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("28 hour"));
        Partition partition2 = new Partition(2L, 21, "p2", new MaterializedIndex(), null);

        // cache is valid
        Assertions.assertTrue(olapTable.isEnableFillDataCache(partition2));

        new MockUp<DateUtils>() {
            @Mock
            LocalDateTime parseDatTimeString(String text) throws Exception {
                throw new AnalysisException("Error");
            }
        };
        Assertions.assertFalse(olapTable.isEnableFillDataCache(partition1));
    }

    @Test
    public void testListPartitionSupportPeriodDurationTestSingleDateColumn() throws AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.DATE), true, null, "", "");
        List<Column> partitionColumns = new LinkedList<Column>();
        partitionColumns.add(k1);

        ListPartitionInfo listPartitionInfo = new ListPartitionInfo(PartitionType.LIST, partitionColumns);
        List<String> multiValues1 = new ArrayList<>(Arrays.asList("2023-01-01"));
        List<List<String>> multiValuesList1 = new ArrayList<>(Arrays.asList(multiValues1));
        listPartitionInfo.setMultiValues(1L, multiValuesList1);
        OlapTable olapTable = new OlapTable(1L, "tb1", partitionColumns, null, (PartitionInfo) listPartitionInfo, null);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        Partition partition1 = new Partition(1L, 11, "p1", new MaterializedIndex(), null);

        // Datacache.partition_duration is not set, cache is valid
        Assertions.assertTrue(olapTable.isEnableFillDataCache(partition1));

        // cache is invalid
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("25 hour"));
        Assertions.assertFalse(olapTable.isEnableFillDataCache(partition1));

        List<String> multiValues2 = new ArrayList<>(Arrays.asList(LocalDate.now().toString()));
        List<List<String>> multiValuesList2 = new ArrayList<>(Arrays.asList(multiValues2));
        listPartitionInfo.setMultiValues(2L, multiValuesList2);
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("28  hour"));
        Partition partition2 = new Partition(2L, 21, "p2", new MaterializedIndex(), null);

        // cache is valid
        Assertions.assertTrue(olapTable.isEnableFillDataCache(partition2));

        new MockUp<DateUtils>() {
            @Mock
            LocalDateTime parseDatTimeString(String text) throws Exception {
                throw new AnalysisException("Error");
            }
        };
        Assertions.assertFalse(olapTable.isEnableFillDataCache(partition1));
    }

    @Test
    public void testListPartitionSupportPeriodDurationTestIdToValues() throws AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.DATE), true, null, "", "");
        List<Column> partitionColumns = new LinkedList<Column>();
        partitionColumns.add(k1);

        ListPartitionInfo listPartitionInfo = new ListPartitionInfo(PartitionType.LIST, partitionColumns);
        List<String> values1 = new ArrayList<>(Arrays.asList("2023-01-01", "2023-10-01"));
        listPartitionInfo.setValues(1L, values1);
        OlapTable olapTable = new OlapTable(1L, "tb1", partitionColumns, null, (PartitionInfo) listPartitionInfo, null);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        Partition partition1 = new Partition(1L, 11, "p1", new MaterializedIndex(), null);

        // Datacache.partition_duration is not set, cache is valid
        Assertions.assertTrue(olapTable.isEnableFillDataCache(partition1));

        // cache is invalid
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("25 hour"));
        Assertions.assertFalse(olapTable.isEnableFillDataCache(partition1));

        List<String> values2 = new ArrayList<>(Arrays.asList(LocalDate.now().toString()));
        listPartitionInfo.setValues(2L, values2);
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("28 hour"));
        Partition partition2 = new Partition(2L, 21, "p2", new MaterializedIndex(), null);

        // cache is valid
        Assertions.assertTrue(olapTable.isEnableFillDataCache(partition2));

        new MockUp<DateUtils>() {
            @Mock
            LocalDateTime parseDatTimeString(String text) throws Exception {
                throw new AnalysisException("Error");
            }
        };
        Assertions.assertFalse(olapTable.isEnableFillDataCache(partition1));
    }

    @Test
    public void testListPartitionSupportPeriodDurationTestDateTimeColumn() throws AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.VARCHAR), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.DATETIME), true, null, "", "");
        Column k3 = new Column("k3", new ScalarType(PrimitiveType.DATE), true, null, "", "");
        List<Column> partitionColumns = new LinkedList<Column>();
        partitionColumns.add(k1);
        partitionColumns.add(k2);
        partitionColumns.add(k3);

        ListPartitionInfo listPartitionInfo = new ListPartitionInfo(PartitionType.LIST, partitionColumns);
        List<String> multiValues1 = new ArrayList<>(Arrays.asList("abcd", "2023-01-01 10:00:00", "2024-01-01"));
        List<List<String>> multiValuesList1 = new ArrayList<>(Arrays.asList(multiValues1));
        listPartitionInfo.setMultiValues(1L, multiValuesList1);
        OlapTable olapTable = new OlapTable(1L, "tb1", partitionColumns, null, (PartitionInfo) listPartitionInfo, null);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        Partition partition = new Partition(1L, 11, "p1", new MaterializedIndex(), null);

        // Datacache.partition_duration is not set, cache is valid
        Assertions.assertTrue(olapTable.isEnableFillDataCache(partition));

        // cache is invalid, because we only take k2 into consideration
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("25 hour"));
        Assertions.assertFalse(olapTable.isEnableFillDataCache(partition));

        List<String> multiValues2 = new ArrayList<>(Arrays.asList("abcd",
                DateUtils.formatDateTimeUnix(LocalDateTime.now()), "2024-01-01"));
        List<List<String>> multiValuesList2 = new ArrayList<>(Arrays.asList(multiValues2));
        listPartitionInfo.setMultiValues(2L, multiValuesList2);
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("28 hour"));
        Partition partition2 = new Partition(2L, 21, "p2", new MaterializedIndex(), null);

        // cache is valid
        Assertions.assertTrue(olapTable.isEnableFillDataCache(partition2));
    }

    @Test
    public void testListPartitionSupportPeriodDurationTestNoneDateTypeColumn() throws AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.VARCHAR), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.INT), true, null, "", "");
        Column k3 = new Column("k3", new ScalarType(PrimitiveType.DOUBLE), true, null, "", "");
        List<Column> partitionColumns = new LinkedList<Column>();
        partitionColumns.add(k1);
        partitionColumns.add(k2);
        partitionColumns.add(k3);

        ListPartitionInfo listPartitionInfo = new ListPartitionInfo(PartitionType.LIST, partitionColumns);
        List<String> multiValues1 = new ArrayList<>(Arrays.asList("abcd", "1", "1.1"));
        List<List<String>> multiValuesList1 = new ArrayList<>(Arrays.asList(multiValues1));
        listPartitionInfo.setMultiValues(1L, multiValuesList1);
        OlapTable olapTable = new OlapTable(1L, "tb1", partitionColumns, null, (PartitionInfo) listPartitionInfo, null);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        Partition partition1 = new Partition(1L, 11, "p1", new MaterializedIndex(), null);
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("25 hour"));

        // cache is valid
        Assertions.assertTrue(olapTable.isEnableFillDataCache(partition1));
    }

    @Test
    public void testGetIndexesBySchema() {
        List<Index> indexesInTable = Lists.newArrayList();
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.VARCHAR), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.DATETIME), true, null, "", "");
        Column k3 = new Column("k3", new ScalarType(PrimitiveType.DATE), true, null, "", "");
        List<Column> schema = new LinkedList<Column>();
        schema.add(k1);
        schema.add(k2);
        schema.add(k3);

        Index index1 = new Index(1L, "index1", Lists.newArrayList(ColumnId.create("k1"), ColumnId.create("k2")),
                IndexDef.IndexType.BITMAP, "comment", null);

        Index index2 = new Index(2L, "index2", Lists.newArrayList(ColumnId.create("k2"), ColumnId.create("k3")),
                IndexDef.IndexType.BITMAP, "comment", null);

        Index index3 = new Index(3L, "index3", Lists.newArrayList(ColumnId.create("k4")), IndexDef.IndexType.BITMAP,
                "comment", null);
        indexesInTable.add(index1);
        indexesInTable.add(index2);
        indexesInTable.add(index3);

        List<Index> result = OlapTable.getIndexesBySchema(indexesInTable, schema);
        Assertions.assertTrue(result.size() == 2);
        Assertions.assertTrue(result.get(0).getIndexName().equals("index1"));
        Assertions.assertTrue(result.get(1).getIndexName().equals("index2"));
    }

    @Test
    public void testGetUniquePropertiesWithFlatJsonConfig() {
        // Test case 1: Flat JSON enabled with all properties set
        OlapTable table1 = new OlapTable();
        TableProperty tableProperty1 = new TableProperty();
        Map<String, String> properties1 = new HashMap<>();
        properties1.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, "true");
        properties1.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, "0.1");
        properties1.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR, "0.8");
        properties1.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX, "50");
        tableProperty1.modifyTableProperties(properties1);
        table1.setTableProperty(tableProperty1);

        Map<String, String> result1 = table1.getUniqueProperties();
        Assertions.assertEquals("true", result1.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE));
        Assertions.assertEquals("0.1", result1.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR));
        Assertions.assertEquals("0.8", result1.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR));
        Assertions.assertEquals("50", result1.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX));

        // Test case 2: Flat JSON enabled but only some properties set
        OlapTable table2 = new OlapTable();
        TableProperty tableProperty2 = new TableProperty();
        Map<String, String> properties2 = new HashMap<>();
        properties2.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, "true");
        properties2.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, "0.2");
        // Don't set sparsity factor and column max
        tableProperty2.modifyTableProperties(properties2);
        table2.setTableProperty(tableProperty2);

        Map<String, String> result2 = table2.getUniqueProperties();
        Assertions.assertEquals("true", result2.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE));
        Assertions.assertEquals("0.2", result2.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR));
        Assertions.assertNull(result2.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR));
        Assertions.assertNull(result2.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX));

        // Test case 3: Flat JSON disabled - other properties should not be included
        OlapTable table3 = new OlapTable();
        TableProperty tableProperty3 = new TableProperty();
        Map<String, String> properties3 = new HashMap<>();
        properties3.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, "false");
        properties3.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, "0.3");
        properties3.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR, "0.9");
        properties3.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX, "100");
        tableProperty3.modifyTableProperties(properties3);
        table3.setTableProperty(tableProperty3);

        Map<String, String> result3 = table3.getUniqueProperties();
        Assertions.assertEquals("false", result3.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE));
        Assertions.assertNull(result3.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR));
        Assertions.assertNull(result3.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR));
        Assertions.assertNull(result3.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX));

        // Test case 4: No flat JSON properties set
        OlapTable table4 = new OlapTable();
        TableProperty tableProperty4 = new TableProperty();
        table4.setTableProperty(tableProperty4);

        Map<String, String> result4 = table4.getUniqueProperties();
        Assertions.assertNull(result4.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE));
        Assertions.assertNull(result4.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR));
        Assertions.assertNull(result4.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR));
        Assertions.assertNull(result4.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX));

        // Test case 5: Flat JSON enabled with null/empty values
        OlapTable table5 = new OlapTable();
        TableProperty tableProperty5 = new TableProperty();
        Map<String, String> properties5 = new HashMap<>();
        properties5.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, "true");
        properties5.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, "");
        properties5.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR, null);
        tableProperty5.modifyTableProperties(properties5);
        table5.setTableProperty(tableProperty5);

        Map<String, String> result5 = table5.getUniqueProperties();
        Assertions.assertEquals("true", result5.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE));
        Assertions.assertNull(result5.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR));
        Assertions.assertNull(result5.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR));
    }

    @Test
    public void testIsRangeDistribution() throws AnalysisException {
        // Test case 1: Table with RangeDistributionInfo
        
        // Create columns for the table
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("k1", IntegerType.INT));
        columns.add(new Column("k2", VarcharType.VARCHAR));
        columns.add(new Column("v1", FloatType.DOUBLE));
        
        // Create range distribution info
        List<Column> partitionColumns = Lists.newArrayList(columns.get(0)); // k1 as partition column
        RangeDistributionInfo rangeDistInfo = new RangeDistributionInfo();
        
        // Create hash distribution info for comparison
        HashDistributionInfo hashDistInfo = new HashDistributionInfo(1, Lists.newArrayList(columns.get(0)));
        
        // Create range partition info
        RangePartitionInfo rangePartitionInfo = new RangePartitionInfo(partitionColumns);
        
        // Test with range distribution table
        OlapTable rangeTable = new OlapTable(1L, "range_table", columns, 
                KeysType.AGG_KEYS, rangePartitionInfo, rangeDistInfo);
        Assertions.assertTrue(rangeTable.isRangeDistribution());
        
        // Test with hash distribution table
        OlapTable hashTable = new OlapTable(2L, "hash_table", columns,
                KeysType.AGG_KEYS, rangePartitionInfo, hashDistInfo);
        Assertions.assertFalse(hashTable.isRangeDistribution());
        
        // Test with random distribution table
        RandomDistributionInfo randomDistInfo = new RandomDistributionInfo(10);
        OlapTable randomTable = new OlapTable(3L, "random_table", columns,
                KeysType.AGG_KEYS, rangePartitionInfo, randomDistInfo);
        Assertions.assertFalse(randomTable.isRangeDistribution());
    }

    @Test
    public void testIsRangeDistributionWithNullDistribution() {
        // Test edge case: table with null distribution info (should not happen in practice)
        OlapTable tableWithNullDist = new OlapTable();
        // Since defaultDistributionInfo is null by default
        Assertions.assertFalse(tableWithNullDist.isRangeDistribution());
    }

    @Test 
    public void testIsRangeDistributionAfterSetDistribution() {
        OlapTable table = new OlapTable();
        
        // Initially no distribution
        Assertions.assertFalse(table.isRangeDistribution());
        
        // Set range distribution
        List<Column> columns = Lists.newArrayList(new Column("k1", IntegerType.INT));
        RangeDistributionInfo rangeDistInfo = new RangeDistributionInfo();
        table.setDefaultDistributionInfo(rangeDistInfo);
        Assertions.assertTrue(table.isRangeDistribution());
        
        // Change to hash distribution
        HashDistributionInfo hashDistInfo = new HashDistributionInfo(1, columns);
        table.setDefaultDistributionInfo(hashDistInfo);
        Assertions.assertFalse(table.isRangeDistribution());
    }

    @Test
    public void testGetPrimaryKeyEncodingType() {
        OlapTable nonPrimaryKeyTable = new OlapTable() {
            @Override
            public KeysType getKeysType() {
                return KeysType.AGG_KEYS;
            }
        };
        Assertions.assertEquals(TPrimaryKeyEncodingType.PK_ENCODING_TYPE_NONE,
                nonPrimaryKeyTable.getPrimaryKeyEncodingType());

        OlapTable primaryKeyNonCloudTable = new OlapTable() {
            @Override
            public KeysType getKeysType() {
                return KeysType.PRIMARY_KEYS;
            }

            @Override
            public boolean isCloudNativeTableOrMaterializedView() {
                return false;
            }
        };
        Assertions.assertEquals(TPrimaryKeyEncodingType.PK_ENCODING_TYPE_V1,
                primaryKeyNonCloudTable.getPrimaryKeyEncodingType());

        OlapTable primaryKeyCloudHashTable = new OlapTable() {
            @Override
            public KeysType getKeysType() {
                return KeysType.PRIMARY_KEYS;
            }

            @Override
            public boolean isCloudNativeTableOrMaterializedView() {
                return true;
            }

            @Override
            public boolean isRangeDistribution() {
                return false;
            }
        };
        Assertions.assertEquals(TPrimaryKeyEncodingType.PK_ENCODING_TYPE_V1,
                primaryKeyCloudHashTable.getPrimaryKeyEncodingType());

        OlapTable primaryKeyCloudRangeTable = new OlapTable() {
            @Override
            public KeysType getKeysType() {
                return KeysType.PRIMARY_KEYS;
            }

            @Override
            public boolean isCloudNativeTableOrMaterializedView() {
                return true;
            }

            @Override
            public boolean isRangeDistribution() {
                return true;
            }
        };
        Assertions.assertEquals(TPrimaryKeyEncodingType.PK_ENCODING_TYPE_V2,
                primaryKeyCloudRangeTable.getPrimaryKeyEncodingType());

        // Test persisted primaryKeyEncodingType overrides computed value
        OlapTable cloudHashTableWithPersistedV2 = new OlapTable() {
            @Override
            public KeysType getKeysType() {
                return KeysType.PRIMARY_KEYS;
            }

            @Override
            public boolean isCloudNativeTableOrMaterializedView() {
                return true;
            }

            @Override
            public boolean isRangeDistribution() {
                return false;
            }
        };
        // Without persisted value, cloud hash table returns V1 (legacy fallback)
        Assertions.assertEquals(TPrimaryKeyEncodingType.PK_ENCODING_TYPE_V1,
                cloudHashTableWithPersistedV2.getPrimaryKeyEncodingType());
        // After setting persisted value to V2, it should return V2
        cloudHashTableWithPersistedV2.setPrimaryKeyEncodingType(TPrimaryKeyEncodingType.PK_ENCODING_TYPE_V2);
        Assertions.assertEquals(TPrimaryKeyEncodingType.PK_ENCODING_TYPE_V2,
                cloudHashTableWithPersistedV2.getPrimaryKeyEncodingType());

        // Non-PK table should always return NONE regardless of persisted value
        nonPrimaryKeyTable.setPrimaryKeyEncodingType(TPrimaryKeyEncodingType.PK_ENCODING_TYPE_V2);
        Assertions.assertEquals(TPrimaryKeyEncodingType.PK_ENCODING_TYPE_NONE,
                nonPrimaryKeyTable.getPrimaryKeyEncodingType());
    }

}
