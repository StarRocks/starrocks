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

package com.starrocks.persist.image;

import com.staros.proto.FilePathInfo;
import com.starrocks.catalog.ColocateRangeUtils;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangeDistributionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.SyncMaterializedViewDefinition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Pair;
import com.starrocks.common.Range;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.vacuum.AutovacuumDaemon;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.sql.optimizer.statistics.IMinMaxStatsMgr;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

/**
 * LocalMetastore written and read through the image v3 sections delivers the same databases and tables as the v2
 * block.
 */
public class LocalMetastoreImageV3Test {
    private static final String DB = "image_v3_metastore";

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @TempDir
    Path tempDir;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        // The statistics daemon creates its database and tables in the background; create them now so that the
        // metastore does not change between the counting, the v2 save and the v3 save below.
        new StatisticsMetaManager().createStatisticsTablesForTest();
        starRocksAssert.withDatabase(DB).useDatabase(DB);

        starRocksAssert.withTable("CREATE TABLE range_tbl (dt date, k1 int, k2 varchar(20), v1 bigint)"
                + " DUPLICATE KEY(dt, k1)"
                + " PARTITION BY RANGE(dt) (PARTITION p1 VALUES LESS THAN ('2024-01-01'),"
                + " PARTITION p2 VALUES LESS THAN ('2025-01-01'))"
                + " DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES ('replication_num' = '1')");
        starRocksAssert.alterTable("ALTER TABLE range_tbl ADD TEMPORARY PARTITION tp1"
                + " VALUES [('2025-01-01'), ('2026-01-01'))");
        starRocksAssert.withTable("CREATE TABLE list_tbl (id bigint, city varchar(20), v1 int)"
                + " DUPLICATE KEY(id, city)"
                + " PARTITION BY LIST (city) (PARTITION p_bj VALUES IN ('beijing'),"
                + " PARTITION p_sh VALUES IN ('shanghai', 'suzhou'))"
                + " DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES ('replication_num' = '1')");
        starRocksAssert.withTable("CREATE TABLE pk_tbl (id bigint NOT NULL AUTO_INCREMENT, name varchar(32),"
                + " score int)"
                + " PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES ('replication_num' = '1')");
        // DEFERRED MANUAL: an automatic refresh would add partitions to the view in the background while the test
        // takes its two snapshots.
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv1 PARTITION BY dt"
                + " DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL PROPERTIES ('replication_num' = '1')"
                + " AS SELECT dt, k1, sum(v1) AS total FROM range_tbl GROUP BY dt, k1");
        starRocksAssert.withView("CREATE VIEW v1 AS SELECT k1, k2 FROM range_tbl");
        starRocksAssert.withTable("CREATE EXTERNAL TABLE mysql_tbl (k1 int, k2 varchar(10)) ENGINE = mysql"
                + " PROPERTIES ('host' = '127.0.0.1', 'port' = '3306', 'user' = 'u', 'password' = 'p',"
                + " 'database' = 'd', 'table' = 't')");

        // A user-defined function lives in the database, not in a table.
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        ScalarFunction udf = ScalarFunction.createUdf(new FunctionName(DB, "img_udf"),
                new Type[] {IntegerType.INT, VarcharType.VARCHAR}, IntegerType.BIGINT, false,
                TFunctionBinaryType.SRJAR, "hdfs://host/udf.jar", "com.example.Udf", null, null, null);
        db.addFunction(udf);

        db.registerTableUnlocked(newRangeColocateTable());
        OlapTable range = (OlapTable) db.getTable("range_tbl");
        range.setZstdCompressionColumns(java.util.Set.of(ColumnId.create("k2")),
                java.util.Map.of(ColumnId.create("k2"), 32768));
        range.getTableProperty().getProperties().put("enable_statistic_auto_collect", "false");
        range.getTableProperty().buildEnableStatisticAutoCollect();
        MaterializedIndexMeta meta = range.getIndexMetaByMetaId(range.getBaseIndexMetaId());
        meta.getSchema().get(3).setDefineExpr(SqlParser.parseSqlToExpr("CAST(k1 AS BIGINT) + 2", 0));
        meta.setSyncMvDefinition(SyncMaterializedViewDefinition.capture(meta.getSchema(),
                SqlParser.parseSqlToExpr("k1 > 0", 0)));
        meta.setSyncMvRequiredTxnId(73001);
        // The watermark remains on an index after it becomes visible.
        Partition partition = range.getPartition("p1");
        MaterializedIndex index = partition.getDefaultPhysicalPartition().getBaseIndices().get(0);
        index.setState(MaterializedIndex.IndexState.SHADOW);
        index.setShadowWatershedTxnId(74001);
        index.setState(MaterializedIndex.IndexState.NORMAL);

        // The auto-increment watermark is the manager message of the DB section.
        Table pkTable = db.getTable("pk_tbl");
        GlobalStateMgr.getCurrentState().getLocalMetastore().addOrReplaceAutoIncrementIdByTableId(pkTable.getId(), 1000L);
    }

    private static LocalMetastore newEmptyMetastore() {
        GlobalStateMgr stateMgr = GlobalStateMgr.getCurrentState();
        return new LocalMetastore(stateMgr, stateMgr.getRecycleBin(), stateMgr.getColocateTableIndex());
    }

    private static LakeTable newRangeColocateTable() {
        List<Column> columns = List.of(
                new Column("k1", IntegerType.INT, true, null, null, false, null, "", 1),
                new Column("k2", IntegerType.INT, true, null, null, true, null, "", 2),
                new Column("k3", VarcharType.VARCHAR, true, null, null, true, null, "", 3));
        SinglePartitionInfo partitionInfo = new SinglePartitionInfo();
        LakeTable table = new LakeTable(890001, "range_colocate_tbl", columns, KeysType.DUP_KEYS,
                partitionInfo, new RangeDistributionInfo());
        MaterializedIndex index = new MaterializedIndex(890020, 890020, MaterializedIndex.IndexState.NORMAL,
                PhysicalPartition.INVALID_SHARD_GROUP_ID);
        List<Tuple> prefixes = List.of(
                new Tuple(List.of(Variant.minVariant(IntegerType.INT))),
                new Tuple(List.of(Variant.of(IntegerType.INT, "10"))),
                new Tuple(List.of(Variant.of(IntegerType.INT, "20"))),
                new Tuple(List.of(Variant.maxVariant(IntegerType.INT))));
        for (int i = 0; i + 1 < prefixes.size(); i++) {
            // Use the same expansion as range-colocate tablet creation: k1 is the colocate prefix,
            // so k2 and k3 acquire typed NULL suffixes at both ends of every tablet range.
            Range<Tuple> expanded = ColocateRangeUtils.expandToFullSortKey(
                    Range.gelt(prefixes.get(i), prefixes.get(i + 1)), columns, 1);
            index.addTablet(new LakeTablet(890030 + i, new TabletRange(expanded)), null, false);
        }
        table.setBaseIndexMetaId(index.getMetaId());
        table.setIndexMeta(index.getMetaId(), table.getName(), columns, 0, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.setMaxColUniqueId(3);
        table.setStorageInfo(FilePathInfo.newBuilder().setFullPath("s3://image-v3-bucket/range_colocate_tbl").build(),
                new DataCacheInfo(true, false));
        Partition partition = new Partition(890010, 890011, "p1", index, new RangeDistributionInfo());
        PhysicalPartition physical = partition.getDefaultPhysicalPartition();
        physical.getVacuumState().advance(10, 20, 30, true, 50, Set.of(index.getId()));
        physical.getVacuumState().setMinRetainedVersion(8);
        physical.setLastSuccVacuumVersion(12);
        physical.updateVisibleVersion(100, 1);
        physical.setNextVersion(101);
        partitionInfo.addPartition(partition.getId(), new DataProperty(TStorageMedium.HDD), (short) 1);
        table.addPartition(partition);
        return table;
    }

    private static void assertRangeColocateTable(Table table) {
        Assertions.assertInstanceOf(LakeTable.class, table);
        OlapTable olap = (OlapTable) table;
        PhysicalPartition physical = olap.getPartition("p1").getDefaultPhysicalPartition();
        Assertions.assertEquals(12, physical.getLastSuccVacuumVersion());
        Assertions.assertEquals(10, physical.getVacuumState().getToDeleteLow());
        Assertions.assertEquals(20, physical.getVacuumState().getToDeleteHigh());
        Assertions.assertEquals(30, physical.getVacuumState().getNextProposeStartVersion());
        Assertions.assertEquals(50, physical.getVacuumState().getPassStartVersion());
        Assertions.assertEquals(Set.of(890020L), physical.getVacuumState().getPassStartIndexIds());
        Assertions.assertEquals(8, physical.getVacuumState().getMinRetainedVersion());
        Assertions.assertTrue(new AutovacuumDaemon().shouldVacuum(physical));
        MaterializedIndex index = olap.getPartition("p1").getDefaultPhysicalPartition().getBaseIndices().get(0);
        List<Tablet> tablets = index.getTablets();
        Assertions.assertEquals(3, tablets.size());
        Assertions.assertEquals("[[MIN, NULL, NULL], [10, NULL, NULL])", tablets.get(0).getRange().toString());
        Assertions.assertEquals("[[10, NULL, NULL], [20, NULL, NULL])", tablets.get(1).getRange().toString());
        Assertions.assertEquals("[[20, NULL, NULL], [MAX, NULL, NULL])", tablets.get(2).getRange().toString());
        Assertions.assertEquals(Variant.minVariant(IntegerType.INT),
                tablets.get(0).getRange().getRange().getLowerBound().getValues().get(0));
        Assertions.assertEquals(Variant.maxVariant(IntegerType.INT),
                tablets.get(2).getRange().getRange().getUpperBound().getValues().get(0));
        for (Tablet tablet : tablets) {
            Assertions.assertSame(tablet, index.getTablet(tablet.getId()));
            Range<Tuple> range = tablet.getRange().getRange();
            Assertions.assertTrue(range.isLowerBoundIncluded());
            Assertions.assertFalse(range.isUpperBoundIncluded());
            for (Tuple bound : List.of(range.getLowerBound(), range.getUpperBound())) {
                Assertions.assertEquals(Variant.nullVariant(IntegerType.INT), bound.getValues().get(1));
                Assertions.assertEquals(Variant.nullVariant(VarcharType.VARCHAR), bound.getValues().get(2));
            }
        }
        for (int i = 1; i < tablets.size(); i++) {
            Assertions.assertSame(tablets.get(i - 1).getRange().getRange().getUpperBound(),
                    tablets.get(i).getRange().getRange().getLowerBound());
        }
        Range<Tuple> middle = tablets.get(1).getRange().getRange();
        Assertions.assertTrue(middle.contains(new Tuple(List.of(Variant.of(IntegerType.INT, "10"),
                Variant.of(IntegerType.INT, "7"), Variant.of(VarcharType.VARCHAR, "row")))));
        Assertions.assertFalse(middle.contains(new Tuple(List.of(Variant.of(IntegerType.INT, "20"),
                Variant.of(IntegerType.INT, "7"), Variant.of(VarcharType.VARCHAR, "row")))));
    }

    @Test
    public void testMvPropertyAndReferenceOnlyExpressionsMatchV2() throws Exception {
        MaterializedView live = (MaterializedView) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getDb(DB).getTable("mv1");
        MaterializedView sourceMv = (MaterializedView) GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(live), Table.class);
        sourceMv.getTableProperty().getProperties().put(PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND, "73");
        sourceMv.setMaxMVRewriteStaleness(5);
        sourceMv.setPartitionExprMaps(new LinkedHashMap<>());
        Assertions.assertFalse(sourceMv.getPartitionRefTableExprs().isEmpty());
        Database sourceDb = new Database(sourceMv.getDbId(), DB);
        sourceDb.registerTableUnlocked(sourceMv);
        LocalMetastore source = newEmptyMetastore();
        source.getIdToDb().put(sourceDb.getId(), sourceDb);

        Path file = tempDir.resolve("mv-recovery.image");
        try (OutputStream out = Files.newOutputStream(file);
                ImageWriterV3 writer = new ImageWriterV3(out, 1)) {
            source.save(writer);
            writer.finish();
        }
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        source.save(image.getImageWriter());
        LocalMetastore v2 = newEmptyMetastore();
        v2.load(image.getMetaBlockReader());
        LocalMetastore v3 = newEmptyMetastore();
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            reader.load(v3.getImageV3Loaders());
        }
        for (LocalMetastore loaded : List.of(v2, v3)) {
            MaterializedView mv = (MaterializedView) loaded.getDb(DB).getTable("mv1");
            Assertions.assertEquals(73, mv.getMaxMVRewriteStaleness());
            Assertions.assertEquals(1, mv.getPartitionExprMaps().size());
            Assertions.assertEquals(ExprToSql.toSql(sourceMv.getPartitionRefTableExprs().get(0)),
                    ExprToSql.toSql(mv.getPartitionExprMaps().keySet().iterator().next()));
        }
        Assertions.assertEquals(5, sourceMv.getMaxMVRewriteStaleness());
        Assertions.assertTrue(sourceMv.getPartitionExprMaps().isEmpty());
    }

    @Test
    public void testV3ImageMatchesV2() throws Exception {
        LocalMetastore live = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database liveDb = live.getDb(DB);
        long pkTableId = liveDb.getTable("pk_tbl").getId();

        // reference: the v2 image round trip
        UtFrameUtils.PseudoImage v2Image = new UtFrameUtils.PseudoImage();
        live.save(v2Image.getImageWriter());
        LocalMetastore v2Loaded = newEmptyMetastore();
        v2Loaded.load(v2Image.getMetaBlockReader());
        assertRangeColocateTable(v2Loaded.getDb(DB).getTable("range_colocate_tbl"));

        // the v3 image round trip
        Path file = tempDir.resolve("local_metastore.image");
        try (OutputStream os = Files.newOutputStream(file);
                ImageWriterV3 writer = new ImageWriterV3(os, 1)) {
            live.save(writer);
            writer.finish();
        }
        LocalMetastore v3Loaded = newEmptyMetastore();
        List<IndexEntryPB> sections;
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            sections = reader.sections();
            Assertions.assertEquals(3, sections.size());
            Assertions.assertEquals(SectionId.LOCAL_METASTORE_DB.getValue(), sections.get(0).getType());
            Assertions.assertEquals(SectionId.LOCAL_METASTORE_TABLE.getValue(), sections.get(1).getType());
            Assertions.assertEquals(SectionId.LOCAL_METASTORE_PARTITION.getValue(), sections.get(2).getType());
            // Scope the assertion to partition attachment. Table restoration rebuilds the schema separately.
            List<Pair<SectionId, SectionLoader>> loaders = v3Loaded.getImageV3Loaders().stream()
                    .map(loader -> {
                        if (!SectionId.LOCAL_METASTORE_PARTITION.equals(loader.first)) {
                            return loader;
                        }
                        SectionLoader checked = section -> {
                            try (MockedStatic<IMinMaxStatsMgr> minMaxStats =
                                         Mockito.mockStatic(IMinMaxStatsMgr.class, Mockito.CALLS_REAL_METHODS)) {
                                loader.second.load(section);
                                minMaxStats.verify(() -> IMinMaxStatsMgr.invalidateTable(Mockito.any(OlapTable.class)),
                                        Mockito.never());
                            }
                        };
                        return Pair.create(loader.first, checked);
                    })
                    .toList();
            reader.load(loaders);
        }

        // Every entry of the image was attached to its parent: the loaded metastore accounts for all of them. The
        // system databases are not in the image; the constructor creates them.
        int dbEntries = 0;
        int tableEntries = 0;
        int partitionEntries = 0;
        for (Database db : v3Loaded.getIdToDb().values()) {
            if (db.getId() <= GlobalStateMgr.NEXT_ID_INIT_VALUE) {
                continue;
            }
            dbEntries++;
            for (Table table : db.getTables()) {
                tableEntries++;
                if (table instanceof OlapTable) {
                    partitionEntries += ((OlapTable) table).getPartitions().size()
                            + ((OlapTable) table).getTempPartitions().size();
                }
            }
        }
        Assertions.assertEquals(dbEntries, sections.get(0).getNumEntries());
        Assertions.assertEquals(tableEntries, sections.get(1).getNumEntries());
        Assertions.assertEquals(partitionEntries, sections.get(2).getNumEntries());
        Assertions.assertTrue(dbEntries >= 1);
        Assertions.assertTrue(tableEntries >= 6);

        // The databases (functions included) and every table are what the live metastore holds, through either
        // format; the live objects are the anchor, so a post-load step shared by the two loads cannot hide a fault.
        Database v2Db = v2Loaded.getDb(DB);
        Database v3Db = v3Loaded.getDb(DB);
        assertRangeColocateTable(v3Db.getTable("range_colocate_tbl"));
        Assertions.assertNotNull(v3Db);
        Assertions.assertEquals(liveDb.getId(), v3Db.getId());
        GsonEquivalence.assertEquivalent(liveDb, v2Db, CatalogEquivalenceRules.TYPE_LABELS_ADDED_ON_LOAD,
                Collections.emptySet());
        GsonEquivalence.assertEquivalent(liveDb, v3Db, CatalogEquivalenceRules.TYPE_LABELS_ADDED_ON_LOAD,
                Collections.emptySet());
        GsonEquivalence.assertEquivalent(v2Db, v3Db);
        Assertions.assertEquals(liveDb.getTables().size(), v2Db.getTables().size());
        Assertions.assertEquals(liveDb.getTables().size(), v3Db.getTables().size());
        for (Table liveTable : liveDb.getTables()) {
            Table v2Table = v2Db.getTable(liveTable.getName());
            Table v3Table = v3Db.getTable(liveTable.getName());
            Assertions.assertNotNull(v2Table, liveTable.getName());
            Assertions.assertNotNull(v3Table, liveTable.getName());
            Assertions.assertEquals(liveTable.getClass(), v2Table.getClass(), liveTable.getName());
            Assertions.assertEquals(liveTable.getClass(), v3Table.getClass(), liveTable.getName());
            GsonEquivalence.assertEquivalent(liveTable, v2Table, CatalogEquivalenceRules.LIVE_TABLE_FIELDS,
                    CatalogEquivalenceRules.UNORDERED_PATHS);
            GsonEquivalence.assertEquivalent(liveTable, v3Table, CatalogEquivalenceRules.LIVE_TABLE_FIELDS,
                    CatalogEquivalenceRules.UNORDERED_PATHS);
            GsonEquivalence.assertEquivalent(v2Table, v3Table, CatalogEquivalenceRules.LEGACY_PARTITION_FIELDS,
                    CatalogEquivalenceRules.UNORDERED_PATHS);
        }
        Assertions.assertSame(v3Db, v3Loaded.getDb(v3Db.getId()));

        // The partitions were attached to their tables, temporary ones included.
        OlapTable v3Range = (OlapTable) v3Db.getTable("range_tbl");
        Assertions.assertEquals(2, v3Range.getPartitions().size());
        Assertions.assertEquals(1, v3Range.getTempPartitions().size());
        Assertions.assertNotNull(v3Range.getPartition("p1"));
        Assertions.assertEquals("tp1", v3Range.getTempPartitions().iterator().next().getName());
        Assertions.assertTrue(v3Db.getTable("mv1") instanceof MaterializedView);
        Assertions.assertEquals(((OlapTable) v2Db.getTable("mv1")).getPartitions().size(),
                ((OlapTable) v3Db.getTable("mv1")).getPartitions().size());

        Assertions.assertEquals(java.util.Set.of(ColumnId.create("k2")), v3Range.getZstdCompressionColumnIds());
        Assertions.assertEquals(java.util.Map.of(ColumnId.create("k2"), 32768), v3Range.getZstdCompressionPageSizes());
        Assertions.assertFalse(v3Range.getTableProperty().enableStatisticAutoCollect());
        MaterializedIndexMeta restoredMeta = v3Range.getIndexMetaByMetaId(v3Range.getBaseIndexMetaId());
        Assertions.assertEquals(73001, restoredMeta.getSyncMvRequiredTxnId());
        Assertions.assertNotNull(restoredMeta.getWhereClause());
        Assertions.assertNotNull(restoredMeta.getSchema().get(3).getDefineExpr());
        Assertions.assertEquals(74001, v3Range.getPartition("p1").getDefaultPhysicalPartition()
                .getBaseIndices().get(0).getShadowWatershedTxnId());
        RangePartitionInfo restoredRanges = (RangePartitionInfo) v3Range.getPartitionInfo();
        for (boolean temp : List.of(false, true)) {
            Assertions.assertEquals(temp ? 1 : 2, restoredRanges.getSortedRangeMap(temp).size());
            for (var range : restoredRanges.getIdToRange(temp).values()) {
                Assertions.assertEquals(range, restoredRanges.getAnyIntersectRange(range, temp));
            }
        }
        // Both forward and reverse tablet lookups refer to the same restored objects.
        for (Table table : v3Db.getTables()) {
            if (!(table instanceof OlapTable olap)) {
                continue;
            }
            for (var physical : olap.getAllPhysicalPartitions()) {
                for (var index : physical.getAllMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                    for (var tablet : index.getTablets()) {
                        Assertions.assertNotNull(GlobalStateMgr.getCurrentState().getTabletInvertedIndex()
                                .getTabletMeta(tablet.getId()));
                        if (tablet instanceof LocalTablet local) {
                            for (var replica : local.getImmutableReplicas()) {
                                Assertions.assertSame(replica, GlobalStateMgr.getCurrentState().getTabletInvertedIndex()
                                        .getReplica(tablet.getId(), replica.getBackendId()));
                                Assertions.assertTrue(GlobalStateMgr.getCurrentState().getTabletInvertedIndex()
                                        .getTabletIdsByBackendId(replica.getBackendId()).contains(tablet.getId()));
                            }
                        }
                    }
                }
            }
        }

        // The manager message carried the auto-increment watermark.
        Assertions.assertEquals(Long.valueOf(1000L), v3Loaded.getCurrentAutoIncrementIdByTableId(pkTableId));
        Assertions.assertEquals(v2Loaded.getCurrentAutoIncrementIdByTableId(pkTableId),
                v3Loaded.getCurrentAutoIncrementIdByTableId(pkTableId));
    }

}
