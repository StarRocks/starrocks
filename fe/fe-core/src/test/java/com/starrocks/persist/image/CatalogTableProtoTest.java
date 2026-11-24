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

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.catalog.BoolVariant;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DateVariant;
import com.starrocks.catalog.DecimalVariant;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfoV2;
import com.starrocks.catalog.IntVariant;
import com.starrocks.catalog.LargeIntVariant;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.RangeDistributionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.StringVariant;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.common.Range;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.LakeMaterializedView;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.proto.DataCacheInfoPB;
import com.starrocks.persist.proto.PartitionPB;
import com.starrocks.persist.proto.TablePB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Set;

import static com.starrocks.persist.image.ProtoInstanceAssertions.assertInstances;

/**
 * The table.proto and partition.proto conversions deliver the same objects as a v2 (GSON) image round
 * trip. Tables come from a real cluster where the DDL exists, and from GSON documents in the v2 form otherwise.
 */
public class CatalogTableProtoTest {
    private static final String DB = "image_v3_tables";
    private static final Set<String> LEGACY_PARTITION_FIELDS = CatalogEquivalenceRules.LEGACY_PARTITION_FIELDS;
    private static final Set<String> UNORDERED_PATHS = CatalogEquivalenceRules.UNORDERED_PATHS;

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB).useDatabase(DB);

        starRocksAssert.withTable("CREATE TABLE range_tbl (dt date, k1 int, k2 varchar(20), v1 bigint,"
                + " INDEX idx_k2 (k2) USING BITMAP COMMENT 'bitmap on k2')"
                + " DUPLICATE KEY(dt, k1)"
                + " PARTITION BY RANGE(dt) (PARTITION p1 VALUES LESS THAN ('2024-01-01'),"
                + " PARTITION p2 VALUES LESS THAN ('2025-01-01'))"
                + " DISTRIBUTED BY HASH(k1) BUCKETS 3"
                + " PROPERTIES ('replication_num' = '1', 'bloom_filter_columns' = 'k2',"
                + " 'colocate_with' = 'image_v3_group')");
        starRocksAssert.alterTable("ALTER TABLE range_tbl ADD TEMPORARY PARTITION tp1"
                + " VALUES [('2025-01-01'), ('2026-01-01'))");
        starRocksAssert.withTable("CREATE TABLE list_tbl (id bigint, city varchar(20), v1 int)"
                + " DUPLICATE KEY(id, city)"
                + " PARTITION BY LIST (city) (PARTITION p_bj VALUES IN ('beijing'),"
                + " PARTITION p_sh VALUES IN ('shanghai', 'suzhou'))"
                + " DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES ('replication_num' = '1')");
        starRocksAssert.withTable("CREATE TABLE expr_tbl (dt datetime, k1 int, v1 int)"
                + " DUPLICATE KEY(dt, k1)"
                + " PARTITION BY date_trunc('day', dt)"
                + " DISTRIBUTED BY HASH(k1) BUCKETS 2 PROPERTIES ('replication_num' = '1')");
        starRocksAssert.withTable("CREATE TABLE expr_v2_tbl (busness_date varchar(8), v1 int)"
                + " DUPLICATE KEY(busness_date)"
                + " PARTITION BY RANGE(str2date(busness_date, '%Y%m%d'))"
                + " (PARTITION p2025 VALUES [('0000-01-01'), ('2026-01-01')),"
                + " PARTITION p2026 VALUES [('2026-01-01'), ('2027-01-01')))"
                + " DISTRIBUTED BY RANDOM PROPERTIES ('replication_num' = '1')");
        starRocksAssert.withTable("CREATE TABLE pk_tbl (id bigint NOT NULL AUTO_INCREMENT, name varchar(32),"
                + " score int, created datetime NOT NULL DEFAULT current_timestamp, gen_col bigint AS score * 2)"
                + " PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 2 ORDER BY (name)"
                + " PROPERTIES ('replication_num' = '1', 'enable_persistent_index' = 'true')");
        starRocksAssert.withTable("CREATE TABLE agg_tbl (k1 int, k2 date, v_sum bigint SUM, v_hll hll HLL_UNION,"
                + " v_bitmap bitmap BITMAP_UNION, v_max decimal(18, 4) MAX)"
                + " AGGREGATE KEY(k1, k2) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        // DEFERRED MANUAL: an automatic refresh would add partitions to the view in the background while the test
        // takes its two snapshots.
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv1 PARTITION BY dt"
                + " DISTRIBUTED BY HASH(k1) BUCKETS 2 REFRESH DEFERRED MANUAL"
                + " PROPERTIES ('replication_num' = '1', 'partition_refresh_number' = '2')"
                + " AS SELECT dt, k1, sum(v1) AS total FROM range_tbl GROUP BY dt, k1");
        starRocksAssert.withView("CREATE VIEW v1 (a, b) COMMENT 'image v3 view' AS SELECT k1, k2 FROM range_tbl");
        starRocksAssert.withTable("CREATE EXTERNAL TABLE mysql_tbl (k1 int, k2 varchar(10)) ENGINE = mysql"
                + " PROPERTIES ('host' = '127.0.0.1', 'port' = '3306', 'user' = 'u', 'password' = 'p',"
                + " 'database' = 'd', 'table' = 't')");
    }

    private static Table getTable(String name) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), name);
    }

    /** What a v2 image delivers: a GSON round trip of the whole table, partitions included. */
    private static Table viaGson(Table table) {
        return GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(table, Table.class), Table.class);
    }

    /**
     * What the v3 image delivers: the TABLE entry, then the PARTITION entries attached the way the PARTITION section
     * loader attaches them.
     */
    private static Table viaProto(Table table) throws Exception {
        Table copy = Table.fromProto(TablePB.parseFrom(assertInstances(table.toProto()).toByteArray()));
        if (table instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) table;
            OlapTable olapCopy = (OlapTable) copy;
            for (Partition partition : olapTable.getPartitions()) {
                olapCopy.addPartition(viaProto(partition));
            }
            for (Partition partition : olapTable.getTempPartitions()) {
                olapCopy.addTempPartition(viaProto(partition));
            }
        }
        return copy;
    }

    private static Partition viaProto(Partition partition) throws Exception {
        return Partition.fromProto(PartitionPB.parseFrom(assertInstances(partition.toProto()).toByteArray()));
    }

    private static void assertTableEquivalent(Table table) throws Exception {
        Table expected = viaGson(table);
        Table actual = viaProto(table);
        Assertions.assertEquals(expected.getClass(), actual.getClass(), table.getName());
        GsonEquivalence.assertEquivalent(expected, actual, LEGACY_PARTITION_FIELDS, UNORDERED_PATHS);
    }

    @Test
    public void testOlapTablesMatchGson() throws Exception {
        OlapTable rangeTable = (OlapTable) getTable("range_tbl");
        Assertions.assertTrue(rangeTable.getPartitionInfo() instanceof RangePartitionInfo);
        Assertions.assertEquals(1, rangeTable.getTempPartitions().size());
        assertTableEquivalent(rangeTable);

        OlapTable listTable = (OlapTable) getTable("list_tbl");
        Assertions.assertTrue(listTable.getPartitionInfo() instanceof ListPartitionInfo);
        assertTableEquivalent(listTable);

        OlapTable exprTable = (OlapTable) getTable("expr_tbl");
        Assertions.assertTrue(exprTable.getPartitionInfo() instanceof ExpressionRangePartitionInfo);
        assertTableEquivalent(exprTable);

        OlapTable exprV2Table = (OlapTable) getTable("expr_v2_tbl");
        Assertions.assertTrue(exprV2Table.getPartitionInfo() instanceof ExpressionRangePartitionInfoV2);
        assertTableEquivalent(exprV2Table);

        assertTableEquivalent(getTable("pk_tbl"));
        assertTableEquivalent(getTable("agg_tbl"));
    }

    @Test
    public void testMaterializedViewMatchesGson() throws Exception {
        Table mv = getTable("mv1");
        Assertions.assertTrue(mv instanceof MaterializedView);
        assertTableEquivalent(mv);
        // The transient state is rebuilt the way the GSON hook does it.
        MaterializedView actual = (MaterializedView) viaProto(mv);
        Assertions.assertFalse(actual.getPartitionExprMaps().isEmpty());
        Assertions.assertEquals(1, actual.getPartitionRefTableExprs().size());
    }

    @Test
    public void testPartitionSubtreeMatchesGson() throws Exception {
        OlapTable rangeTable = (OlapTable) getTable("range_tbl");
        for (Partition partition : rangeTable.getPartitions()) {
            Partition expected = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(partition), Partition.class);
            Partition actual = viaProto(partition);
            GsonEquivalence.assertEquivalent(expected, actual, LEGACY_PARTITION_FIELDS, UNORDERED_PATHS);
            PhysicalPartition physicalPartition = actual.getDefaultPhysicalPartition();
            Assertions.assertEquals(partition.getId(), physicalPartition.getParentId());
            Assertions.assertEquals(1, physicalPartition.getBaseIndices().size());
            MaterializedIndex baseIndex = physicalPartition.getBaseIndices().get(0);
            Assertions.assertEquals(3, baseIndex.getTablets().size());
            for (Tablet tablet : baseIndex.getTablets()) {
                Assertions.assertSame(tablet, baseIndex.getTablet(tablet.getId()));
            }
        }
    }

    @Test
    public void testViewAndMysqlTableMatchGson() throws Exception {
        assertTableEquivalent(getTable("v1"));
        assertTableEquivalent(getTable("mysql_tbl"));
    }

    @Test
    public void testExternalTablesFromGsonDocumentsMatchGson() throws Exception {
        // The external table kinds whose DDL needs a live external system: start from the v2 (GSON) document of a
        // cluster table, keep the Table fields and swap in the class-specific ones.
        JsonObject base = GsonUtils.GSON.toJsonTree(getTable("v1"), Table.class).getAsJsonObject();
        for (String viewField : new String[] {"o", "i", "m", "s"}) {
            base.remove(viewField);
        }

        JsonObject es = base.deepCopy();
        es.addProperty("clazz", "EsTable");
        es.addProperty("type", "ELASTICSEARCH");
        JsonObject tableContext = new JsonObject();
        tableContext.addProperty("hosts", "http://127.0.0.1:9200,http://127.0.0.2:9200");
        tableContext.addProperty("userName", "u");
        tableContext.addProperty("passwd", "p");
        tableContext.addProperty("indexName", "idx1");
        tableContext.addProperty("mappingType", "_doc");
        tableContext.addProperty("transport", "http");
        tableContext.addProperty("majorVersion", "7.10.0");
        tableContext.addProperty("enableDocValueScan", "true");
        tableContext.addProperty("enableKeywordSniff", "false");
        tableContext.addProperty("maxDocValueFields", "30");
        tableContext.addProperty("es.nodes.wan.only", "true");
        es.add("tc", tableContext);
        es.add("p", GsonUtils.GSON.toJsonTree(new SinglePartitionInfo()));

        JsonObject hive = base.deepCopy();
        hive.addProperty("clazz", "HiveTable");
        hive.addProperty("type", "HIVE");
        hive.addProperty("dn", "hive_db");
        hive.addProperty("tn", "hive_tbl");
        hive.addProperty("rn", "hive_resource");
        hive.addProperty("tl", "hdfs://ns/warehouse/hive_tbl");
        hive.add("pcn", stringArray("dt"));
        hive.add("dcn", stringArray("k1", "k2"));
        JsonObject hiveProperties = new JsonObject();
        hiveProperties.addProperty("hive.metastore.uris", "thrift://127.0.0.1:9083");
        hive.add("prop", hiveProperties);
        hive.add("sp", new JsonObject());
        hive.addProperty("asj", "{\"type\":\"record\"}");

        JsonObject hudi = base.deepCopy();
        hudi.addProperty("clazz", "HudiTable");
        hudi.addProperty("type", "HUDI");
        hudi.addProperty("dn", "hudi_db");
        hudi.addProperty("tn", "hudi_tbl");
        hudi.addProperty("rn", "hudi_resource");
        hudi.add("pcn", stringArray());
        hudi.add("dcn", stringArray("k1", "k2"));
        JsonObject hudiProperties = new JsonObject();
        hudiProperties.addProperty("hudi.table.type", "COPY_ON_WRITE");
        hudi.add("prop", hudiProperties);

        JsonObject iceberg = base.deepCopy();
        iceberg.addProperty("clazz", "IcebergTable");
        iceberg.addProperty("type", "ICEBERG");
        iceberg.addProperty("dn", "ice_db");
        iceberg.addProperty("tn", "ice_tbl");
        iceberg.addProperty("rn", "ice_resource");
        JsonObject icebergProperties = new JsonObject();
        icebergProperties.addProperty("iceberg.catalog.type", "hive");
        iceberg.add("prop", icebergProperties);

        JsonObject jdbc = base.deepCopy();
        jdbc.addProperty("clazz", "JDBCTable");
        jdbc.addProperty("type", "JDBC");
        jdbc.addProperty("tn", "jdbc_tbl");
        jdbc.addProperty("rn", "jdbc_resource");
        jdbc.addProperty("qt", true);

        JsonObject file = base.deepCopy();
        file.addProperty("clazz", "FileTable");
        file.addProperty("type", "FILE");
        JsonObject fileProperties = new JsonObject();
        fileProperties.addProperty("path", "s3://bucket/dir/");
        fileProperties.addProperty("format", "parquet");
        file.add("fp", fileProperties);

        for (JsonObject document : List.of(es, hive, hudi, iceberg, jdbc, file)) {
            Table expected = GsonUtils.GSON.fromJson(document, Table.class);
            Table actual = viaProto(expected);
            Assertions.assertEquals(expected.getClass(), actual.getClass());
            GsonEquivalence.assertEquivalent(expected, actual, LEGACY_PARTITION_FIELDS, UNORDERED_PATHS);
        }
    }

    @Test
    public void testExternalOlapTableFromGsonDocumentMatchesGson() throws Exception {
        // An OLAP_EXTERNAL table is an OlapTable plus the remote table info.
        JsonObject document = GsonUtils.GSON.toJsonTree(getTable("range_tbl"), Table.class).getAsJsonObject();
        document.addProperty("clazz", "ExternalOlapTable");
        document.addProperty("type", "OLAP_EXTERNAL");
        JsonObject externalInfo = new JsonObject();
        externalInfo.addProperty("ht", "10.0.0.1");
        externalInfo.addProperty("pt", 9020);
        externalInfo.addProperty("us", "root");
        externalInfo.addProperty("pw", "");
        externalInfo.addProperty("db", "remote_db");
        externalInfo.addProperty("tb", "remote_tbl");
        externalInfo.addProperty("dbi", 10001);
        externalInfo.addProperty("tbi", 10002);
        externalInfo.addProperty("tbt", "OLAP");
        document.add("ef", externalInfo);

        Table expected = GsonUtils.GSON.fromJson(document, Table.class);
        Table actual = viaProto(expected);
        Assertions.assertEquals(expected.getClass(), actual.getClass());
        GsonEquivalence.assertEquivalent(expected, actual, LEGACY_PARTITION_FIELDS, UNORDERED_PATHS);
    }

    @Test
    public void testLakeTablesMatchGson() throws Exception {
        LakeTable lakeTable = newLakeTable();
        assertTableEquivalent(lakeTable);
        LakeTable actual = (LakeTable) viaProto(lakeTable);
        Assertions.assertTrue(actual.getDefaultDistributionInfo() instanceof RangeDistributionInfo);
        Assertions.assertNotNull(actual.getTableProperty().getStorageInfo().getFilePathInfo());

        LakeMaterializedView lakeMv = newLakeMaterializedView();
        assertTableEquivalent(lakeMv);
    }

    private static LakeTable newLakeTable() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        long tableId = 700001L;
        long partitionId = 700010L;
        long physicalPartitionId = 700011L;
        long indexId = 700020L;
        List<Column> columns = Lists.newArrayList(
                new Column("c0", IntegerType.BIGINT, true, null, null, false, null, "key", 1),
                new Column("c1", VarcharType.VARCHAR, false, null, null, true, null, "", 2));
        LakeTable lakeTable = new LakeTable(tableId, "lake_range_tbl", columns, KeysType.DUP_KEYS,
                new SinglePartitionInfo(), new RangeDistributionInfo(), null);
        MaterializedIndex baseIndex = new MaterializedIndex(indexId, indexId, MaterializedIndex.IndexState.NORMAL,
                PhysicalPartition.INVALID_SHARD_GROUP_ID);
        TabletMeta tabletMeta = new TabletMeta(db.getId(), tableId, physicalPartitionId, indexId,
                TStorageMedium.HDD, true);
        // Range-distributed tablets exercise every variant kind of the tablet range.
        List<Tuple> bounds = Lists.newArrayList(
                new Tuple(Lists.newArrayList(new BoolVariant(false), new IntVariant(IntegerType.BIGINT, 0))),
                new Tuple(Lists.newArrayList(new BoolVariant(true), new IntVariant(IntegerType.BIGINT, 1000))),
                new Tuple(Lists.newArrayList(
                        new LargeIntVariant("170141183460469231731687303715884105727"),
                        new StringVariant(VarcharType.VARCHAR, "m"))),
                new Tuple(Lists.newArrayList(
                        new DateVariant(DateType.DATETIME, Instant.ofEpochSecond(1_700_000_000L, 123_000_000L)),
                        new DecimalVariant(TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 4),
                                new BigDecimal("12345.6789")))));
        baseIndex.addTablet(new LakeTablet(indexId + 1, new TabletRange(Range.lt(bounds.get(0)))), tabletMeta);
        baseIndex.addTablet(new LakeTablet(indexId + 2,
                new TabletRange(Range.of(bounds.get(0), bounds.get(1), true, false))), tabletMeta);
        baseIndex.addTablet(new LakeTablet(indexId + 3,
                new TabletRange(Range.of(bounds.get(2), bounds.get(3), true, true))), tabletMeta);
        baseIndex.addTablet(new LakeTablet(indexId + 4, new TabletRange(Range.ge(bounds.get(3)))), tabletMeta);
        lakeTable.setBaseIndexMetaId(baseIndex.getMetaId());
        lakeTable.setIndexMeta(baseIndex.getMetaId(), "lake_range_tbl", columns, 0, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        lakeTable.setStorageInfo(newFilePathInfo("lake_range_tbl"), new DataCacheInfo(true, false));
        Partition partition = new Partition(partitionId, physicalPartitionId, "p1", baseIndex,
                new RangeDistributionInfo());
        partition.getDefaultPhysicalPartition().setVisibleVersion(5, System.currentTimeMillis());
        partition.getDefaultPhysicalPartition().setNextVersion(6);
        lakeTable.addPartition(partition);
        return lakeTable;
    }

    private static LakeMaterializedView newLakeMaterializedView() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        long mvId = 700101L;
        long partitionId = 700110L;
        long physicalPartitionId = 700111L;
        long indexId = 700120L;
        List<Column> columns = Lists.newArrayList(
                new Column("k1", IntegerType.INT, true, null, null, false, null, "", 1),
                new Column("total", IntegerType.BIGINT, false, null, null, true, null, "", 2));
        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        LakeMaterializedView lakeMv = new LakeMaterializedView(mvId, db.getId(), "lake_mv", columns,
                KeysType.DUP_KEYS, new SinglePartitionInfo(), new RandomDistributionInfo(2), refreshScheme);
        MaterializedIndex baseIndex = new MaterializedIndex(indexId, indexId, MaterializedIndex.IndexState.NORMAL,
                PhysicalPartition.INVALID_SHARD_GROUP_ID);
        TabletMeta tabletMeta = new TabletMeta(db.getId(), mvId, physicalPartitionId, indexId, TStorageMedium.HDD, true);
        baseIndex.addTablet(new LakeTablet(indexId + 1), tabletMeta);
        baseIndex.addTablet(new LakeTablet(indexId + 2), tabletMeta);
        lakeMv.setBaseIndexMetaId(baseIndex.getMetaId());
        lakeMv.setIndexMeta(baseIndex.getMetaId(), "lake_mv", columns, 0, 0, (short) 1, TStorageType.COLUMN,
                KeysType.DUP_KEYS);
        lakeMv.setStorageInfo(newFilePathInfo("lake_mv"), new DataCacheInfo(false, false));
        lakeMv.addPartition(new Partition(partitionId, physicalPartitionId, "lake_mv", baseIndex,
                new RandomDistributionInfo(2)));
        return lakeMv;
    }

    private static FilePathInfo newFilePathInfo(String tableName) {
        FilePathInfo.Builder builder = FilePathInfo.newBuilder();
        FileStoreInfo.Builder fsBuilder = builder.getFsInfoBuilder();
        S3FileStoreInfo.Builder s3FsBuilder = fsBuilder.getS3FsInfoBuilder();
        s3FsBuilder.setBucket("image-v3-bucket");
        s3FsBuilder.setRegion("cn-test-1");
        fsBuilder.setFsType(FileStoreType.S3);
        fsBuilder.setFsKey("image-v3-bucket");
        builder.setFullPath("s3://image-v3-bucket/" + tableName);
        return builder.build();
    }

    private static JsonArray stringArray(String... values) {
        JsonArray array = new JsonArray();
        for (String value : values) {
            array.add(new JsonPrimitive(value));
        }
        return array;
    }

    @Test
    public void testDataCacheInfoBytesRoundTrip() throws Exception {
        // The StarOS messages travel as bytes, exactly as in the GSON image.
        DataCacheInfo cacheInfo = new DataCacheInfo(true, true);
        DataCacheInfo expected = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(cacheInfo), DataCacheInfo.class);
        DataCacheInfo actual = DataCacheInfo.fromProto(DataCacheInfoPB.parseFrom(cacheInfo.toProto().toByteArray()));
        GsonEquivalence.assertEquivalent(expected, actual);
        FileCacheInfo fileCacheInfo = actual.getCacheInfo();
        Assertions.assertTrue(fileCacheInfo.getEnableCache());
        Assertions.assertTrue(fileCacheInfo.getAsyncWriteBack());
        Assertions.assertEquals(Base64.getEncoder().encodeToString(cacheInfo.getCacheInfo().toByteArray()),
                Base64.getEncoder().encodeToString(fileCacheInfo.toByteArray()));
    }
}
