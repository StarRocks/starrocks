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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.starrocks.alter.AlterJobMgr;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.alter.LakeRangeRollupJob;
import com.starrocks.alter.LakeTableAddIndexJob;
import com.starrocks.alter.LakeTableAlterMetaJobBase;
import com.starrocks.alter.LakeTableDropIndexJob;
import com.starrocks.alter.LakeTableSchemaChangeJob;
import com.starrocks.alter.MaterializedViewHandler;
import com.starrocks.alter.OlapTableHistorySchema;
import com.starrocks.alter.OptimizeTask;
import com.starrocks.alter.RollupJobV2;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.alter.SchemaChangeJobV2;
import com.starrocks.alter.SystemHandler;
import com.starrocks.catalog.BoolVariant;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.IntVariant;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.catalog.SyncMaterializedViewDefinition;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Range;
import com.starrocks.extension.ExtensionManager;
import com.starrocks.lake.LakeTablet;
import com.starrocks.persist.OriginStatementInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.gson.internal.RuntimeTypeAdapterTypes;
import com.starrocks.persist.proto.AlterJobPB;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TDropIndexInfo;
import com.starrocks.thrift.TIndexType;
import com.starrocks.thrift.TOlapTableIndex;
import com.starrocks.thrift.TPrimaryKeyEncodingType;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.invoke;
import static com.starrocks.common.jmockit.Deencapsulation.setField;

/**
 * The alter jobs written through the ALTER_JOB_MGR section come back as the v2 (GSON) block delivers them. The
 * shared-nothing jobs come from a real cluster, the lake jobs are built as GSON documents in the v2 form, and the two
 * index fast-path jobs are built with their constructors because a GSON document cannot carry their Thrift payload.
 */
public class AlterJobMgrImageV3Test {
    private static final String DB = "image_v3_alter";
    private static final long FAKE_DB_ID = 8_100_000L;
    private static final long FAKE_TABLE_ID = 8_100_001L;

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static long nextId = 8_200_000L;

    @TempDir
    Path tempDir;

    @BeforeAll
    public static void beforeClass() throws Exception {
        ExtensionManager.getInstance().loadExtensionsFromClassPath(Path.of("target/classes").toString());
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB).useDatabase(DB);
        starRocksAssert.withTable("CREATE TABLE sc_tbl (k1 int, k2 varchar(20), v1 bigint)"
                + " DUPLICATE KEY(k1, k2) DISTRIBUTED BY HASH(k1) BUCKETS 2 PROPERTIES ('replication_num' = '1')");
        // A synchronous materialized view with a WHERE clause: RollupJobV2 keeps the original statement.
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW sync_mv AS"
                + " SELECT k1, sum(v1) FROM sc_tbl WHERE k1 > 1 GROUP BY k1");
        starRocksAssert.alterTable("ALTER TABLE sc_tbl ADD COLUMN v2 int DEFAULT '0'");
        starRocksAssert.checkSchemaChangeJob();
    }

    private static AlterJobMgr newEmptyAlterJobMgr() {
        return new AlterJobMgr(new SchemaChangeHandler(), new MaterializedViewHandler(), new SystemHandler());
    }

    private static long nextId() {
        return nextId++;
    }

    @Test
    public void testV3ImageMatchesV2() throws Exception {
        AlterJobMgr source = newEmptyAlterJobMgr();
        for (AlterJobV2 job : sourceJobs()) {
            ProtoInstanceAssertions.assertInstances(job.toProto());
            GsonEquivalence.assertEquivalent(job, AlterJobV2.fromProto(job.toProto()));
            if (job.getType() == AlterJobV2.JobType.ROLLUP) {
                source.getMaterializedViewHandler().addAlterJobV2(job);
            } else {
                source.getSchemaChangeHandler().addAlterJobV2(job);
            }
        }
        Map<Long, AlterJobV2> expectedJobs = new TreeMap<>();
        expectedJobs.putAll(source.getSchemaChangeHandler().getAlterJobsV2());
        expectedJobs.putAll(source.getMaterializedViewHandler().getAlterJobsV2());
        Assertions.assertEquals(16, expectedJobs.size());
        Map<Class<?>, String> registered = getField(RuntimeTypeAdapterTypes.CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES
                .get(AlterJobV2.class), "subtypeToLabel");
        Assertions.assertEquals(registered.keySet(), expectedJobs.values().stream().map(Object::getClass)
                .collect(java.util.stream.Collectors.toSet()));

        UtFrameUtils.PseudoImage v2Image = new UtFrameUtils.PseudoImage();
        source.save(v2Image.getImageWriter());
        AlterJobMgr v2Loaded = newEmptyAlterJobMgr();
        v2Loaded.load(v2Image.getMetaBlockReader());

        Path file = tempDir.resolve("alter_job_mgr.image");
        try (OutputStream os = Files.newOutputStream(file);
                ImageWriterV3 writer = new ImageWriterV3(os, 1)) {
            source.save(writer);
            writer.finish();
        }
        AlterJobMgr v3Loaded = newEmptyAlterJobMgr();
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            List<IndexEntryPB> sections = reader.sections();
            Assertions.assertEquals(1, sections.size());
            Assertions.assertEquals(SectionId.ALTER_JOB_MGR.getValue(), sections.get(0).getType());
            Assertions.assertEquals(expectedJobs.size(), sections.get(0).getNumEntries());
            reader.load(v3Loaded.getImageV3Loaders());
        }

        // The same jobs land in the same handlers, and each loaded job is the source job: the source is the anchor,
        // so a load step shared by the two formats cannot hide a fault.
        Assertions.assertEquals(v2Loaded.getSchemaChangeHandler().getAlterJobsV2().keySet(),
                v3Loaded.getSchemaChangeHandler().getAlterJobsV2().keySet());
        Assertions.assertEquals(v2Loaded.getMaterializedViewHandler().getAlterJobsV2().keySet(),
                v3Loaded.getMaterializedViewHandler().getAlterJobsV2().keySet());
        Map<Long, AlterJobV2> v3Jobs = new TreeMap<>();
        v3Jobs.putAll(v3Loaded.getSchemaChangeHandler().getAlterJobsV2());
        v3Jobs.putAll(v3Loaded.getMaterializedViewHandler().getAlterJobsV2());
        Assertions.assertEquals(expectedJobs.keySet(), v3Jobs.keySet());
        for (Map.Entry<Long, AlterJobV2> entry : expectedJobs.entrySet()) {
            AlterJobV2 v2Job = jobOf(v2Loaded, entry.getKey());
            AlterJobV2 v3Job = jobOf(v3Loaded, entry.getKey());
            Assertions.assertEquals(entry.getValue().getClass(), v3Job.getClass());
            JsonObject expected = expectedAfterLoad(entry.getValue());
            GsonEquivalence.assertEquivalent(expected, snapshot(v2Job));
            GsonEquivalence.assertEquivalent(expected, snapshot(v3Job));
            if (v3Job instanceof LakeTableSchemaChangeJob) {
                for (AlterJobV2 loaded : List.of(entry.getValue(), v2Job, v3Job)) {
                    Assertions.assertEquals(List.of(0), invoke(loaded, "shadowSortKeyIdxes", 502L, true));
                    Assertions.assertEquals(List.of(1), invoke(loaded, "shadowSortKeyUniqueIds", 502L, true));
                    Assertions.assertEquals(List.of(1, 0), invoke(loaded, "shadowSortKeyIdxes", 503L, false));
                    Assertions.assertEquals(List.of(2, 1), invoke(loaded, "shadowSortKeyUniqueIds", 503L, false));
                    Assertions.assertEquals(List.of(), invoke(loaded, "shadowSortKeyIdxes", 504L, false));
                    Assertions.assertEquals(List.of(), invoke(loaded, "shadowSortKeyUniqueIds", 504L, false));
                    Assertions.assertNull(invoke(loaded, "shadowSortKeyIdxes", 505L, false));
                    Assertions.assertNull(invoke(loaded, "shadowSortKeyUniqueIds", 505L, false));
                }
            }
            if (v3Job instanceof LakeRangeRollupJob) {
                for (AlterJobV2 loaded : List.of(v2Job, v3Job)) {
                    Assertions.assertTrue(((LakeRangeRollupJob) loaded).isSyncMaterializedView());
                    List<Column> schema = getField(loaded, "rollupSchema");
                    Assertions.assertNotNull(schema.get(2).getDefineExpr());
                    Assertions.assertTrue(AstToSQLBuilder.toSQL(schema.get(2).getDefineExpr()).contains("7"));
                    List<Column> sortColumns = getField(loaded, "rollupSortKeyColumns");
                    Assertions.assertSame(schema.get(0), sortColumns.get(0));
                }
            }
            JsonArray tasks = tree(v3Job).getAsJsonObject().getAsJsonArray("rewriteTasks");
            if (tasks != null) {
                for (JsonElement task : tasks) {
                    Assertions.assertEquals("optimize_db", task.getAsJsonObject().get("dbName").getAsString());
                }
            }
        }


    }

    @Test
    public void testCompletedJobsArePrunedAfterLoad() throws Exception {
        AlterJobMgr source = newEmptyAlterJobMgr();
        for (AlterJobV2.JobState state : List.of(AlterJobV2.JobState.FINISHED, AlterJobV2.JobState.CANCELLED)) {
            JsonObject document = schemaChangeJobV2Document();
            document.addProperty("jobState", state.name());
            AlterJobV2 job = fromDocument(document);
            source.getSchemaChangeHandler().addAlterJobV2(job);
            Assertions.assertEquals(1, ((com.google.common.collect.Table<?, ?, ?>)
                    getField(job, "physicalPartitionIndexMap")).size());
        }
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        source.save(image.getImageWriter());
        AlterJobMgr v2 = newEmptyAlterJobMgr();
        v2.load(image.getMetaBlockReader());
        Path file = tempDir.resolve("completed-jobs.image");
        try (ImageWriterV3 writer = new ImageWriterV3(Files.newOutputStream(file), 1)) {
            source.save(writer);
            writer.finish();
        }
        AlterJobMgr v3 = newEmptyAlterJobMgr();
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            reader.load(v3.getImageV3Loaders());
        }
        for (AlterJobV2 original : source.getSchemaChangeHandler().getAlterJobsV2().values()) {
            for (AlterJobMgr loaded : List.of(v2, v3)) {
                AlterJobV2 job = jobOf(loaded, original.getJobId());
                Assertions.assertEquals(original.getJobState(), job.getJobState());
                Assertions.assertTrue((boolean) getField(job, "metaPruned"));
                JsonObject restored = snapshot(job);
                Assertions.assertTrue(restored.getAsJsonObject("partitionIndexMap").getAsJsonArray("cells").isEmpty());
                Assertions.assertEquals(0, restored.getAsJsonObject("indexSchemaMap").size());
            }
            GsonEquivalence.assertEquivalent(jobOf(v2, original.getJobId()), jobOf(v3, original.getJobId()));
        }
    }

    @Test
    public void testLegacyLakeSchemaChangeWithoutIndexSortKeys() throws Exception {
        JsonObject document = lakeTableSchemaChangeJobDocument();
        AlterJobPB.Builder legacy = fromDocument(document).toProto().toBuilder();
        legacy.setLakeTableSchemaChangeJob(legacy.getLakeTableSchemaChangeJob().toBuilder()
                .clearIndexSortKeyIdxes().clearIndexSortKeyUniqueIds());
        document.remove("indexSortKeyIdxes");
        document.remove("indexSortKeyUniqueIds");
        AlterJobV2 v2 = fromDocument(document);
        AlterJobV2 v3 = AlterJobV2.fromProto(AlterJobPB.parseFrom(legacy.build().toByteArray()));
        GsonEquivalence.assertEquivalent(v2, v3);
        for (AlterJobV2 job : List.of(v2, v3)) {
            Assertions.assertTrue(((Map<?, ?>) getField(job, "indexSortKeyIdxes")).isEmpty());
            Assertions.assertTrue(((Map<?, ?>) getField(job, "indexSortKeyUniqueIds")).isEmpty());
            Assertions.assertEquals(List.of(0), invoke(job, "shadowSortKeyIdxes", 502L, true));
            Assertions.assertEquals(List.of(1), invoke(job, "shadowSortKeyUniqueIds", 502L, true));
            Assertions.assertNull(invoke(job, "shadowSortKeyIdxes", 503L, false));
            Assertions.assertNull(invoke(job, "shadowSortKeyUniqueIds", 503L, false));
        }
    }

    private static JsonObject expectedAfterLoad(AlterJobV2 source) {
        JsonObject expected = snapshot(source);
        if (source.getJobState().isFinalState()) {
            // Completed jobs retain their SHOW ALTER fields but discard per-tablet/schema payload on load.
            if (source instanceof SchemaChangeJobV2) {
                for (String field : List.of("partitionIndexMap", "partitionIndexTabletMap")) {
                    JsonObject table = expected.getAsJsonObject(field);
                    for (String member : List.of("rowKeys", "columnKeys", "cells")) {
                        table.add(member, new JsonArray());
                    }
                }
                expected.add("indexSchemaMap", new JsonObject());
                expected.add("indexShortKeyMap", new JsonObject());
            } else if (source instanceof RollupJobV2) {
                expected.add("partitionIdToRollupIndex", new JsonObject());
                expected.add("partitionIdToBaseRollupTabletIdMap", new JsonObject());
            } else if (source instanceof LakeTableAlterMetaJobBase) {
                JsonObject table = expected.getAsJsonObject("partitionIndexMap");
                for (String member : List.of("rowKeys", "columnKeys", "cells")) {
                    table.add(member, new JsonArray());
                }
            }
        }
        return expected;
    }

    private static AlterJobV2 jobOf(AlterJobMgr mgr, long jobId) {
        AlterJobV2 job = mgr.getSchemaChangeHandler().getAlterJobsV2().get(jobId);
        if (job == null) {
            job = mgr.getMaterializedViewHandler().getAlterJobsV2().get(jobId);
        }
        Assertions.assertNotNull(job, "job " + jobId);
        return job;
    }

    // ---- the source jobs ----

    private static List<AlterJobV2> sourceJobs() {
        List<AlterJobV2> jobs = Lists.newArrayList();
        // The real jobs of the cluster, as documents so that the source objects are not the live ones.
        JsonObject rollup = null;
        JsonObject schemaChange = null;
        for (AlterJobV2 job : GlobalStateMgr.getCurrentState().getRollupHandler().getAlterJobsV2().values()) {
            if (job.getTableName().equals("sc_tbl")) {
                rollup = snapshot(job);
            }
        }
        for (AlterJobV2 job : GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2().values()) {
            if (job.getTableName().equals("sc_tbl")) {
                schemaChange = snapshot(job);
            }
        }
        Assertions.assertNotNull(rollup);
        Assertions.assertNotNull(schemaChange);
        jobs.add(fromDocument(rollup));
        jobs.add(fromDocument(schemaChange));
        // The same rollup job as it was persisted while pending: the load rebuilds the define expressions and the
        // where clause from the original statement, and replays it into the catalog.
        JsonObject pendingRollup = rollup.deepCopy();
        pendingRollup.addProperty("jobId", nextId());
        pendingRollup.addProperty("jobState", "PENDING");
        jobs.add(fromDocument(pendingRollup));

        jobs.add(fromDocument(schemaChangeJobV2Document()));
        jobs.add(fromDocument(optimizeJobDocument("OptimizeJobV2")));
        jobs.add(fromDocument(optimizeJobDocument("OnlineOptimizeJobV2")));
        jobs.add(fromDocument(mergePartitionJobDocument()));
        jobs.add(fromDocument(lakeTableSchemaChangeJobDocument()));
        jobs.add(fromDocument(lakeRollupJobDocument()));
        jobs.add(fromDocument(lakeRangeRewriteSchemaChangeJobDocument("LakeRangeRewriteSchemaChangeJob")));
        jobs.add(fromDocument(lakeRangeRewriteSchemaChangeJobDocument("LakeMvSortKeyRewriteJob")));
        jobs.add(fromDocument(lakeRangeRollupJobDocument()));
        jobs.add(fromDocument(lakeTableAlterMetaJobDocument()));
        jobs.add(fromDocument(lakeTableAsyncFastSchemaChangeJobDocument()));
        jobs.add(lakeTableAddIndexJob());
        jobs.add(lakeTableDropIndexJob());
        return jobs;
    }

    private static JsonObject snapshot(AlterJobV2 job) {
        return GsonUtils.GSON.toJsonTree(job, AlterJobV2.class).getAsJsonObject();
    }

    private static AlterJobV2 fromDocument(JsonObject document) {
        // The runtime type adapter removes the class label from the object it reads; keep the document intact.
        return GsonUtils.GSON.fromJson(document.deepCopy(), AlterJobV2.class);
    }

    private static JsonElement tree(Object value) {
        return GsonUtils.GSON.toJsonTree(value);
    }

    /** The fields every job has, in the v2 form. The ids point at no table, so nothing is replayed into the catalog. */
    private static JsonObject jobDocument(String clazz, String type, String state) {
        JsonObject doc = new JsonObject();
        doc.addProperty("clazz", clazz);
        doc.addProperty("type", type);
        doc.addProperty("jobId", nextId());
        doc.addProperty("jobState", state);
        doc.addProperty("dbId", FAKE_DB_ID);
        doc.addProperty("tableId", FAKE_TABLE_ID);
        doc.addProperty("tableName", "lake_tbl");
        doc.addProperty("errMsg", "");
        doc.addProperty("createTimeMs", 1_700_000_000_000L);
        doc.addProperty("finishedTimeMs", -1L);
        doc.addProperty("timeoutMs", 86_400_000L);
        doc.addProperty("warehouseId", 7L);
        JsonObject resource = new JsonObject();
        resource.addProperty("clazz", "WarehouseComputeResource");
        resource.addProperty("cnGroupId", 71L);
        resource.addProperty("warehouseId", 7L);
        doc.add("computeResource", resource);
        doc.addProperty("forceSkippedAtCommitted", true);
        return doc;
    }

    private static List<Column> columns() {
        return Lists.newArrayList(
                new Column("k1", IntegerType.INT, true, null, null, false, null, "key", 1),
                new Column("k2", VarcharType.VARCHAR, true, null, null, true, null, "", 2),
                new Column("v1", IntegerType.BIGINT, false, null, null, true, null, "value", 3));
    }

    private static JsonArray columnsJson() {
        JsonArray array = new JsonArray();
        for (Column column : columns()) {
            array.add(tree(column));
        }
        return array;
    }

    private static Index index() {
        Map<String, String> properties = new TreeMap<>();
        properties.put("gram_num", "2");
        return new Index(11L, "idx_k2", Lists.newArrayList(ColumnId.create("k2")), IndexDef.IndexType.NGRAMBF,
                "ngram", properties);
    }

    private static MaterializedIndex shadowIndex(long indexId, boolean lake) {
        MaterializedIndex index = new MaterializedIndex(indexId, indexId, MaterializedIndex.IndexState.SHADOW,
                PhysicalPartition.INVALID_SHARD_GROUP_ID);
        TabletMeta meta = new TabletMeta(FAKE_DB_ID, FAKE_TABLE_ID, indexId + 100, indexId, TStorageMedium.HDD, lake);
        if (lake) {
            index.addTablet(new LakeTablet(indexId + 1), meta, false);
            index.addTablet(new LakeTablet(indexId + 2), meta, false);
        } else {
            index.addTablet(new LocalTablet(indexId + 1, Lists.newArrayList(
                    new Replica(indexId + 11, 10001L, Replica.ReplicaState.NORMAL, 3L, 1234))), meta, false);
        }
        return index;
    }

    /** The v2 form of a Guava table with one cell. */
    private static JsonObject tableCell(long rowKey, long columnKey, JsonElement value) {
        JsonObject table = new JsonObject();
        table.addProperty("clazz", "HashBasedTable");
        JsonArray rowKeys = new JsonArray();
        rowKeys.add(rowKey);
        JsonArray columnKeys = new JsonArray();
        columnKeys.add(columnKey);
        JsonArray cells = new JsonArray();
        cells.add(0);
        cells.add(0);
        cells.add(value);
        table.add("rowKeys", rowKeys);
        table.add("columnKeys", columnKeys);
        table.add("cells", cells);
        return table;
    }

    private static JsonObject longMap(long key, long value) {
        JsonObject map = new JsonObject();
        map.addProperty(String.valueOf(key), value);
        return map;
    }

    private static JsonArray longs(long... values) {
        JsonArray array = new JsonArray();
        for (long value : values) {
            array.add(value);
        }
        return array;
    }

    private static JsonArray strings(String... values) {
        JsonArray array = new JsonArray();
        for (String value : values) {
            array.add(value);
        }
        return array;
    }

    private static JsonObject schemaInfoJson(long id) {
        SchemaInfo info = SchemaInfo.newBuilder()
                .setId(id)
                .setVersion(2)
                .setKeysType(KeysType.DUP_KEYS)
                .setShortKeyColumnCount((short) 1)
                .setStorageType(TStorageType.COLUMN)
                .addColumns(columns())
                .setSortKeyIndexes(Lists.newArrayList(0, 1))
                .setSortKeyUniqueIds(Lists.newArrayList(1, 2))
                .setIndexes(Lists.newArrayList(index()))
                .setBloomFilterColumnNames(Lists.newArrayList(ColumnId.create("k2")))
                .setBloomFilterFpp(0.05)
                .setZstdCompressionColumns(List.of(ColumnId.create("v1")), Map.of(ColumnId.create("v1"), 65536))
                .setSchemaHash(777)
                .setCompressionType(TCompressionType.LZ4_FRAME)
                .setCompressionLevel(3)
                .setPrimaryKeyEncodingType(TPrimaryKeyEncodingType.PK_ENCODING_TYPE_V1)
                .build();
        return tree(info).getAsJsonObject();
    }

    private static JsonObject historySchemaJson() {
        JsonObject indexSchema = new JsonObject();
        indexSchema.addProperty("indexId", 501L);
        indexSchema.addProperty("indexName", "sc_tbl");
        indexSchema.add("schemaInfo", schemaInfoJson(9001L));
        JsonArray infos = new JsonArray();
        infos.add(indexSchema);
        JsonObject history = new JsonObject();
        history.addProperty("historyTxnIdThreshold", 4321L);
        history.add("schemaInfos", infos);
        return history;
    }

    private static JsonObject optimizeTaskJson(String name) {
        OptimizeTask task = new OptimizeTask(name);
        task.setUserIdentity(new UserIdentity("ephemeral-user", "%", false, true));
        task.setPartitionName("p1");
        task.setTempPartitionName("tp1");
        task.setLastVersion(5L);
        task.setPartitionNames(Lists.newArrayList("p1", "p2"));
        task.setTempPartitionNames(Lists.newArrayList("tp1", "tp2"));
        JsonObject json = tree(task).getAsJsonObject();
        json.addProperty("dbName", "default_cluster:optimize_db");
        return json;
    }

    private static JsonObject schemaChangeJobV2Document() {
        JsonObject doc = jobDocument("SchemaChangeJobV2", "SCHEMA_CHANGE", "WAITING_TXN");
        doc.add("partitionIndexTabletMap", tableCell(601L, 502L, longMap(50201L, 50101L)));
        doc.add("partitionIndexMap", tableCell(601L, 502L, tree(shadowIndex(502L, false))));
        doc.add("indexIdMap", longMap(502L, 501L));
        JsonObject names = new JsonObject();
        names.addProperty("502", "sc_tbl");
        doc.add("indexIdToName", names);
        JsonObject schemas = new JsonObject();
        schemas.add("502", columnsJson());
        doc.add("indexSchemaMap", schemas);
        JsonObject versionAndHash = new JsonObject();
        versionAndHash.addProperty("version", 3);
        versionAndHash.addProperty("hash", 4567);
        JsonObject versions = new JsonObject();
        versions.add("502", versionAndHash);
        doc.add("indexSchemaVersionAndHashMap", versions);
        doc.add("indexShortKeyMap", longMap(502L, 2L));
        addCompressionFields(doc);
        doc.addProperty("hasBfChange", true);
        doc.add("bfColumns", strings("k2", "k1"));
        doc.addProperty("bfFpp", 0.02);
        doc.addProperty("indexChange", true);
        JsonArray indexes = new JsonArray();
        indexes.add(tree(index()));
        doc.add("indexes", indexes);
        doc.addProperty("watershedTxnId", 99L);
        doc.addProperty("startTime", 1_700_000_001_000L);
        doc.add("sortKeyIdxes", longs(1, 0));
        doc.add("sortKeyUniqueIds", longs(2, 1));
        doc.addProperty("disableReplicatedStorageForGIN", true);
        doc.add("historySchema", historySchemaJson());
        return doc;
    }

    private static JsonObject optimizeJobDocument(String clazz) {
        JsonObject doc = jobDocument(clazz, "OPTIMIZE", "RUNNING");
        doc.addProperty("watershedTxnId", 100L);
        doc.add("tmpPartitionIds", longs(701L, 702L));
        JsonArray tasks = new JsonArray();
        tasks.add(optimizeTaskJson("optimize-p1"));
        doc.add("rewriteTasks", tasks);
        doc.add("sourcePartitionNames", strings("p1", "p2"));
        doc.add("tmpPartitionNames", strings("tp1", "tp2"));
        doc.addProperty("allPartitionOptimized", true);
        doc.add("distributionInfo", GsonUtils.GSON.toJsonTree(
                new HashDistributionInfo(4, columns().subList(0, 1)), DistributionInfo.class));
        doc.addProperty("optimizeOperation", "DISTRIBUTED BY HASH(k1) BUCKETS 4");
        return doc;
    }

    private static JsonObject mergePartitionJobDocument() {
        JsonObject doc = jobDocument("MergePartitionJob", "OPTIMIZE", "RUNNING");
        doc.addProperty("watershedTxnId", 101L);
        JsonObject ids = new JsonObject();
        ids.addProperty("clazz", "ArrayListMultimap");
        JsonObject idMap = new JsonObject();
        idMap.add("801", longs(701L, 702L));
        ids.add("map", idMap);
        doc.add("tmpPartitionIdToSourcePartitionIds", ids);
        JsonObject names = new JsonObject();
        names.addProperty("clazz", "ArrayListMultimap");
        JsonObject nameMap = new JsonObject();
        nameMap.add("tp_merged", strings("p1", "p2"));
        names.add("map", nameMap);
        doc.add("tempPartitionNameToSourcePartitionNames", names);
        JsonArray tasks = new JsonArray();
        tasks.add(optimizeTaskJson("merge-p1-p2"));
        doc.add("rewriteTasks", tasks);
        doc.add("distributionInfo", GsonUtils.GSON.toJsonTree(
                new HashDistributionInfo(2, columns().subList(0, 1)), DistributionInfo.class));
        doc.addProperty("optimizeOperation", "MERGE PARTITIONS");
        return doc;
    }

    private static JsonObject lakeTableSchemaChangeJobDocument() {
        JsonObject doc = jobDocument("LakeTableSchemaChangeJob", "SCHEMA_CHANGE", "FINISHED_REWRITING");
        doc.addProperty("watershedTxnId", 102L);
        doc.addProperty("watershedGtid", 1002L);
        doc.add("partitionIndexTabletMap", tableCell(601L, 502L, longMap(50201L, 50101L)));
        doc.add("partitionIndexMap", tableCell(601L, 502L, tree(shadowIndex(502L, true))));
        doc.add("indexIdMap", longMap(502L, 501L));
        JsonObject names = new JsonObject();
        names.addProperty("502", "lake_tbl");
        doc.add("indexIdToName", names);
        JsonObject schemas = new JsonObject();
        schemas.add("502", columnsJson());
        doc.add("indexSchemaMap", schemas);
        doc.add("indexShortKeyMap", longMap(502L, 1L));
        addCompressionFields(doc);
        doc.addProperty("hasBfChange", false);
        doc.add("bfColumns", strings("k1"));
        doc.addProperty("bfFpp", 0.01);
        doc.addProperty("indexChange", false);
        doc.addProperty("startTime", 1_700_000_002_000L);
        doc.add("commitVersionMap", longMap(601L, 9L));
        doc.add("sortKeyIdxes", longs(0));
        doc.add("sortKeyUniqueIds", longs(1));
        JsonObject indexSortKeyIdxes = new JsonObject();
        indexSortKeyIdxes.add("503", longs(1, 0));
        indexSortKeyIdxes.add("504", new JsonArray());
        doc.add("indexSortKeyIdxes", indexSortKeyIdxes);
        JsonObject indexSortKeyUniqueIds = new JsonObject();
        indexSortKeyUniqueIds.add("503", longs(2, 1));
        indexSortKeyUniqueIds.add("504", new JsonArray());
        doc.add("indexSortKeyUniqueIds", indexSortKeyUniqueIds);
        return doc;
    }

    private static JsonObject rollupFields(JsonObject doc) {
        JsonObject tabletIds = new JsonObject();
        tabletIds.add("601", longMap(50301L, 50101L));
        doc.add("partitionIdToBaseRollupTabletIdMap", tabletIds);
        JsonObject indexes = new JsonObject();
        indexes.add("601", tree(shadowIndex(503L, true)));
        doc.add("partitionIdToRollupIndex", indexes);
        doc.addProperty("baseIndexId", 501L);
        doc.addProperty("rollupIndexId", 503L);
        doc.addProperty("baseIndexName", "lake_tbl");
        doc.addProperty("rollupIndexName", "r1");
        doc.add("rollupSchema", columnsJson());
        doc.addProperty("rollupSchemaVersion", 1);
        doc.addProperty("baseSchemaHash", 111);
        doc.addProperty("rollupSchemaHash", 222);
        doc.addProperty("rollupKeysType", "AGG_KEYS");
        doc.addProperty("rollupShortKeyColumnCount", 2);
        doc.add("origStmt", tree(new OriginStatementInfo("CREATE MATERIALIZED VIEW r1 AS SELECT k1 FROM lake_tbl", 0)));
        doc.addProperty("viewDefineSql", "SELECT k1 FROM lake_tbl");
        doc.addProperty("isColocateMVIndex", true);
        return doc;
    }

    private static JsonObject lakeRollupJobDocument() {
        JsonObject doc = jobDocument("LakeRollupJob", "ROLLUP", "RUNNING");
        doc.addProperty("watershedTxnId", 103L);
        doc.addProperty("watershedGtid", 1003L);
        doc.add("commitVersionMap", longMap(601L, 12L));
        return rollupFields(doc);
    }

    private static JsonObject onlineRewriteFields(JsonObject doc) {
        doc.addProperty("watershedTxnId", 104L);
        doc.addProperty("watershedGtid", 1004L);
        doc.addProperty("shadowIndexMetaId", 504L);
        doc.addProperty("originIndexMetaId", 501L);
        doc.addProperty("shadowIndexName", "__shadow_lake_tbl");
        JsonObject state = new JsonObject();
        state.add("shadowIndex", tree(shadowIndex(504L, true)));
        state.addProperty("tabletCount", 2);
        JsonArray boundaries = new JsonArray();
        boundaries.add(tree(new Tuple(Lists.newArrayList(new BoolVariant(false), new IntVariant(IntegerType.INT, 10)))));
        state.add("boundaries", boundaries);
        state.addProperty("watershedVersion", 6L);
        state.addProperty("rewriteTxnId", 66L);
        state.addProperty("sourceEmpty", false);
        state.addProperty("rewriteFinished", true);
        JsonObject states = new JsonObject();
        states.add("601", state);
        doc.add("partitionStates", states);
        return doc;
    }

    private static JsonObject lakeRangeRewriteSchemaChangeJobDocument(String clazz) {
        JsonObject doc = onlineRewriteFields(jobDocument(clazz, "SCHEMA_CHANGE", "RUNNING"));
        doc.add("newSchema", columnsJson());
        doc.addProperty("newKeysType", "PRIMARY_KEYS");
        doc.add("newSortKeyIdxes", longs(1));
        doc.add("newSortKeyUniqueIds", longs(2));
        JsonArray sortKeyColumns = new JsonArray();
        sortKeyColumns.add(tree(columns().get(1)));
        doc.add("newSortKeyColumns", sortKeyColumns);
        doc.addProperty("shadowShortKeyColumnCount", 1);
        if (clazz.equals("LakeMvSortKeyRewriteJob")) {
            doc.addProperty("refreshWasActiveAtSubmit", true);
        }
        return doc;
    }

    private static JsonObject lakeRangeRollupJobDocument() {
        JsonObject doc = onlineRewriteFields(jobDocument("LakeRangeRollupJob", "ROLLUP", "WAITING_TXN"));
        List<Column> schema = columns();
        schema.get(2).setDefineExpr(SqlParser.parseSqlToExpr("CAST(k1 AS BIGINT) + 7", 0));
        SyncMaterializedViewDefinition definition = SyncMaterializedViewDefinition.capture(schema,
                SqlParser.parseSqlToExpr("k1 > 0", 0));
        doc.add("rollupSchema", tree(schema));
        doc.add("syncMvDefinition", tree(definition));
        doc.add("syncMvOriginStatement", tree(new OriginStatementInfo(
                "CREATE MATERIALIZED VIEW r AS SELECT k1, k2, CAST(k1 AS BIGINT) + 7 AS v1 FROM lake_tbl WHERE k1 > 0", 0)));
        doc.addProperty("viewDefineSql", "SELECT k1, k2, CAST(k1 AS BIGINT) + 7 AS v1 FROM lake_tbl WHERE k1 > 0");
        doc.addProperty("rollupKeysType", "DUP_KEYS");
        doc.add("rollupSortKeyIdxes", longs(0));
        doc.add("rollupSortKeyUniqueIds", longs(1));
        JsonArray sortKeyColumns = new JsonArray();
        sortKeyColumns.add(tree(columns().get(0)));
        doc.add("rollupSortKeyColumns", sortKeyColumns);
        doc.addProperty("shadowShortKeyColumnCount", 1);
        return doc;
    }

    private static JsonObject alterMetaFields(JsonObject doc) {
        doc.addProperty("watershedTxnId", 105L);
        doc.addProperty("watershedGtid", 1005L);
        doc.add("partitionIndexMap", tableCell(601L, 501L, tree(shadowIndex(501L, true))));
        doc.add("commitVersionMap", longMap(601L, 15L));
        return doc;
    }

    private static JsonObject lakeTableAlterMetaJobDocument() {
        JsonObject doc = alterMetaFields(jobDocument("LakeTableAlterMetaJob", "SCHEMA_CHANGE", "FINISHED"));
        doc.addProperty("metaType", "CHANGE_DATA_CAPTURE");
        doc.addProperty("enableChangeDataCapture", true);
        doc.addProperty("metaValue", true);
        doc.addProperty("persistentIndexType", "CLOUD_NATIVE");
        doc.addProperty("enableFileBundling", true);
        doc.addProperty("compactionStrategy", "REAL_TIME");
        JsonObject flatJson = new JsonObject();
        flatJson.addProperty("flatJsonEnable", true);
        flatJson.addProperty("flatJsonNullFactor", 0.3);
        flatJson.addProperty("flatJsonSparsityFactor", 0.9);
        flatJson.addProperty("flatJsonColumnMax", 100);
        flatJson.addProperty("configVersion", 2L);
        doc.add("flatJsonConfig", flatJson);
        return doc;
    }

    private static JsonObject lakeTableAsyncFastSchemaChangeJobDocument() {
        // The GSON label of LakeTableAsyncFastSchemaChangeJob.
        JsonObject doc = alterMetaFields(jobDocument("LakeTableFastSchemaEvolutionJob", "SCHEMA_CHANGE", "RUNNING"));
        JsonObject indexSchema = new JsonObject();
        indexSchema.addProperty("indexId", 501L);
        indexSchema.addProperty("indexName", "lake_tbl");
        indexSchema.add("schemaInfo", schemaInfoJson(9002L));
        JsonArray infos = new JsonArray();
        infos.add(indexSchema);
        doc.add("schemaInfos", infos);
        doc.addProperty("disableFseV2", true);
        doc.add("historySchema", historySchemaJson());
        Tuple lower = new Tuple(Lists.newArrayList(new IntVariant(IntegerType.BIGINT, 0)));
        Tuple upper = new Tuple(Lists.newArrayList(new IntVariant(IntegerType.BIGINT, 1000)));
        JsonObject ranges = new JsonObject();
        ranges.add("50101", tree(new TabletRange(Range.of(lower, upper, true, false))));
        doc.add("targetRanges", ranges);
        return doc;
    }

    private static LakeTableAddIndexJob lakeTableAddIndexJob() {
        TOlapTableIndex thriftIndex = new TOlapTableIndex();
        thriftIndex.setIndex_id(11L);
        thriftIndex.setIndex_name("idx_k2");
        thriftIndex.setIndex_type(TIndexType.NGRAMBF);
        thriftIndex.setColumns(Lists.newArrayList("k2"));
        Map<String, String> properties = new TreeMap<>();
        properties.put("gram_num", "2");
        thriftIndex.setIndex_properties(properties);
        LakeTableAddIndexJob job = new LakeTableAddIndexJob(nextId(), FAKE_DB_ID, FAKE_TABLE_ID, "lake_tbl",
                86_400_000L, Lists.newArrayList(index()), Lists.newArrayList("k1"));
        setField(job, "historySchema", GsonUtils.GSON.fromJson(historySchemaJson(), OlapTableHistorySchema.class));
        job.putNewSchema(501L, 9003L, 3L);
        return job;
    }

    private static LakeTableDropIndexJob lakeTableDropIndexJob() {
        TDropIndexInfo dropInfo = new TDropIndexInfo();
        dropInfo.setIndex_id(11L);
        dropInfo.setCol_unique_id(3);
        dropInfo.setIndex_type(TIndexType.BITMAP);
        LakeTableDropIndexJob job = new LakeTableDropIndexJob(nextId(), FAKE_DB_ID, FAKE_TABLE_ID, "lake_tbl", 86_400_000L,
                Lists.newArrayList(11L), Lists.newArrayList("idx_k2"),
                Lists.newArrayList("k2"));
        setField(job, "historySchema", GsonUtils.GSON.fromJson(historySchemaJson(), OlapTableHistorySchema.class));
        return job;
    }

    private static void addCompressionFields(JsonObject doc) {
        doc.addProperty("hasZstdCompressionChange", true);
        doc.add("zstdCompressionColumns", strings("v1"));
        JsonObject sizes = new JsonObject();
        sizes.addProperty("v1", 65536);
        doc.add("zstdCompressionPageSizes", sizes);
    }
}
