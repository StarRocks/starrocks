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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.NullVariant;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Range;
import com.starrocks.lake.LakeTable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TAgentTaskRequest;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TTabletMetaInfo;
import com.starrocks.thrift.TTabletSchema;
import com.starrocks.thrift.TTaskType;
import com.starrocks.thrift.TUpdateTabletMetaInfoReq;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.MockGenericPool;
import com.starrocks.utframe.MockedBackend;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LakeTableAsyncFastSchemaChangeJobTest extends LakeFastSchemaChangeTestBase {

    @Override
    public boolean isFastSchemaEvolutionV2() {
        return false;
    }

    @Test
    public void testCompressionSettings() throws Exception {
        // Create a table with zstd compression and level 9
        LakeTable table = createTable(connectContext,
                "CREATE TABLE t_compression_test(c0 INT) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) " +
                        "BUCKETS 1 PROPERTIES('cloud_native_fast_schema_evolution_v2'='false', 'compression'='zstd(9)')");

        Assertions.assertEquals(TCompressionType.ZSTD, table.getCompressionType());
        Assertions.assertEquals(9, table.getCompressionLevel());

        List<MockedBackend.MockBeThriftClient> thriftClients = ((MockGenericPool<?>) ThriftConnectionPool.backendPool)
                .getAllBackends().stream().map(MockedBackend::getMockBeThriftClient).toList();
        Assertions.assertFalse(thriftClients.isEmpty());
        thriftClients.forEach(client -> client.setCaptureAgentTask(true));
        try {
            String alterSql = "ALTER TABLE t_compression_test ADD COLUMN c1 BIGINT";
            AlterJobV2 alterJob = executeAlterAndWaitFinish(table, alterSql, true);
            Assertions.assertInstanceOf(LakeTableAsyncFastSchemaChangeJob.class, alterJob);
            LakeTableAsyncFastSchemaChangeJob job = (LakeTableAsyncFastSchemaChangeJob) alterJob;
            List<SchemaInfo> schemaInfos = job.getSchemaInfoList();
            Assertions.assertEquals(1, schemaInfos.size());
            Assertions.assertEquals(TCompressionType.ZSTD, schemaInfos.get(0).getCompressionType());
            Assertions.assertEquals(9, schemaInfos.get(0).getCompressionLevel());

            // Get all tablet IDs from the table
            Set<Long> tableTabletIds = new HashSet<>();
            for (Partition partition : table.getPartitions()) {
                for (com.starrocks.catalog.PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                    com.starrocks.catalog.MaterializedIndex index = physicalPartition.getLatestIndex(table.getBaseIndexMetaId());
                    if (index != null) {
                        for (com.starrocks.catalog.Tablet tablet : index.getTablets()) {
                            tableTabletIds.add(tablet.getId());
                        }
                    }
                }
            }
            Assertions.assertEquals(1, tableTabletIds.size());

            // 1. get all TAgentTask by using MockBeThriftClient::getCapturedAgentTasks
            List<TAgentTaskRequest> allTasks = thriftClients.stream()
                    .flatMap(client -> client.getCapturedAgentTasks().stream())
                    .toList();

            // 2. get all tasks related to fast schema evolution for table t_compression_test
            // Fast schema evolution uses UPDATE_TABLET_META_INFO task type
            List<TAgentTaskRequest> fastSchemaEvolutionTasks = allTasks.stream()
                    .filter(task -> task.getTask_type() == TTaskType.UPDATE_TABLET_META_INFO)
                    .filter(task -> task.isSetUpdate_tablet_meta_info_req())
                    .toList();

            // 3. get TTabletSchema from agent task, filter by tablet IDs belonging to the table
            List<TTabletSchema> tabletSchemas = fastSchemaEvolutionTasks.stream()
                    .map(TAgentTaskRequest::getUpdate_tablet_meta_info_req)
                    .map(TUpdateTabletMetaInfoReq::getTabletMetaInfos)
                    .flatMap(List::stream)
                    .filter(metaInfo -> tableTabletIds.contains(metaInfo.getTablet_id()))
                    .filter(TTabletMetaInfo::isSetTablet_schema)
                    .map(TTabletMetaInfo::getTablet_schema)
                    .toList();

            Assertions.assertEquals(1, tabletSchemas.size(),
                    "There should be exactly one TTabletSchema for fast schema evolution");

            // 4. verify the compression type and level in TTabletSchema is correct
            TTabletSchema tabletSchema = tabletSchemas.get(0);
            Assertions.assertEquals(TCompressionType.ZSTD, tabletSchema.getCompression_type());
            Assertions.assertEquals(9, tabletSchema.getCompression_level());
        } finally {
            thriftClients.forEach(client -> client.setCaptureAgentTask(false));
            thriftClients.forEach(MockedBackend.MockBeThriftClient::clearCapturedAgentTasks);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Trailing sort-key range flip: persisted target map, in-place flip, pre-WAL validation,
    // exact-state replay idempotency. Every new behavior is guarded on targetRanges != null.
    // ---------------------------------------------------------------------------------------------

    private static Tuple oneColTuple(String v) {
        return new Tuple(Lists.newArrayList(Variant.of(IntegerType.INT, v)));
    }

    private static TabletRange oneColRange(int base) {
        return new TabletRange(Range.of(oneColTuple(String.valueOf(base)),
                oneColTuple(String.valueOf(base + 9)), true, true));
    }

    private List<Tablet> baseTablets(LakeTable table) {
        List<Tablet> tablets = new ArrayList<>();
        for (PhysicalPartition pp : table.getPhysicalPartitions()) {
            MaterializedIndex index = pp.getLatestIndex(table.getBaseIndexMetaId());
            if (index != null) {
                tablets.addAll(index.getTablets());
            }
        }
        return tablets;
    }

    /** New trailing key column with a constant default and a fresh unique id. */
    private static Column newTrailingKeyColumn(LakeTable table) {
        Column c = new Column("c_new", IntegerType.INT);
        c.setIsKey(true);
        c.setUniqueId(table.getMaxColUniqueId() + 1);
        c.setDefaultValue("0");
        return c;
    }

    /**
     * Build the TARGET schema info for a metadata-only trailing key add on a DUP table with base
     * schema (c0 key, c1 value): result is (c0 key, c_new key, c1 value) with the sort key spanning
     * (c0, c_new), a bumped version, and a short-key count that increases by one.
     */
    private SchemaInfo buildTrailingKeyTargetSchema(LakeTable table, Column newKeyColumn) {
        MaterializedIndexMeta baseMeta = table.getIndexMetaByMetaId(table.getBaseIndexMetaId());
        List<Column> oldSchema = baseMeta.getSchema();
        Column c0 = oldSchema.get(0);
        Column c1 = oldSchema.get(1);
        List<Column> targetColumns = Lists.newArrayList(c0, newKeyColumn, c1);
        short targetShortKey = (short) (baseMeta.getShortKeyColumnCount() + 1);
        return SchemaInfo.newBuilder()
                .setId(GlobalStateMgr.getCurrentState().getNextId())
                .setVersion(baseMeta.getSchemaVersion() + 1)
                .setKeysType(baseMeta.getKeysType())
                .setShortKeyColumnCount(targetShortKey)
                .setStorageType(table.getStorageType())
                .addColumns(targetColumns)
                .setSortKeyIndexes(List.of(0, 1))
                .setSortKeyUniqueIds(List.of(c0.getUniqueId(), newKeyColumn.getUniqueId()))
                .setIndexes(table.getCopiedIndexes())
                .build();
    }

    private LakeTable createDupTableWithBuckets(String name, int buckets) throws Exception {
        return createTable(connectContext, String.format(
                "CREATE TABLE %s (c0 INT, c1 INT) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS %d " +
                        "PROPERTIES('cloud_native_fast_schema_evolution_v2'='false')", name, buckets));
    }

    private LakeTableAsyncFastSchemaChangeJob newJob(Database db, LakeTable table, SchemaInfo target) {
        long jobId = GlobalStateMgr.getCurrentState().getNextId();
        LakeTableAsyncFastSchemaChangeJob job = new LakeTableAsyncFastSchemaChangeJob(
                jobId, db.getId(), table.getId(), table.getName(), 3600_000L);
        job.setIndexTabletSchema(table.getBaseIndexMetaId(),
                table.getIndexNameByMetaId(table.getBaseIndexMetaId()), target);
        return job;
    }

    @Test
    public void testTargetRangeFlipInPlace() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeTable table = createDupTableWithBuckets("t_range_flip", 3);
        long baseMetaId = table.getBaseIndexMetaId();

        List<Tablet> tablets = baseTablets(table);
        Assertions.assertEquals(3, tablets.size());

        Column newKey = newTrailingKeyColumn(table);
        Map<Long, TabletRange> targetRanges = new HashMap<>();
        int i = 0;
        for (Tablet tablet : tablets) {
            TabletRange old = oneColRange(i * 10);
            tablet.setRange(old);
            targetRanges.put(tablet.getId(),
                    new TabletRange(TrailingSortKeyRangeReprojection.appendTrailing(old.getRange(), List.of(newKey))));
            i++;
        }

        int oldVersion = table.getIndexMetaByMetaId(baseMetaId).getSchemaVersion();
        SchemaInfo target = buildTrailingKeyTargetSchema(table, newKey);

        LakeTableAsyncFastSchemaChangeJob job = newJob(db, table, target);
        job.setTargetRanges(targetRanges);
        job.updateCatalog(db, table, false);

        // Index-meta arity and short-key installed.
        MaterializedIndexMeta after = table.getIndexMetaByMetaId(baseMetaId);
        Assertions.assertEquals(List.of(0, 1), after.getSortKeyIdxes());
        Assertions.assertEquals(List.of(after.getSchema().get(0).getUniqueId(), newKey.getUniqueId()),
                after.getSortKeyUniqueIds());
        Assertions.assertEquals((short) 2, after.getShortKeyColumnCount());
        Assertions.assertEquals(3, after.getSchema().size());
        Assertions.assertTrue(after.getSchemaVersion() > oldVersion);

        // In-place flip: same tablet object identity, range gained one trailing NULL sentinel.
        List<Tablet> refetched = baseTablets(table);
        Assertions.assertEquals(tablets.size(), refetched.size());
        for (Tablet tablet : refetched) {
            Assertions.assertSame(tabletById(tablets, tablet.getId()), tablet);
            List<Variant> lower = tablet.getRange().getRange().getLowerBound().getValues();
            List<Variant> upper = tablet.getRange().getRange().getUpperBound().getValues();
            Assertions.assertEquals(2, lower.size());
            Assertions.assertEquals(2, upper.size());
            Assertions.assertInstanceOf(NullVariant.class, lower.get(1));
            Assertions.assertInstanceOf(NullVariant.class, upper.get(1));
            // Prefix preserved.
            Assertions.assertEquals(targetRanges.get(tablet.getId()), tablet.getRange());
        }
    }

    private static Tablet tabletById(List<Tablet> tablets, long id) {
        return tablets.stream().filter(t -> t.getId() == id).findFirst().orElseThrow();
    }

    @Test
    public void testExactVersionReplayDoesNotDoubleAppend() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeTable table = createDupTableWithBuckets("t_range_replay", 2);
        long baseMetaId = table.getBaseIndexMetaId();

        List<Tablet> tablets = baseTablets(table);
        Column newKey = newTrailingKeyColumn(table);
        Map<Long, TabletRange> targetRanges = new HashMap<>();
        int i = 0;
        for (Tablet tablet : tablets) {
            TabletRange old = oneColRange(i * 10);
            tablet.setRange(old);
            targetRanges.put(tablet.getId(),
                    new TabletRange(TrailingSortKeyRangeReprojection.appendTrailing(old.getRange(), List.of(newKey))));
            i++;
        }
        SchemaInfo target = buildTrailingKeyTargetSchema(table, newKey);

        LakeTableAsyncFastSchemaChangeJob job = newJob(db, table, target);
        job.setTargetRanges(targetRanges);

        // First apply flips to N+1. After this, the job's schema version equals the current one.
        job.updateCatalog(db, table, false);
        int versionAfterFirst = table.getIndexMetaByMetaId(baseMetaId).getSchemaVersion();

        // Exact-state replay: verifies live == target and returns without re-appending.
        job.updateCatalog(db, table, true);
        Assertions.assertEquals(versionAfterFirst, table.getIndexMetaByMetaId(baseMetaId).getSchemaVersion());
        for (Tablet tablet : baseTablets(table)) {
            // Still exactly N+1 columns (no double append).
            Assertions.assertEquals(2, tablet.getRange().getRange().getLowerBound().getValues().size());
            Assertions.assertEquals(targetRanges.get(tablet.getId()), tablet.getRange());
        }
    }

    @Test
    public void testMissingCoverageRejectedBeforeWal() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeTable table = createDupTableWithBuckets("t_range_missing", 3);
        long baseMetaId = table.getBaseIndexMetaId();

        List<Tablet> tablets = baseTablets(table);
        Column newKey = newTrailingKeyColumn(table);
        Map<Long, TabletRange> targetRanges = new HashMap<>();
        int i = 0;
        for (Tablet tablet : tablets) {
            TabletRange old = oneColRange(i * 10);
            tablet.setRange(old);
            // Deliberately drop coverage for the first tablet.
            if (i != 0) {
                targetRanges.put(tablet.getId(),
                        new TabletRange(TrailingSortKeyRangeReprojection.appendTrailing(old.getRange(), List.of(newKey))));
            }
            i++;
        }
        int oldVersion = table.getIndexMetaByMetaId(baseMetaId).getSchemaVersion();
        SchemaInfo target = buildTrailingKeyTargetSchema(table, newKey);

        LakeTableAsyncFastSchemaChangeJob job = newJob(db, table, target);
        job.setTargetRanges(targetRanges);
        job.setJobState(AlterJobV2.JobState.FINISHED_REWRITING);

        Assertions.assertThrows(AlterCancelException.class, () -> job.validateBeforeFinishUnprotected(db, table));

        // Pre-WAL failure: job stays FINISHED_REWRITING, catalog + ranges untouched (still 1-col).
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        Assertions.assertEquals(oldVersion, table.getIndexMetaByMetaId(baseMetaId).getSchemaVersion());
        for (Tablet tablet : baseTablets(table)) {
            Assertions.assertEquals(1, tablet.getRange().getRange().getLowerBound().getValues().size());
        }
    }

    @Test
    public void testExtraCoverageRejectedBeforeWal() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeTable table = createDupTableWithBuckets("t_range_extra", 2);

        List<Tablet> tablets = baseTablets(table);
        Column newKey = newTrailingKeyColumn(table);
        Map<Long, TabletRange> targetRanges = new HashMap<>();
        int i = 0;
        for (Tablet tablet : tablets) {
            TabletRange old = oneColRange(i * 10);
            tablet.setRange(old);
            targetRanges.put(tablet.getId(),
                    new TabletRange(TrailingSortKeyRangeReprojection.appendTrailing(old.getRange(), List.of(newKey))));
            i++;
        }
        // An extra tablet id that is not part of the live set.
        targetRanges.put(-9999L, new TabletRange(
                TrailingSortKeyRangeReprojection.appendTrailing(oneColRange(0).getRange(), List.of(newKey))));

        SchemaInfo target = buildTrailingKeyTargetSchema(table, newKey);
        LakeTableAsyncFastSchemaChangeJob job = newJob(db, table, target);
        job.setTargetRanges(targetRanges);

        Assertions.assertThrows(AlterCancelException.class, () -> job.validateBeforeFinishUnprotected(db, table));
    }

    @Test
    public void testGsonRoundTripPreservesTargetsByValue() {
        long jobId = GlobalStateMgr.getCurrentState().getNextId();
        LakeTableAsyncFastSchemaChangeJob job = new LakeTableAsyncFastSchemaChangeJob(
                jobId, 1L, 2L, "t_gson", 1000L);
        Map<Long, TabletRange> targetRanges = new HashMap<>();
        targetRanges.put(100L, new TabletRange(Range.of(oneColTuple("5"), oneColTuple("9"), true, true)));
        targetRanges.put(200L, new TabletRange(Range.of(oneColTuple("10"), oneColTuple("19"), false, true)));
        job.setTargetRanges(targetRanges);

        String json = GsonUtils.GSON.toJson(job.copyForPersist());
        LakeTableAsyncFastSchemaChangeJob restored =
                (LakeTableAsyncFastSchemaChangeJob) GsonUtils.GSON.fromJson(json, AlterJobV2.class);

        Assertions.assertEquals(targetRanges, restored.getTargetRanges());
        // Re-wrapped immutable in gsonPostProcess.
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> restored.getTargetRanges().put(300L, new TabletRange()));
    }

    @Test
    public void testTabletRangeEqualsIsValueBased() {
        TabletRange a = new TabletRange(Range.of(oneColTuple("1"), oneColTuple("5"), true, true));
        TabletRange b = new TabletRange(Range.of(oneColTuple("1"), oneColTuple("5"), true, true));
        TabletRange c = new TabletRange(Range.of(oneColTuple("1"), oneColTuple("6"), true, true));

        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertNotEquals(a, c);
    }

    // Regression: with targetRanges unset, an ordinary value-column FSE update behaves exactly as
    // before -- no coverage rejection, no NPE, no range flip, and the original short-key assertion
    // still applies.
    @Test
    public void testUnsetTargetRangesIsByteForByte() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeTable table = createDupTableWithBuckets("t_range_unset", 2);
        long baseMetaId = table.getBaseIndexMetaId();
        MaterializedIndexMeta baseMeta = table.getIndexMetaByMetaId(baseMetaId);

        // Value-column add: short-key count unchanged (the original :187 assertion must hold).
        List<Column> oldSchema = baseMeta.getSchema();
        Column c2 = new Column("c2", IntegerType.INT);
        c2.setUniqueId(table.getMaxColUniqueId() + 1);
        c2.setDefaultValue("0");
        List<Column> targetColumns = Lists.newArrayList(oldSchema.get(0), oldSchema.get(1), c2);
        SchemaInfo target = SchemaInfo.newBuilder()
                .setId(GlobalStateMgr.getCurrentState().getNextId())
                .setVersion(baseMeta.getSchemaVersion() + 1)
                .setKeysType(baseMeta.getKeysType())
                .setShortKeyColumnCount(baseMeta.getShortKeyColumnCount())
                .setStorageType(table.getStorageType())
                .addColumns(targetColumns)
                .setSortKeyIndexes(baseMeta.getSortKeyIdxes())
                .setSortKeyUniqueIds(baseMeta.getSortKeyUniqueIds())
                .setIndexes(table.getCopiedIndexes())
                .build();

        List<Tablet> tablets = baseTablets(table);
        LakeTableAsyncFastSchemaChangeJob job = newJob(db, table, target);
        // No targetRanges set: validation is a no-op even though nothing covers the tablets.
        Assertions.assertDoesNotThrow(() -> job.validateBeforeFinishUnprotected(db, table));

        job.updateCatalog(db, table, false);

        Assertions.assertEquals(3, table.getIndexMetaByMetaId(baseMetaId).getSchema().size());
        Assertions.assertEquals(baseMeta.getShortKeyColumnCount(),
                table.getIndexMetaByMetaId(baseMetaId).getShortKeyColumnCount());
        // Hash-distribution tablets keep a null range -- no flip on the unset path.
        for (Tablet tablet : tablets) {
            Assertions.assertNull(tablet.getRange());
        }
    }
}
