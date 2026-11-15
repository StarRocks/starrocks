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
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Range;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeTable;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.thrift.TAgentTaskRequest;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TTabletMetaInfo;
import com.starrocks.thrift.TTabletSchema;
import com.starrocks.thrift.TTaskType;
import com.starrocks.thrift.TUpdateTabletMetaInfoReq;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.MockGenericPool;
import com.starrocks.utframe.MockedBackend;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LakeTableAsyncFastSchemaChangeJobTest extends LakeFastSchemaChangeTestBase {

    @Override
    public boolean isFastSchemaEvolutionV2() {
        return false;
    }

    @BeforeEach
    public void resetFollowerJournalQueue() throws Exception {
        // Only the tests that read the journal drain the follower queue, so start every test from an empty one.
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
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
                    com.starrocks.catalog.MaterializedIndex index =
                            physicalPartition.getLatestIndex(table.getBaseIndexMetaId());
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

    @Test
    public void testLeaderPrunesFinishedJob() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeTable table = createDupTableWithBuckets("t_prune_finished", 2);
        LakeTableAsyncFastSchemaChangeJob job = newRangeFlipJob(db, table);

        // The first round sends the tablet meta updates and submits the publish.
        job.run();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        assertHasTabletData(job);
        Assertions.assertFalse(partitionsWithSchemaFile(job).isEmpty());
        runToFinished(job);

        assertPruned(job);
        Assertions.assertTrue(partitionsWithSchemaFile(job).isEmpty());
        Assertions.assertEquals(1, job.getSchemaInfoList().size());
        Assertions.assertTrue(job.getHistorySchema().isPresent());
        Assertions.assertFalse(job.getCommitVersionMap().isEmpty());
        List<LakeTableAsyncFastSchemaChangeJob> entries = readJournaledEntries(job.getJobId());
        // The first entry is journaled before the job collects the partition indexes.
        for (LakeTableAsyncFastSchemaChangeJob entry : entries.subList(1, entries.size())) {
            assertHasTabletData(entry);
        }
        LakeTableAsyncFastSchemaChangeJob finishedEntry = entries.get(entries.size() - 1);
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, finishedEntry.getJobState());
        assertShowAlterUnchangedByPrune(finishedEntry);
    }

    @Test
    public void testLeaderPrunesCancelledJob() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeTable table = createDupTableWithBuckets("t_prune_cancelled", 2);
        LakeTableAsyncFastSchemaChangeJob job = newRangeFlipJob(db, table);
        job.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
        assertHasTabletData(job);
        Assertions.assertNotNull(job.batchTask);

        Assertions.assertTrue(job.cancel("cancel"));

        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
        assertPruned(job);
        Assertions.assertTrue(partitionsWithSchemaFile(job).isEmpty());
        LakeTableAsyncFastSchemaChangeJob cancelledEntry = lastJournaledEntry(job.getJobId());
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, cancelledEntry.getJobState());
        assertHasTabletData(cancelledEntry);
        assertShowAlterUnchangedByPrune(cancelledEntry);
    }

    @Test
    public void testFollowerReplayPrunesFinishedJob() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeTable table = createDupTableWithBuckets("t_prune_follower", 2);
        LakeTableAsyncFastSchemaChangeJob job = newRangeFlipJob(db, table);
        Map<Long, TabletRange> targetRanges = job.getTargetRanges();
        // The follower starts from the catalog as it was before the alter.
        UtFrameUtils.PseudoImage initialImage = saveMetastore();
        runToFinished(job);
        List<LakeTableAsyncFastSchemaChangeJob> entries = readJournaledEntries(job.getJobId());

        LocalMetastore followerMetastore = loadMetastore(initialImage);
        LakeTable followerTable = (LakeTable) followerMetastore.getTable(DB_NAME, table.getName());
        Assertions.assertNotEquals(columnNames(table), columnNames(followerTable));
        SchemaChangeHandler followerHandler = new SchemaChangeHandler();
        for (LakeTableAsyncFastSchemaChangeJob entry : entries) {
            replayOn(followerMetastore, followerHandler, entry);
        }

        // The handler keeps the first replayed entry and copies the data of each later one into it.
        LakeTableAsyncFastSchemaChangeJob retained = entries.get(0);
        Assertions.assertSame(retained, followerHandler.getAlterJobsV2().get(job.getJobId()));
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, retained.getJobState());
        assertPruned(retained);
        for (LakeTableAsyncFastSchemaChangeJob entry : entries.subList(1, entries.size())) {
            assertHasTabletData(entry);
        }
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, followerTable.getState());
        Assertions.assertEquals(columnNames(table), columnNames(followerTable));
        for (Tablet tablet : baseTablets(followerTable)) {
            Assertions.assertEquals(targetRanges.get(tablet.getId()), tablet.getRange());
        }
        OlapTableHistorySchema historySchema = retained.getHistorySchema().orElseThrow();
        Assertions.assertEquals(job.getHistorySchema().orElseThrow().getHistoryTxnIdThreshold(),
                historySchema.getHistoryTxnIdThreshold());
        Assertions.assertTrue(historySchema.getSchemaByIndexMetaId(table.getBaseIndexMetaId()).isPresent());
    }

    @Test
    public void testLoadPrunesDoneJobFromImage() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeTable table = createDupTableWithBuckets("t_prune_image", 2);
        LakeTableAsyncFastSchemaChangeJob job = newRangeFlipJob(db, table);
        runToFinished(job);
        // Images written by older versions hold the done job with its per-tablet data, as the journal entry does.
        LakeTableAsyncFastSchemaChangeJob doneJob = lastJournaledEntry(job.getJobId());
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, doneJob.getJobState());
        assertHasTabletData(doneJob);
        AlterJobMgr alterJobMgr = new AlterJobMgr(new SchemaChangeHandler(), new MaterializedViewHandler(),
                new SystemHandler());
        alterJobMgr.getSchemaChangeHandler().addAlterJobV2(doneJob);
        // SHOW ALTER of the loaded job must match that of a job that went through the same serialization.
        List<List<Comparable>> expectedRows = new ArrayList<>();
        gsonCopy(doneJob).getInfo(expectedRows);

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        alterJobMgr.save(image.getImageWriter());
        AlterJobMgr restoredMgr = new AlterJobMgr(new SchemaChangeHandler(), new MaterializedViewHandler(),
                new SystemHandler());
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        restoredMgr.load(reader);
        reader.close();

        LakeTableAsyncFastSchemaChangeJob restored = (LakeTableAsyncFastSchemaChangeJob)
                restoredMgr.getSchemaChangeHandler().getAlterJobsV2().get(job.getJobId());
        assertPruned(restored);
        Assertions.assertTrue(restored.getHistorySchema().isPresent());
        List<List<Comparable>> restoredRows = new ArrayList<>();
        restored.getInfo(restoredRows);
        Assertions.assertFalse(expectedRows.isEmpty());
        Assertions.assertEquals(expectedRows, restoredRows);
    }

    /**
     * A metadata-only trailing key add whose job carries a target range for every tablet of the table.
     */
    private LakeTableAsyncFastSchemaChangeJob newRangeFlipJob(Database db, LakeTable table) {
        Column newKey = newTrailingKeyColumn(table);
        Map<Long, TabletRange> targetRanges = new HashMap<>();
        int i = 0;
        for (Tablet tablet : baseTablets(table)) {
            TabletRange old = oneColRange(i * 10);
            tablet.setRange(old);
            targetRanges.put(tablet.getId(),
                    new TabletRange(TrailingSortKeyRangeReprojection.appendTrailing(old.getRange(), List.of(newKey))));
            i++;
        }
        LakeTableAsyncFastSchemaChangeJob job = newJob(db, table, buildTrailingKeyTargetSchema(table, newKey));
        job.setTargetRanges(targetRanges);
        return job;
    }

    private static List<String> columnNames(OlapTable olapTable) {
        return olapTable.getBaseSchema().stream().map(Column::getName).collect(Collectors.toList());
    }

    private static void runToFinished(LakeTableAsyncFastSchemaChangeJob job) throws Exception {
        long deadlineMs = System.currentTimeMillis() + 60_000L;
        while (!job.isDone()) {
            Assertions.assertTrue(System.currentTimeMillis() < deadlineMs, "job is still " + job.getJobState());
            job.run();
            Thread.sleep(10);
        }
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState(), job.errMsg);
    }

    /**
     * Reads the journal entries of the job, from its first one to the one of its final state.
     */
    private static List<LakeTableAsyncFastSchemaChangeJob> readJournaledEntries(long jobId) throws Exception {
        List<LakeTableAsyncFastSchemaChangeJob> entries = new ArrayList<>();
        AlterJobV2 entry;
        do {
            entry = (AlterJobV2) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_JOB_V2);
            Assertions.assertEquals(jobId, entry.getJobId());
            entries.add((LakeTableAsyncFastSchemaChangeJob) entry);
        } while (!entry.isDone());
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, entries.get(0).getJobState());
        return entries;
    }

    private static LakeTableAsyncFastSchemaChangeJob lastJournaledEntry(long jobId) throws Exception {
        List<LakeTableAsyncFastSchemaChangeJob> entries = readJournaledEntries(jobId);
        return entries.get(entries.size() - 1);
    }

    private static AlterJobV2 gsonCopy(AlterJobV2 job) {
        return GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(job, AlterJobV2.class), AlterJobV2.class);
    }

    private static UtFrameUtils.PseudoImage saveMetastore() throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        GlobalStateMgr.getCurrentState().getLocalMetastore().save(image.getImageWriter());
        return image;
    }

    private static LocalMetastore loadMetastore(UtFrameUtils.PseudoImage image) throws Exception {
        LocalMetastore metastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        metastore.load(reader);
        reader.close();
        return metastore;
    }

    /**
     * Replays the journal entry into the handler against the given catalog, as a follower does.
     */
    private static void replayOn(LocalMetastore metastore, SchemaChangeHandler handler, AlterJobV2 entry) {
        LocalMetastore leaderMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        GlobalStateMgr.getCurrentState().setLocalMetastore(metastore);
        try {
            handler.replayAlterJobV2(entry);
        } finally {
            GlobalStateMgr.getCurrentState().setLocalMetastore(leaderMetastore);
        }
    }

    /**
     * SHOW ALTER of a done job reads none of the data the prune drops.
     */
    private static void assertShowAlterUnchangedByPrune(LakeTableAsyncFastSchemaChangeJob doneJob) {
        List<List<Comparable>> rowsBefore = new ArrayList<>();
        doneJob.getInfo(rowsBefore);
        doneJob.pruneMeta();
        assertPruned(doneJob);
        List<List<Comparable>> rowsAfter = new ArrayList<>();
        doneJob.getInfo(rowsAfter);
        Assertions.assertFalse(rowsBefore.isEmpty());
        Assertions.assertEquals(rowsBefore, rowsAfter);
    }

    private static Set<String> partitionsWithSchemaFile(LakeTableAsyncFastSchemaChangeJob job) {
        return Deencapsulation.getField(job, "partitionsWithSchemaFile");
    }

    /**
     * The job's per-tablet data: the partition indexes it updates and the target range of every tablet.
     */
    private static void assertHasTabletData(LakeTableAsyncFastSchemaChangeJob job) {
        Assertions.assertFalse(job.getPartitionIndexMap().isEmpty());
        Assertions.assertFalse(job.getTargetRanges().isEmpty());
    }

    private static void assertPruned(LakeTableAsyncFastSchemaChangeJob job) {
        Assertions.assertTrue(job.getPartitionIndexMap().isEmpty());
        Assertions.assertNull(job.batchTask);
        Assertions.assertNull(job.getTargetRanges());
    }
}
