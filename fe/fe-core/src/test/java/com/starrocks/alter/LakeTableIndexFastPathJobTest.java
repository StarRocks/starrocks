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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AlterReplicaTask;
import com.starrocks.thrift.TAlterTabletReqV2;
import com.starrocks.thrift.TDropIndexInfo;
import com.starrocks.thrift.TIndexType;
import com.starrocks.thrift.TOlapTableIndex;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Light unit coverage for the Lake ADD/DROP INDEX fast-path Job classes.
 * We test the shape / serialization-friendliness of the classes rather than
 * the full AlterJobV2 lifecycle. The tests at the end run jobs on a min
 * shared-data cluster to check what a done job keeps in memory, and what a job
 * an FE loaded from the journal or an image sends BE.
 */
public class LakeTableIndexFastPathJobTest {
    private static final String DB_NAME = "db_lake_index_fast_path_job_test";
    private static ConnectContext connectContext;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        UtFrameUtils.stopBackgroundSchemaChangeHandler(60000);
        UtFrameUtils.setUpForPersistTest();
        CreateDbStmt createDbStmt =
                (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser("create database " + DB_NAME, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        connectContext.setDatabase(DB_NAME);
    }

    @BeforeEach
    public void resetFollowerJournalQueue() throws Exception {
        // Only the tests that read the journal drain the follower queue, so start every test from an empty one.
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
    }

    @Test
    public void testAddIndexJobConstruction() {
        long indexId = 101L;
        Index ix = new Index(indexId, "ix_a", Collections.singletonList(ColumnId.create("c1")),
                IndexDef.IndexType.BITMAP, "", new java.util.HashMap<>());

        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(ix));

        assertEquals(AlterJobV2.JobState.PENDING, job.getJobState());
        assertEquals(AlterJobV2.JobType.SCHEMA_CHANGE, job.getType());
        assertEquals(1, job.getNewIndexes().size());
        assertEquals("ix_a", job.getNewIndexes().get(0).getIndexName());
    }

    @Test
    public void testAddIndexJobCopyForPersist() {
        Index ix = new Index(5L, "ix_b", Collections.singletonList(ColumnId.create("c2")),
                IndexDef.IndexType.NGRAMBF, "", new java.util.HashMap<>());

        LakeTableAddIndexJob job = new LakeTableAddIndexJob(7L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(ix));

        AlterJobV2 copy = job.copyForPersist();
        assertInstanceOf(LakeTableAddIndexJob.class, copy);
        LakeTableAddIndexJob lc = (LakeTableAddIndexJob) copy;
        assertEquals(job.getJobId(), lc.getJobId());
        assertEquals(job.getNewIndexes().size(), lc.getNewIndexes().size());
        // Copy should be a distinct list instance.
        assertNotNull(lc.getNewIndexes());
        assertTrue(lc.getNewIndexes() != job.getNewIndexes());
    }

    @Test
    public void testDropIndexJobConstruction() {
        LakeTableDropIndexJob job = new LakeTableDropIndexJob(10L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(201L), Collections.singletonList("ix_a"));

        assertEquals(AlterJobV2.JobState.PENDING, job.getJobState());
        assertEquals(1, job.getDropIndexIds().size());
        assertEquals(List.of("ix_a"), job.getDropIndexNames());
        assertEquals(201L, job.getDropIndexIds().get(0));
    }

    @Test
    public void testDropIndexJobCopyForPersist() {
        LakeTableDropIndexJob job = new LakeTableDropIndexJob(11L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(201L), Collections.singletonList("ix_a"));

        AlterJobV2 copy = job.copyForPersist();
        assertInstanceOf(LakeTableDropIndexJob.class, copy);
        LakeTableDropIndexJob lc = (LakeTableDropIndexJob) copy;
        assertEquals(job.getJobId(), lc.getJobId());
        // Every journal entry carries the names the BE payload is rebuilt from.
        assertEquals(List.of("ix_a"), lc.getDropIndexNames());
        assertTrue(lc.getDropIndexNames() != job.getDropIndexNames());
    }

    @Test
    public void testTransactionIdNotSetBeforeWatershed() {
        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L,
                Collections.emptyList(), Collections.emptyList());
        // watershedTxnId defaults to -1 before runPendingJob allocates it.
        assertTrue(job.getTransactionId().isEmpty());
    }

    // -----------------------------------------------------------------
    // BE payload, rebuilt from the persisted definitions at each dispatch
    // -----------------------------------------------------------------

    @Test
    public void testAddIndexJob_BuildIndexesToAdd_OnlyTheIndexesTheTableLacks() throws Exception {
        // newIndexes is the table's whole index list after the alter; only the indexes the table does not carry
        // yet are built, under their columns' names.
        Index kept = new Index(-1L, "ix_kept", Collections.singletonList(ColumnId.create("c0")),
                IndexDef.IndexType.BITMAP, "", new java.util.HashMap<>());
        Index added = new Index(7L, "ix_added", Collections.singletonList(ColumnId.create("c1")),
                IndexDef.IndexType.GIN, "", Map.of("parser", "english"));
        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L, List.of(kept, added));
        OlapTable table = mock(OlapTable.class);
        when(table.getIndexes()).thenReturn(new ArrayList<>(List.of(kept)));
        when(table.getColumn(ColumnId.create("c1"))).thenReturn(new Column("c1", IntegerType.BIGINT));

        List<TOlapTableIndex> indexes = job.buildIndexesToAdd(table);

        assertEquals(1, indexes.size());
        TOlapTableIndex index = indexes.get(0);
        assertEquals("ix_added", index.getIndex_name());
        assertEquals(7L, index.getIndex_id());
        assertEquals(TIndexType.GIN, index.getIndex_type());
        assertEquals(List.of("c1"), index.getColumns());
        assertEquals(Map.of("parser", "english"), index.getIndex_properties());
    }

    @Test
    public void testAddIndexJob_BuildIndexesToAdd_BloomFilterColumns() throws Exception {
        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L, new ArrayList<>(),
                List.of("c1"));
        OlapTable table = mock(OlapTable.class);
        when(table.getBfFpp()).thenReturn(0.01);
        when(table.getColumn("c1")).thenReturn(new Column("c1", IntegerType.BIGINT));

        List<TOlapTableIndex> indexes = job.buildIndexesToAdd(table);

        assertEquals(1, indexes.size());
        TOlapTableIndex index = indexes.get(0);
        assertEquals(TIndexType.BLOOM_FILTER, index.getIndex_type());
        assertEquals(-1L, index.getIndex_id());
        assertEquals(List.of("c1"), index.getColumns());
        assertEquals("0.01", index.getIndex_properties().get("bloom_filter_fpp"));

        // A column that is gone cannot get a bloom filter: the dispatch cancels the job instead of skipping it.
        when(table.getColumn("c1")).thenReturn(null);
        assertThrows(AlterCancelException.class, () -> job.prepareAlterRequests(table));
    }

    @Test
    public void testAddIndexJob_PrepareAlterRequests_CancelsWithNothingToBuild() {
        // Every index the job lists is already on the table. Empty requests would let the job finish without
        // building anything, so the dispatch refuses.
        Index ix = new Index(-1L, "ix_a", Collections.singletonList(ColumnId.create("c1")),
                IndexDef.IndexType.BITMAP, "", new java.util.HashMap<>());
        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L, List.of(ix));
        OlapTable table = mock(OlapTable.class);
        when(table.getName()).thenReturn("t");
        when(table.getIndexes()).thenReturn(new ArrayList<>(List.of(ix)));

        AlterCancelException e = assertThrows(AlterCancelException.class, () -> job.prepareAlterRequests(table));
        assertTrue(e.getMessage().contains("no index to build"), e.getMessage());
    }

    @Test
    public void testDropIndexJob_BuildDropInfos() throws Exception {
        // One entry per column of each named index (matched case-insensitively), then one per bloom filter column.
        // A column the table no longer resolves is skipped, as the builder did.
        Index ix = new Index(-1L, "IX_A", List.of(ColumnId.create("c1"), ColumnId.create("c2"), ColumnId.create("gone")),
                IndexDef.IndexType.NGRAMBF, "", new java.util.HashMap<>());
        LakeTableDropIndexJob job = new LakeTableDropIndexJob(1L, 2L, 3L, "t", 60_000L, List.of(-1L),
                List.of("ix_a"), List.of("c3"));
        OlapTable table = mock(OlapTable.class);
        when(table.getIndexes()).thenReturn(new ArrayList<>(List.of(ix)));
        when(table.getColumn(ColumnId.create("c1"))).thenReturn(columnWithUniqueId("c1", 11));
        when(table.getColumn(ColumnId.create("c2"))).thenReturn(columnWithUniqueId("c2", 12));
        when(table.getColumn("c3")).thenReturn(columnWithUniqueId("c3", 13));

        List<TDropIndexInfo> infos = job.buildDropInfos(table);

        assertEquals(3, infos.size());
        assertDropEntry(infos.get(0), -1L, 11, TIndexType.NGRAMBF);
        assertDropEntry(infos.get(1), -1L, 12, TIndexType.NGRAMBF);
        assertDropEntry(infos.get(2), -1L, 13, TIndexType.BLOOM_FILTER);
    }

    @Test
    public void testDropIndexJob_PrepareAlterRequests_CancelsWhenNothingResolves() {
        OlapTable table = mock(OlapTable.class);
        when(table.getName()).thenReturn("t");
        when(table.getIndexes()).thenReturn(new ArrayList<>());

        LakeTableDropIndexJob gone = new LakeTableDropIndexJob(1L, 2L, 3L, "t", 60_000L, List.of(-1L),
                List.of("ix_gone"));
        AlterCancelException e = assertThrows(AlterCancelException.class, () -> gone.prepareAlterRequests(table));
        assertTrue(e.getMessage().contains("ix_gone"), e.getMessage());

        LakeTableDropIndexJob empty = new LakeTableDropIndexJob(2L, 2L, 3L, "t", 60_000L, List.of(), List.of());
        e = assertThrows(AlterCancelException.class, () -> empty.prepareAlterRequests(table));
        assertTrue(e.getMessage().contains("nothing to drop"), e.getMessage());

        LakeTableDropIndexJob goneBf = new LakeTableDropIndexJob(3L, 2L, 3L, "t", 60_000L, List.of(), List.of(),
                List.of("bf_gone"));
        e = assertThrows(AlterCancelException.class, () -> goneBf.prepareAlterRequests(table));
        assertTrue(e.getMessage().contains("bf_gone"), e.getMessage());
    }

    // -----------------------------------------------------------------
    // applyCatalogMutation: ADD INDEX path (no BF columns)
    // -----------------------------------------------------------------

    @Test
    public void testAddIndexJob_ApplyCatalogMutation_AppendsIndex() {
        Index ix = new Index(101L, "ix_a", Collections.singletonList(ColumnId.create("c1")),
                IndexDef.IndexType.BITMAP, "", new java.util.HashMap<>());

        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(ix));

        OlapTable table = mock(OlapTable.class);
        List<Index> existing = new ArrayList<>();
        when(table.getIndexes()).thenReturn(existing);
        job.applyCatalogMutation(table);
        assertEquals(1, existing.size());
        assertEquals("ix_a", existing.get(0).getIndexName());
    }

    @Test
    public void testAddIndexJob_ApplyCatalogMutation_IdempotentByIndexId() {
        // A pre-existing index with the same id (replay path): no duplicate append.
        Index ix = new Index(101L, "ix_a", Collections.singletonList(ColumnId.create("c1")),
                IndexDef.IndexType.BITMAP, "", new java.util.HashMap<>());
        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(ix), Collections.emptyList());

        OlapTable table = mock(OlapTable.class);
        List<Index> existing = new ArrayList<>();
        existing.add(new Index(101L, "ix_a", Collections.singletonList(ColumnId.create("c1")),
                IndexDef.IndexType.BITMAP, "", new java.util.HashMap<>()));
        when(table.getIndexes()).thenReturn(existing);
        job.applyCatalogMutation(table);
        assertEquals(1, existing.size());
    }

    @Test
    public void testAddIndexJob_ApplyCatalogMutation_DuplicateNameSkipped() {
        // Sanity: index id < 0 means "no concrete id" (BITMAP/NGRAMBF), in
        // which case sameIndex() falls back to name comparison.
        Index ix = new Index(-1L, "name_only", Collections.singletonList(ColumnId.create("c1")),
                IndexDef.IndexType.NGRAMBF, "", new java.util.HashMap<>());
        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(ix), Collections.emptyList());

        OlapTable table = mock(OlapTable.class);
        List<Index> existing = new ArrayList<>();
        existing.add(new Index(-1L, "name_only", Collections.singletonList(ColumnId.create("c1")),
                IndexDef.IndexType.NGRAMBF, "", new java.util.HashMap<>()));
        when(table.getIndexes()).thenReturn(existing);
        job.applyCatalogMutation(table);
        assertEquals(1, existing.size());
    }

    @Test
    public void testAddIndexJob_ApplyCatalogMutation_BumpsPerIndexSchema() {
        // The durability fix bumps EACH affected index meta's schema id/version
        // (per-index, so rollup / sync-MV index metas stay consistent with their
        // BE-stamped tablets). Assert the per-index map drives a shallowCopy ->
        // setSchemaId/setSchemaVersion -> put -> rebuildFullSchema for each entry.
        Index ix = new Index(101L, "ix_a", Collections.singletonList(ColumnId.create("c1")),
                IndexDef.IndexType.BITMAP, "", new java.util.HashMap<>());
        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(ix), Collections.emptyList());
        job.putNewSchema(500L, 900L, 7); // base index meta
        job.putNewSchema(501L, 901L, 4); // a rollup / MV index meta

        OlapTable table = mock(OlapTable.class);
        when(table.getIndexes()).thenReturn(new ArrayList<>());
        MaterializedIndexMeta meta500 = mock(MaterializedIndexMeta.class);
        MaterializedIndexMeta copy500 = mock(MaterializedIndexMeta.class);
        when(meta500.shallowCopy()).thenReturn(copy500);
        when(table.getIndexMetaByMetaId(500L)).thenReturn(meta500);
        MaterializedIndexMeta meta501 = mock(MaterializedIndexMeta.class);
        MaterializedIndexMeta copy501 = mock(MaterializedIndexMeta.class);
        when(meta501.shallowCopy()).thenReturn(copy501);
        when(table.getIndexMetaByMetaId(501L)).thenReturn(meta501);
        java.util.Map<Long, MaterializedIndexMeta> metaMap = new java.util.HashMap<>();
        when(table.getIndexMetaIdToMeta()).thenReturn(metaMap);

        job.applyCatalogMutation(table);

        verify(copy500).setSchemaId(900L);
        verify(copy500).setSchemaVersion(7);
        verify(copy501).setSchemaId(901L);
        verify(copy501).setSchemaVersion(4);
        assertEquals(copy500, metaMap.get(500L));
        assertEquals(copy501, metaMap.get(501L));
        verify(table).rebuildFullSchema();
    }

    @Test
    public void testAddIndexJob_PopulateAlterRequest_StampsPerIndexSchema() {
        // Each tablet's task is stamped with ITS index meta's new schema id (so BE
        // bumps that index's schema); a tablet whose index has no allocated entry
        // gets no stamp.
        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L,
                Collections.emptyList(), Collections.emptyList());
        job.putNewSchema(500L, 900L, 7);

        AlterReplicaTask stamped = mock(AlterReplicaTask.class);
        job.populateAlterRequest(stamped, 500L, null, null);
        verify(stamped).setOnlyAddIndex(any());
        verify(stamped).setNewIndexSchema(900L, 7L);

        AlterReplicaTask unstamped = mock(AlterReplicaTask.class);
        job.populateAlterRequest(unstamped, 999L, null, null); // no per-index entry
        verify(unstamped).setOnlyAddIndex(any());
        verify(unstamped, never()).setNewIndexSchema(anyLong(), anyLong());
    }

    @Test
    public void testAddIndexJob_PopulateAlterRequest_SkipsProjectingRollup() throws Exception {
        // A rollup / sync-MV whose schema projects away the indexed column must
        // receive an EMPTY index set (BE then writes a no-op txn log) and no
        // schema-id stamp; the base meta receives the full set + its stamp.
        Index ix = new Index(101L, "ix_a", Collections.singletonList(ColumnId.create("c1")),
                IndexDef.IndexType.BITMAP, "", new java.util.HashMap<>());
        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(ix));
        job.putNewSchema(500L, 900L, 7); // base meta only; rollup meta 501 gets no bump
        OlapTable table = mock(OlapTable.class);
        when(table.getIndexes()).thenReturn(new ArrayList<>());
        job.prepareAlterRequests(table);

        MaterializedIndexMeta baseMeta = mock(MaterializedIndexMeta.class);
        when(baseMeta.getSchema()).thenReturn(List.of(new Column("c1", IntegerType.BIGINT)));
        AlterReplicaTask baseTask = mock(AlterReplicaTask.class);
        job.populateAlterRequest(baseTask, 500L, baseMeta, table);
        ArgumentCaptor<List<TOlapTableIndex>> baseCap = ArgumentCaptor.forClass(List.class);
        verify(baseTask).setOnlyAddIndex(baseCap.capture());
        assertEquals(1, baseCap.getValue().size());
        verify(baseTask).setNewIndexSchema(900L, 7L);

        MaterializedIndexMeta rollupMeta = mock(MaterializedIndexMeta.class);
        when(rollupMeta.getSchema()).thenReturn(List.of(new Column("k9", IntegerType.BIGINT)));
        AlterReplicaTask rollupTask = mock(AlterReplicaTask.class);
        job.populateAlterRequest(rollupTask, 501L, rollupMeta, table);
        ArgumentCaptor<List<TOlapTableIndex>> rollupCap = ArgumentCaptor.forClass(List.class);
        verify(rollupTask).setOnlyAddIndex(rollupCap.capture());
        assertTrue(rollupCap.getValue().isEmpty());
        verify(rollupTask, never()).setNewIndexSchema(anyLong(), anyLong());
    }

    @Test
    public void testApplicableIndexes_MatchesByColumnIdNotName() {
        // A rollup / sync-MV that carries the indexed column under a RENAMED
        // output but the SAME ColumnId must still be considered applicable —
        // applicableIndexes matches by ColumnId (via the base table), not by
        // name, mirroring OlapTable.getIndexesBySchema. A name-based check would
        // wrongly skip it and leave the FE/BE schema id diverged.
        TOlapTableIndex thrift = new TOlapTableIndex();
        thrift.setIndex_type(TIndexType.BITMAP);
        thrift.setColumns(Collections.singletonList("v")); // base column name

        OlapTable table = mock(OlapTable.class);
        Column baseV = new Column("v", IntegerType.BIGINT); // ColumnId = "v"
        when(table.getColumn("v")).thenReturn(baseV);

        // Meta carries the same column id "v" but under a renamed output "w".
        Column renamed = new Column("w", IntegerType.BIGINT);
        renamed.setColumnId(ColumnId.create("v"));
        MaterializedIndexMeta meta = mock(MaterializedIndexMeta.class);
        when(meta.getSchema()).thenReturn(List.of(renamed));

        List<TOlapTableIndex> applicable = LakeTableAddIndexJob.applicableIndexes(
                Collections.singletonList(thrift), meta, table);
        assertEquals(1, applicable.size()); // matched by id despite the name differing

        // Sanity: a meta that carries neither the id nor the name is excluded.
        Column other = new Column("k9", IntegerType.BIGINT);
        MaterializedIndexMeta otherMeta = mock(MaterializedIndexMeta.class);
        when(otherMeta.getSchema()).thenReturn(List.of(other));
        assertTrue(LakeTableAddIndexJob.applicableIndexes(
                Collections.singletonList(thrift), otherMeta, table).isEmpty());
    }

    // -----------------------------------------------------------------
    // applyCatalogMutation: ADD plain BF columns
    // -----------------------------------------------------------------

    @Test
    public void testAddIndexJob_ApplyCatalogMutation_MergesAddBfColumns() {
        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L,
                new ArrayList<>(),
                List.of("c2"));

        OlapTable table = mock(OlapTable.class);
        when(table.getIndexes()).thenReturn(new ArrayList<>());
        // Existing bf_columns: c1 (and the unrelated col "ignored" not in props).
        Set<ColumnId> existingBf = new TreeSet<>(ColumnId.CASE_INSENSITIVE_ORDER);
        existingBf.add(ColumnId.create("c1"));
        when(table.getBfColumnIds()).thenReturn(existingBf);
        when(table.getBfFpp()).thenReturn(0.05);
        Column col = mock(Column.class);
        when(col.getName()).thenReturn("c2");
        when(table.getColumn("c2")).thenReturn(col);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Set<ColumnId>> setCaptor = ArgumentCaptor.forClass(Set.class);
        ArgumentCaptor<Double> fppCaptor = ArgumentCaptor.forClass(Double.class);
        job.applyCatalogMutation(table);
        verify(table).setBloomFilterInfo(setCaptor.capture(), fppCaptor.capture());

        Set<ColumnId> merged = setCaptor.getValue();
        assertEquals(2, merged.size());
        assertTrue(merged.contains(ColumnId.create("c1")));
        assertTrue(merged.contains(ColumnId.create("c2")));
        assertEquals(0.05, fppCaptor.getValue(), 1e-9);
    }

    @Test
    public void testAddIndexJob_ApplyCatalogMutation_AddBfColumnsUsesDefaultFppWhenZero() {
        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L,
                new ArrayList<>(),
                List.of("c1"));

        OlapTable table = mock(OlapTable.class);
        when(table.getIndexes()).thenReturn(new ArrayList<>());
        when(table.getBfColumnIds()).thenReturn(null);
        // Zero fpp → the job substitutes DEFAULT_BLOOM_FILTER_FPP.
        when(table.getBfFpp()).thenReturn(0.0);
        Column col = mock(Column.class);
        when(col.getName()).thenReturn("c1");
        when(table.getColumn("c1")).thenReturn(col);

        ArgumentCaptor<Double> fppCaptor = ArgumentCaptor.forClass(Double.class);
        job.applyCatalogMutation(table);
        verify(table).setBloomFilterInfo(any(), fppCaptor.capture());
        assertEquals(FeConstants.DEFAULT_BLOOM_FILTER_FPP, fppCaptor.getValue(), 1e-9);
    }

    @Test
    public void testAddIndexJob_ApplyCatalogMutation_SkipsMissingColumn() {
        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L,
                new ArrayList<>(),
                List.of("missing", "c1"));

        OlapTable table = mock(OlapTable.class);
        when(table.getIndexes()).thenReturn(new ArrayList<>());
        when(table.getBfColumnIds()).thenReturn(null);
        when(table.getBfFpp()).thenReturn(0.05);
        Column c1 = mock(Column.class);
        when(c1.getName()).thenReturn("c1");
        when(table.getColumn("c1")).thenReturn(c1);
        when(table.getColumn("missing")).thenReturn(null);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Set<ColumnId>> setCaptor = ArgumentCaptor.forClass(Set.class);
        job.applyCatalogMutation(table);
        verify(table).setBloomFilterInfo(setCaptor.capture(), anyDouble());
        // Only the resolvable column ends up in the merged set.
        assertEquals(1, setCaptor.getValue().size());
    }

    @Test
    public void testAddIndexJob_ApplyCatalogMutation_SkipsBfBranchWhenEmpty() {
        // No addBfColumns → setBloomFilterInfo should NOT be called.
        Index ix = new Index(101L, "ix_a", Collections.singletonList(ColumnId.create("c1")),
                IndexDef.IndexType.BITMAP, "", new java.util.HashMap<>());
        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(ix), Collections.emptyList());
        OlapTable table = mock(OlapTable.class);
        when(table.getIndexes()).thenReturn(new ArrayList<>());
        job.applyCatalogMutation(table);
        verify(table, never()).setBloomFilterInfo(any(), anyDouble());
    }

    // -----------------------------------------------------------------
    // applyCatalogMutation: DROP INDEX path
    // -----------------------------------------------------------------

    @Test
    public void testDropIndexJob_ApplyCatalogMutation_RemovesById() {
        LakeTableDropIndexJob job = new LakeTableDropIndexJob(1L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(201L), Collections.emptyList());

        OlapTable table = mock(OlapTable.class);
        List<Index> existing = new ArrayList<>();
        existing.add(new Index(201L, "ix_drop", Collections.singletonList(ColumnId.create("c1")),
                IndexDef.IndexType.BITMAP, "", new java.util.HashMap<>()));
        existing.add(new Index(202L, "ix_keep", Collections.singletonList(ColumnId.create("c2")),
                IndexDef.IndexType.BITMAP, "", new java.util.HashMap<>()));
        when(table.getIndexes()).thenReturn(existing);
        job.applyCatalogMutation(table);
        assertEquals(1, existing.size());
        assertEquals("ix_keep", existing.get(0).getIndexName());
    }

    @Test
    public void testDropIndexJob_ApplyCatalogMutation_RemovesByNameWhenIdsAreMinusOne() {
        // Regression: BITMAP / NGRAMBF / BLOOM_FILTER all share indexId = -1
        // (only GIN/VECTOR get a real id). Matching purely by id wipes out
        // every same-class index on the table. Verify name-based matching
        // removes only the named target.
        LakeTableDropIndexJob job = new LakeTableDropIndexJob(1L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(-1L),
                Collections.singletonList("A_bm"));

        OlapTable table = mock(OlapTable.class);
        List<Index> existing = new ArrayList<>();
        existing.add(new Index(-1L, "A_bm", Collections.singletonList(ColumnId.create("v1")),
                IndexDef.IndexType.BITMAP, "", new java.util.HashMap<>()));
        existing.add(new Index(-1L, "B_ng", Collections.singletonList(ColumnId.create("v2")),
                IndexDef.IndexType.NGRAMBF, "", new java.util.HashMap<>()));
        when(table.getIndexes()).thenReturn(existing);
        job.applyCatalogMutation(table);
        assertEquals(1, existing.size());
        assertEquals("B_ng", existing.get(0).getIndexName());
    }

    @Test
    public void testDropIndexJob_ApplyCatalogMutation_DropsBfColumnsPartial() {
        // Drop one of two bf columns → setBloomFilterInfo with the remaining set.
        LakeTableDropIndexJob job = new LakeTableDropIndexJob(1L, 2L, 3L, "t", 60_000L,
                new ArrayList<Long>(), new ArrayList<String>(),
                List.of("c1"));

        OlapTable table = mock(OlapTable.class);
        when(table.getIndexes()).thenReturn(new ArrayList<>());
        Set<ColumnId> existingBf = new TreeSet<>(ColumnId.CASE_INSENSITIVE_ORDER);
        existingBf.add(ColumnId.create("c1"));
        existingBf.add(ColumnId.create("c2"));
        when(table.getBfColumnIds()).thenReturn(existingBf);
        when(table.getBfFpp()).thenReturn(0.05);
        Column col = mock(Column.class);
        when(col.getName()).thenReturn("c1");
        when(table.getColumn("c1")).thenReturn(col);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Set<ColumnId>> setCaptor = ArgumentCaptor.forClass(Set.class);
        job.applyCatalogMutation(table);
        verify(table).setBloomFilterInfo(setCaptor.capture(), anyDouble());
        Set<ColumnId> remaining = setCaptor.getValue();
        assertEquals(1, remaining.size());
        assertTrue(remaining.contains(ColumnId.create("c2")));
    }

    @Test
    public void testDropIndexJob_ApplyCatalogMutation_DropsLastBfColumnClearsFpp() {
        // Last column drop → setBloomFilterInfo(null, 0).
        LakeTableDropIndexJob job = new LakeTableDropIndexJob(1L, 2L, 3L, "t", 60_000L,
                new ArrayList<Long>(), new ArrayList<String>(),
                List.of("only"));

        OlapTable table = mock(OlapTable.class);
        when(table.getIndexes()).thenReturn(new ArrayList<>());
        Set<ColumnId> existingBf = new TreeSet<>(ColumnId.CASE_INSENSITIVE_ORDER);
        existingBf.add(ColumnId.create("only"));
        when(table.getBfColumnIds()).thenReturn(existingBf);
        Column col = mock(Column.class);
        when(col.getName()).thenReturn("only");
        when(table.getColumn("only")).thenReturn(col);
        job.applyCatalogMutation(table);
        verify(table).setBloomFilterInfo(null, 0);
    }

    @Test
    public void testDropIndexJob_ApplyCatalogMutation_NoBfColumnsNoBfCall() {
        LakeTableDropIndexJob job = new LakeTableDropIndexJob(1L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(99L), Collections.emptyList());
        OlapTable table = mock(OlapTable.class);
        when(table.getIndexes()).thenReturn(new ArrayList<>());
        job.applyCatalogMutation(table);
        verify(table, never()).setBloomFilterInfo(any(), anyDouble());
    }

    @Test
    public void testDropIndexJob_ApplyCatalogMutation_MissingColumnSkipped() {
        // A renamed/dropped column referenced in dropBfColumns should be
        // skipped silently rather than NPE.
        LakeTableDropIndexJob job = new LakeTableDropIndexJob(1L, 2L, 3L, "t", 60_000L,
                new ArrayList<Long>(), new ArrayList<String>(),
                List.of("ghost"));

        OlapTable table = mock(OlapTable.class);
        when(table.getIndexes()).thenReturn(new ArrayList<>());
        Set<ColumnId> existingBf = new TreeSet<>(ColumnId.CASE_INSENSITIVE_ORDER);
        existingBf.add(ColumnId.create("real"));
        when(table.getBfColumnIds()).thenReturn(existingBf);
        when(table.getBfFpp()).thenReturn(0.05);
        when(table.getColumn("ghost")).thenReturn(null);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Set<ColumnId>> setCaptor = ArgumentCaptor.forClass(Set.class);
        job.applyCatalogMutation(table);
        // The "real" entry should remain since "ghost" wasn't resolvable.
        verify(table).setBloomFilterInfo(setCaptor.capture(), anyDouble());
        assertTrue(setCaptor.getValue().contains(ColumnId.create("real")));
    }

    // -----------------------------------------------------------------
    // copyForPersist: shape and field copy
    // -----------------------------------------------------------------

    @Test
    public void testAddIndexJob_CopyForPersist_PreservesAddBfColumns() {
        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L,
                new ArrayList<>(),
                List.of("c1", "c2"));
        LakeTableAddIndexJob copy = (LakeTableAddIndexJob) job.copyForPersist();
        assertEquals(2, copy.getAddBfColumns().size());
        assertTrue(copy.getAddBfColumns().contains("c1"));
        assertTrue(copy.getAddBfColumns().contains("c2"));
        // Distinct list instance.
        assertFalse(copy.getAddBfColumns() == job.getAddBfColumns());
    }

    @Test
    public void testDropIndexJob_CopyForPersist_PreservesDropBfColumns() {
        LakeTableDropIndexJob job = new LakeTableDropIndexJob(1L, 2L, 3L, "t", 60_000L,
                new ArrayList<Long>(), new ArrayList<String>(),
                List.of("c1"));
        LakeTableDropIndexJob copy = (LakeTableDropIndexJob) job.copyForPersist();
        assertEquals(1, copy.getDropBfColumns().size());
        assertEquals("c1", copy.getDropBfColumns().get(0));
        assertFalse(copy.getDropBfColumns() == job.getDropBfColumns());
    }

    @Test
    public void testAddIndexJob_AccessorsExposeFields() {
        Index ix = new Index(1L, "ix", Collections.singletonList(ColumnId.create("c1")),
                IndexDef.IndexType.BITMAP, "", new java.util.HashMap<>());
        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(ix),
                List.of("c1"));
        assertEquals(1, job.getNewIndexes().size());
        assertEquals(1, job.getAddBfColumns().size());
    }

    @Test
    public void testDropIndexJob_AccessorsExposeFields() {
        LakeTableDropIndexJob job = new LakeTableDropIndexJob(1L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(1L), Collections.singletonList("idx_name"), List.of("c1"));
        assertEquals(1, job.getDropIndexIds().size());
        assertEquals(1, job.getDropIndexNames().size());
        assertEquals(1, job.getDropBfColumns().size());
    }

    @Test
    public void testAddIndexJob_DefaultCtorForGsonDeserialization() {
        // Default constructor used by Gson; required to keep the class
        // round-trippable through the editlog.
        LakeTableAddIndexJob job = new LakeTableAddIndexJob();
        assertEquals(AlterJobV2.JobType.SCHEMA_CHANGE, job.getType());
        assertNotNull(job.getNewIndexes());
        assertNotNull(job.getAddBfColumns());
    }

    @Test
    public void testDropIndexJob_DefaultCtorForGsonDeserialization() {
        LakeTableDropIndexJob job = new LakeTableDropIndexJob();
        assertEquals(AlterJobV2.JobType.SCHEMA_CHANGE, job.getType());
        assertNotNull(job.getDropIndexIds());
        assertNotNull(job.getDropIndexNames());
        assertNotNull(job.getDropBfColumns());
    }

    @Test
    public void testAllowConcurrentPartitionCreation() {
        // The lake ADD/DROP INDEX fast-path jobs are metadata-only and tolerate concurrent
        // partition creation; they declare the capability so the partition-creation guards
        // can proceed instead of rejecting/cancelling.
        LakeTableAddIndexJob addJob = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L,
                new ArrayList<>(), new ArrayList<>());
        assertTrue(addJob.allowConcurrentPartitionCreation());

        LakeTableDropIndexJob dropJob = new LakeTableDropIndexJob(1L, 2L, 3L, "t", 60_000L,
                new ArrayList<>(), new ArrayList<>());
        assertTrue(dropJob.allowConcurrentPartitionCreation());

        // A full-rewrite schema-change job (shared-nothing or lake) must NOT declare the
        // capability: it registers shadow meta and iterates the live partition list.
        assertFalse(new SchemaChangeJobV2(1L, 2L, 3L, "t", 60_000L).allowConcurrentPartitionCreation());
        assertFalse(new LakeTableSchemaChangeJob(1L, 2L, 3L, "t", 60_000L).allowConcurrentPartitionCreation());
    }

    @Test
    public void testLeaderPrunesFinishedAddIndexJob() throws Exception {
        OlapTable table = createTable("t_add_index_finished", "c0 INT, c1 INT");
        long baseIndexMetaId = table.getBaseIndexMetaId();
        long retiredSchemaId = table.getIndexMetaByMetaId(baseIndexMetaId).getSchemaId();
        LakeTableIndexFastPathJobBase job = submitAlter(table, "ADD INDEX idx_c1 (c1) USING BITMAP");
        assertInstanceOf(LakeTableAddIndexJob.class, job);
        completeTasksAndPublishOnSend();

        runToFinished(job);

        assertPruned(job);
        assertFalse(job.commitVersionMap.isEmpty());
        assertEquals(List.of("idx_c1"), indexNames(table));
        // The schema the flip retired stays resolvable for loads that were bound to it.
        assertTrue(job.getHistorySchema().orElseThrow().getSchemaBySchemaId(retiredSchemaId).isPresent());
        assertJournaledWithTabletData(job.getJobId(), AlterJobV2.JobState.FINISHED);
    }

    @Test
    public void testLeaderPrunesFinishedDropIndexJob() throws Exception {
        OlapTable table = createTable("t_drop_index_finished", "c0 INT, c1 INT, INDEX idx_c1 (c1) USING BITMAP");
        LakeTableIndexFastPathJobBase job = submitAlter(table, "DROP INDEX idx_c1");
        assertInstanceOf(LakeTableDropIndexJob.class, job);
        completeTasksAndPublishOnSend();

        runToFinished(job);

        assertPruned(job);
        assertFalse(job.commitVersionMap.isEmpty());
        assertTrue(indexNames(table).isEmpty());
        assertJournaledWithTabletData(job.getJobId(), AlterJobV2.JobState.FINISHED);
    }

    @Test
    public void testLeaderPrunesCancelledJob() throws Exception {
        OlapTable table = createTable("t_add_index_cancelled", "c0 INT, c1 INT");
        LakeTableIndexFastPathJobBase job = submitAlter(table, "ADD INDEX idx_c1 (c1) USING BITMAP");
        // The alter tasks are sent but never finish.
        new MockUp<AgentTaskExecutor>() {
            @Mock
            public void submit(AgentBatchTask batchTask) {
            }
        };
        job.run();
        assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
        assertHasTabletData(job);
        assertTrue(job.batchTask.getTaskNum() > 0);

        assertTrue(job.cancel("cancel"));

        assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
        assertPruned(job);
        assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        assertTrue(indexNames(table).isEmpty());
        assertJournaledWithTabletData(job.getJobId(), AlterJobV2.JobState.CANCELLED);
    }

    @Test
    public void testFollowerReplayPrunesFinishedJob() throws Exception {
        OlapTable table = createTable("t_add_index_follower", "c0 INT, c1 INT");
        // The follower starts from the catalog as it was before the alter.
        UtFrameUtils.PseudoImage initialImage = saveMetastore();
        LakeTableIndexFastPathJobBase job = submitAlter(table, "ADD INDEX idx_c1 (c1) USING BITMAP");
        completeTasksAndPublishOnSend();
        runToFinished(job);
        List<LakeTableIndexFastPathJobBase> entries = readJournaledEntries(job.getJobId());

        LocalMetastore followerMetastore = loadMetastore(initialImage);
        OlapTable followerTable = (OlapTable) followerMetastore.getTable(DB_NAME, table.getName());
        assertTrue(indexNames(followerTable).isEmpty());
        SchemaChangeHandler followerHandler = new SchemaChangeHandler();
        for (LakeTableIndexFastPathJobBase entry : entries) {
            replayOn(followerMetastore, followerHandler, entry);
        }

        // The handler keeps the first replayed entry and copies the data of each later one into it.
        LakeTableIndexFastPathJobBase retained = entries.get(0);
        assertSame(retained, followerHandler.getAlterJobsV2().get(job.getJobId()));
        assertEquals(AlterJobV2.JobState.FINISHED, retained.getJobState());
        assertPruned(retained);
        for (LakeTableIndexFastPathJobBase entry : entries.subList(1, entries.size())) {
            assertHasTabletData(entry);
        }
        assertEquals(OlapTable.OlapTableState.NORMAL, followerTable.getState());
        assertEquals(List.of("idx_c1"), indexNames(followerTable));
        long baseIndexMetaId = table.getBaseIndexMetaId();
        assertEquals(table.getIndexMetaByMetaId(baseIndexMetaId).getSchemaId(),
                followerTable.getIndexMetaByMetaId(baseIndexMetaId).getSchemaId());
        assertEquals(job.getHistorySchema().orElseThrow().getHistoryTxnIdThreshold(),
                retained.getHistorySchema().orElseThrow().getHistoryTxnIdThreshold());
    }

    @Test
    public void testLoadPrunesDoneJobFromImage() throws Exception {
        OlapTable table = createTable("t_add_index_image", "c0 INT, c1 INT");
        LakeTableIndexFastPathJobBase job = submitAlter(table, "ADD INDEX idx_c1 (c1) USING BITMAP");
        completeTasksAndPublishOnSend();
        runToFinished(job);
        // Images written by older versions hold the done job with its per-tablet data, as the journal entry does.
        LakeTableIndexFastPathJobBase doneJob = lastJournaledEntry(job.getJobId());
        assertEquals(AlterJobV2.JobState.FINISHED, doneJob.getJobState());
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

        LakeTableIndexFastPathJobBase restored = (LakeTableIndexFastPathJobBase)
                restoredMgr.getSchemaChangeHandler().getAlterJobsV2().get(job.getJobId());
        assertPruned(restored);
        assertTrue(restored.getHistorySchema().isPresent());
        List<List<Comparable>> restoredRows = new ArrayList<>();
        restored.getInfo(restoredRows);
        assertFalse(expectedRows.isEmpty());
        assertEquals(expectedRows, restoredRows);
    }

    @Test
    public void testAddIndexJobReplayedFromJournalSendsTheIndexes() throws Exception {
        OlapTable table = createTable("t_add_index_journal", "c0 INT, c1 INT, c2 INT");
        LakeTableIndexFastPathJobBase job = submitAlter(table,
                "ADD INDEX idx_c1 (c1) USING BITMAP, ADD INDEX idx_c2 (c2) USING BITMAP");
        // A follower that takes over as leader, or a restarted FE, holds the job its journal entry recorded.
        LakeTableIndexFastPathJobBase replayed = replayFirstJournaledEntry(job.getJobId());

        List<TAlterTabletReqV2> requests = runUntilDispatched(replayed);
        try {
            for (TAlterTabletReqV2 request : requests) {
                assertTrue(request.isOnly_add_index());
                assertEquals(2, request.getIndexes_to_addSize(), request.toString());
                for (int i = 0; i < 2; i++) {
                    TOlapTableIndex index = request.getIndexes_to_add().get(i);
                    assertEquals("idx_c" + (i + 1), index.getIndex_name());
                    assertEquals(TIndexType.BITMAP, index.getIndex_type());
                    assertEquals(List.of("c" + (i + 1)), index.getColumns());
                }
                assertTrue(request.getNew_index_schema_id() > 0);
            }
        } finally {
            assertTrue(replayed.cancel("test done"));
        }
    }

    @Test
    public void testDropIndexJobReplayedFromJournalSendsTheDroppedIndexes() throws Exception {
        OlapTable table = createTable("t_drop_index_journal",
                "c0 INT, c1 INT, c2 INT, INDEX idx_c1 (c1) USING BITMAP, INDEX idx_c2 (c2) USING BITMAP");
        LakeTableIndexFastPathJobBase job = submitAlter(table, "DROP INDEX idx_c1, DROP INDEX idx_c2");
        LakeTableIndexFastPathJobBase replayed = replayFirstJournaledEntry(job.getJobId());

        List<TAlterTabletReqV2> requests = runUntilDispatched(replayed);
        try {
            for (TAlterTabletReqV2 request : requests) {
                assertTrue(request.isOnly_drop_index());
                assertEquals(2, request.getDrop_indexesSize(), request.toString());
                assertDropEntry(request.getDrop_indexes().get(0), -1L, table.getColumn("c1").getUniqueId(),
                        TIndexType.BITMAP);
                assertDropEntry(request.getDrop_indexes().get(1), -1L, table.getColumn("c2").getUniqueId(),
                        TIndexType.BITMAP);
            }
        } finally {
            assertTrue(replayed.cancel("test done"));
        }
    }

    @Test
    public void testAddIndexJobReplayedFromJournalSkipsRollupWithoutTheColumn() throws Exception {
        // The rollup does not carry c1: its tablets get an empty index set and keep their schema id, exactly as on
        // the FE that built the job, while the base tablets build the index under the new schema id.
        OlapTable table = createTable("t_add_index_rollup_journal", "c0 INT, c1 INT, c2 INT",
                " ROLLUP (r1 (c0, c2))");
        LakeTableIndexFastPathJobBase job = submitAlter(table, "ADD INDEX idx_c1 (c1) USING BITMAP");
        LakeTableIndexFastPathJobBase replayed = replayFirstJournaledEntry(job.getJobId());

        List<TAlterTabletReqV2> requests = runUntilDispatched(replayed);
        try {
            long baseIndexMetaId = table.getBaseIndexMetaId();
            int baseRequests = 0;
            int rollupRequests = 0;
            for (TAlterTabletReqV2 request : requests) {
                if (replayed.tabletToIndexMetaId.get(request.getBase_tablet_id()) == baseIndexMetaId) {
                    baseRequests++;
                    assertEquals(1, request.getIndexes_to_addSize(), request.toString());
                    assertEquals(List.of("c1"), request.getIndexes_to_add().get(0).getColumns());
                    assertTrue(request.getNew_index_schema_id() > 0);
                } else {
                    rollupRequests++;
                    assertEquals(0, request.getIndexes_to_addSize(), request.toString());
                    assertFalse(request.isSetNew_index_schema_id(), request.toString());
                }
            }
            assertTrue(baseRequests > 0 && rollupRequests > 0, requests.toString());
        } finally {
            assertTrue(replayed.cancel("test done"));
        }
    }

    @Test
    public void testAddBloomFilterJobReplayedFromJournalSendsTheBloomFilter() throws Exception {
        OlapTable table = createTable("t_add_bf_journal", "c0 INT, c1 INT");
        LakeTableIndexFastPathJobBase job = submitAlter(table, "SET (\"bloom_filter_columns\" = \"c1\")");
        assertInstanceOf(LakeTableAddIndexJob.class, job);
        LakeTableIndexFastPathJobBase replayed = replayFirstJournaledEntry(job.getJobId());

        List<TAlterTabletReqV2> requests = runUntilDispatched(replayed);
        try {
            for (TAlterTabletReqV2 request : requests) {
                assertEquals(1, request.getIndexes_to_addSize(), request.toString());
                TOlapTableIndex index = request.getIndexes_to_add().get(0);
                assertEquals(TIndexType.BLOOM_FILTER, index.getIndex_type());
                assertEquals(List.of("c1"), index.getColumns());
                assertEquals(Double.toString(FeConstants.DEFAULT_BLOOM_FILTER_FPP),
                        index.getIndex_properties().get("bloom_filter_fpp"));
            }
        } finally {
            assertTrue(replayed.cancel("test done"));
        }
    }

    @Test
    public void testDropBloomFilterJobLoadedFromImageSendsTheDroppedBloomFilter() throws Exception {
        OlapTable table = createTable("t_drop_bf_image", "c0 INT, c1 INT, c2 INT",
                " PROPERTIES (\"bloom_filter_columns\" = \"c1,c2\")");
        LakeTableIndexFastPathJobBase job = submitAlter(table, "SET (\"bloom_filter_columns\" = \"c2\")");
        assertInstanceOf(LakeTableDropIndexJob.class, job);
        // A restarted FE holds the job its image recorded.
        LakeTableIndexFastPathJobBase loaded = saveAndLoadThroughImage(job);

        List<TAlterTabletReqV2> requests = runUntilDispatched(loaded);
        try {
            for (TAlterTabletReqV2 request : requests) {
                assertEquals(1, request.getDrop_indexesSize(), request.toString());
                assertDropEntry(request.getDrop_indexes().get(0), -1L, table.getColumn("c1").getUniqueId(),
                        TIndexType.BLOOM_FILTER);
            }
        } finally {
            assertTrue(loaded.cancel("test done"));
        }
    }

    @Test
    public void testAddIndexJobJournaledByOlderVersionSendsTheIndex() throws Exception {
        // An older FE journaled the payload under "indexesToAdd", each element an empty object: all Gson kept of
        // a Thrift struct. The job loaded from such an entry rebuilds the payload, so a job in flight across the
        // upgrade builds its index instead of finishing as a no-op.
        OlapTable table = createTable("t_add_index_old_journal", "c0 INT, c1 INT");
        LakeTableIndexFastPathJobBase job = submitAlter(table, "ADD INDEX idx_c1 (c1) USING BITMAP");
        SchemaChangeHandler followerHandler = new SchemaChangeHandler();
        followerHandler.replayAlterJobV2(asWrittenByOlderVersion(job, "indexesToAdd", false));
        LakeTableIndexFastPathJobBase replayed =
                (LakeTableIndexFastPathJobBase) followerHandler.getAlterJobsV2().get(job.getJobId());

        List<TAlterTabletReqV2> requests = runUntilDispatched(replayed);
        try {
            for (TAlterTabletReqV2 request : requests) {
                assertEquals(1, request.getIndexes_to_addSize(), request.toString());
                assertEquals("idx_c1", request.getIndexes_to_add().get(0).getIndex_name());
                assertEquals(List.of("c1"), request.getIndexes_to_add().get(0).getColumns());
            }
        } finally {
            assertTrue(replayed.cancel("test done"));
        }
    }

    @Test
    public void testDropIndexJobJournaledByOlderVersionSendsTheDroppedIndex() throws Exception {
        // Same for "dropInfos": BE rejects an empty entry, which used to cancel the job. An older FE also left the
        // index names out of every entry after the submitting one (its copyForPersist did not copy them); the
        // follower keeps the job of the first entry it replays and copies only the base fields of later ones, so
        // the names the payload is rebuilt from survive.
        OlapTable table = createTable("t_drop_index_old_journal", "c0 INT, c1 INT, INDEX idx_c1 (c1) USING BITMAP");
        LakeTableIndexFastPathJobBase job = submitAlter(table, "DROP INDEX idx_c1");
        SchemaChangeHandler followerHandler = new SchemaChangeHandler();
        followerHandler.replayAlterJobV2(asWrittenByOlderVersion(job, "dropInfos", false));
        followerHandler.replayAlterJobV2(asWrittenByOlderVersion(job, "dropInfos", true));
        LakeTableIndexFastPathJobBase replayed =
                (LakeTableIndexFastPathJobBase) followerHandler.getAlterJobsV2().get(job.getJobId());

        List<TAlterTabletReqV2> requests = runUntilDispatched(replayed);
        try {
            for (TAlterTabletReqV2 request : requests) {
                assertEquals(1, request.getDrop_indexesSize(), request.toString());
                assertDropEntry(request.getDrop_indexes().get(0), -1L, table.getColumn("c1").getUniqueId(),
                        TIndexType.BITMAP);
            }
        } finally {
            assertTrue(replayed.cancel("test done"));
        }
    }

    @Test
    public void testIndexJobThatCannotRebuildItsPayloadIsCancelled() throws Exception {
        // The job names an index the table does not have, so there is no payload to send. It must end CANCELLED and
        // give the table back, rather than wait in RUNNING or send BE requests it rejects.
        OlapTable table = createTable("t_drop_index_unresolvable", "c0 INT, c1 INT");
        long dbId = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME).getId();
        LakeTableDropIndexJob job = new LakeTableDropIndexJob(GlobalStateMgr.getCurrentState().getNextId(), dbId,
                table.getId(), table.getName(), 60_000L, List.of(-1L), List.of("idx_gone"));
        job.setComputeResource(WarehouseManager.DEFAULT_RESOURCE);
        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);

        long deadlineMs = System.currentTimeMillis() + 60_000L;
        while (!job.isDone()) {
            assertTrue(System.currentTimeMillis() < deadlineMs, "job is still " + job.getJobState());
            job.run();
            Thread.sleep(10);
        }

        assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
        assertTrue(job.errMsg.contains("idx_gone"), job.errMsg);
        assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
    }

    private static OlapTable createTable(String tableName, String columns) throws Exception {
        return createTable(tableName, columns, null);
    }

    /**
     * Creates a DUPLICATE KEY(c0) table; {@code tail}, when not null, follows the DISTRIBUTED BY clause (ROLLUP,
     * PROPERTIES).
     */
    private static OlapTable createTable(String tableName, String columns, String tail) throws Exception {
        String sql = "CREATE TABLE " + tableName + " (" + columns + ") DUPLICATE KEY(c0) "
                + "DISTRIBUTED BY HASH(c0) BUCKETS 2" + (tail == null ? "" : tail);
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(DB_NAME, tableName);
    }

    /**
     * Runs the ALTER TABLE statement and returns the index fast-path job it submitted, which the test then drives.
     */
    private static LakeTableIndexFastPathJobBase submitAlter(OlapTable table, String alterClause) throws Exception {
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "ALTER TABLE " + table.getName() + " " + alterClause, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, stmt);
        SchemaChangeHandler handler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        List<AlterJobV2> jobs = handler.getUnfinishedAlterJobV2ByTableId(table.getId());
        handler.clearJobs();
        assertEquals(1, jobs.size());
        return assertInstanceOf(LakeTableIndexFastPathJobBase.class, jobs.get(0));
    }

    /**
     * Marks every alter task finished as soon as it is sent, and lets the publish succeed without reaching a node.
     */
    private static void completeTasksAndPublishOnSend() {
        new MockUp<AgentTaskExecutor>() {
            @Mock
            public void submit(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }
        };
        new MockUp<LakeTableIndexFastPathJobBase>() {
            @Mock
            public boolean lakePublishVersion() {
                return true;
            }
        };
    }

    private static void runToFinished(LakeTableIndexFastPathJobBase job) throws Exception {
        long deadlineMs = System.currentTimeMillis() + 60_000L;
        while (!job.isDone()) {
            assertTrue(System.currentTimeMillis() < deadlineMs, "job is still " + job.getJobState());
            job.run();
            Thread.sleep(10);
        }
        assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState(), job.errMsg);
    }

    private static List<String> indexNames(OlapTable table) {
        return table.getIndexes().stream().map(Index::getIndexName).collect(Collectors.toList());
    }

    /**
     * Reads the journal entries of the job, from the one that submitted it to the one of its final state.
     */
    private static List<LakeTableIndexFastPathJobBase> readJournaledEntries(long jobId) throws Exception {
        List<LakeTableIndexFastPathJobBase> entries = new ArrayList<>();
        AlterJobV2 entry;
        do {
            entry = (AlterJobV2) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_JOB_V2);
            assertEquals(jobId, entry.getJobId());
            entries.add((LakeTableIndexFastPathJobBase) entry);
        } while (!entry.isDone());
        assertEquals(AlterJobV2.JobState.PENDING, entries.get(0).getJobState());
        return entries;
    }

    /**
     * Replays the journal entry that submitted the job into a fresh handler, as a follower does, and returns the job
     * the follower then holds.
     */
    private static LakeTableIndexFastPathJobBase replayFirstJournaledEntry(long jobId) throws Exception {
        AlterJobV2 entry = (AlterJobV2) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(
                OperationType.OP_ALTER_JOB_V2);
        assertEquals(jobId, entry.getJobId());
        assertEquals(AlterJobV2.JobState.PENDING, entry.getJobState());
        SchemaChangeHandler followerHandler = new SchemaChangeHandler();
        followerHandler.replayAlterJobV2(entry);
        return (LakeTableIndexFastPathJobBase) followerHandler.getAlterJobsV2().get(jobId);
    }

    /**
     * The journal entry of the job as an older FE wrote it: its BE payload under {@code payloadKey} as one empty
     * object and, for an entry written after the submitting one ({@code laterEntry}), no index names.
     */
    private static AlterJobV2 asWrittenByOlderVersion(AlterJobV2 job, String payloadKey, boolean laterEntry) {
        JsonObject json = JsonParser.parseString(GsonUtils.GSON.toJson(job)).getAsJsonObject();
        assertFalse(json.has(payloadKey), json.toString());
        JsonArray lostPayload = new JsonArray();
        lostPayload.add(new JsonObject());
        json.add(payloadKey, lostPayload);
        if (laterEntry) {
            json.add("dropIndexNames", new JsonArray());
        }
        return GsonUtils.GSON.fromJson(json, AlterJobV2.class);
    }

    private static Column columnWithUniqueId(String name, int uniqueId) {
        Column column = new Column(name, IntegerType.BIGINT);
        column.setUniqueId(uniqueId);
        return column;
    }

    /**
     * Writes the unfinished job into an image and returns the job an FE loading that image holds.
     */
    private static LakeTableIndexFastPathJobBase saveAndLoadThroughImage(AlterJobV2 job) throws Exception {
        AlterJobMgr alterJobMgr = new AlterJobMgr(new SchemaChangeHandler(), new MaterializedViewHandler(),
                new SystemHandler());
        alterJobMgr.getSchemaChangeHandler().addAlterJobV2(job);
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        alterJobMgr.save(image.getImageWriter());
        AlterJobMgr restoredMgr = new AlterJobMgr(new SchemaChangeHandler(), new MaterializedViewHandler(),
                new SystemHandler());
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        restoredMgr.load(reader);
        reader.close();
        AlterJobV2 loaded = restoredMgr.getSchemaChangeHandler().getAlterJobsV2().get(job.getJobId());
        assertNotNull(loaded);
        return (LakeTableIndexFastPathJobBase) loaded;
    }

    /**
     * Runs the job as the leader until it sends its alter tasks, and returns the request each task carries. The tasks
     * are never delivered, so the job stays RUNNING.
     */
    private static List<TAlterTabletReqV2> runUntilDispatched(LakeTableIndexFastPathJobBase job) throws Exception {
        new MockUp<AgentTaskExecutor>() {
            @Mock
            public void submit(AgentBatchTask batchTask) {
            }
        };
        long deadlineMs = System.currentTimeMillis() + 60_000L;
        while (job.batchTask == null) {
            assertFalse(job.isDone(), "job is " + job.getJobState() + ": " + job.errMsg);
            assertTrue(System.currentTimeMillis() < deadlineMs, "job is still " + job.getJobState());
            job.run();
            Thread.sleep(10);
        }
        List<TAlterTabletReqV2> requests = new ArrayList<>();
        for (AgentTask task : job.batchTask.getAllTasks()) {
            requests.add(((AlterReplicaTask) task).toThrift());
        }
        assertFalse(requests.isEmpty());
        return requests;
    }

    private static void assertDropEntry(TDropIndexInfo info, long indexId, int colUniqueId, TIndexType indexType) {
        assertTrue(info.isSetIndex_id() && info.isSetCol_unique_id() && info.isSetIndex_type(), info.toString());
        assertEquals(indexId, info.getIndex_id());
        assertEquals(colUniqueId, info.getCol_unique_id());
        assertEquals(indexType, info.getIndex_type());
    }

    private static LakeTableIndexFastPathJobBase lastJournaledEntry(long jobId) throws Exception {
        List<LakeTableIndexFastPathJobBase> entries = readJournaledEntries(jobId);
        return entries.get(entries.size() - 1);
    }

    /**
     * The journal entries keep the per-tablet data the live job dropped, and SHOW ALTER of the final one does not
     * depend on it.
     */
    private static void assertJournaledWithTabletData(long jobId, AlterJobV2.JobState finalState) throws Exception {
        List<LakeTableIndexFastPathJobBase> entries = readJournaledEntries(jobId);
        // The job is journaled once on submission, before it collects the tablets.
        for (LakeTableIndexFastPathJobBase entry : entries.subList(1, entries.size())) {
            assertHasTabletData(entry);
        }
        LakeTableIndexFastPathJobBase finalEntry = entries.get(entries.size() - 1);
        assertEquals(finalState, finalEntry.getJobState());
        List<List<Comparable>> rowsBefore = new ArrayList<>();
        finalEntry.getInfo(rowsBefore);
        finalEntry.pruneMeta();
        assertPruned(finalEntry);
        List<List<Comparable>> rowsAfter = new ArrayList<>();
        finalEntry.getInfo(rowsAfter);
        assertFalse(rowsBefore.isEmpty());
        assertEquals(rowsBefore, rowsAfter);
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
     * The job's per-tablet data: the tablets it alters, grouped by partition, and the index of each.
     */
    private static void assertHasTabletData(LakeTableIndexFastPathJobBase job) {
        assertFalse(job.partitionToTablets.isEmpty());
        assertFalse(job.tabletToIndexMetaId.isEmpty());
    }

    private static void assertPruned(LakeTableIndexFastPathJobBase job) {
        assertTrue(job.partitionToTablets.isEmpty());
        assertTrue(job.tabletToIndexMetaId.isEmpty());
        assertNull(job.batchTask);
    }
}
