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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.thrift.TDropIndexInfo;
import com.starrocks.thrift.TIndexType;
import com.starrocks.thrift.TOlapTableIndex;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Light unit coverage for the Lake ADD/DROP INDEX fast-path Job classes.
 * We test the shape / serialization-friendliness of the classes rather than
 * the full AlterJobV2 lifecycle (which needs a running catalog). The
 * lifecycle is exercised by e2e SQL tests in a separate follow-up.
 */
public class LakeTableIndexFastPathJobTest {

    @Test
    public void testAddIndexJobConstruction() {
        long indexId = 101L;
        Index ix = new Index(indexId, "ix_a", Collections.singletonList(ColumnId.create("c1")),
                IndexDef.IndexType.BITMAP, "", new java.util.HashMap<>());
        TOlapTableIndex thrift = new TOlapTableIndex();
        thrift.setIndex_name("ix_a");
        thrift.setIndex_id(indexId);
        thrift.setIndex_type(TIndexType.BITMAP);
        thrift.setColumns(Collections.singletonList("c1"));

        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(ix), Collections.singletonList(thrift));

        assertEquals(AlterJobV2.JobState.PENDING, job.getJobState());
        assertEquals(AlterJobV2.JobType.SCHEMA_CHANGE, job.getType());
        assertEquals(1, job.getNewIndexes().size());
        assertEquals(1, job.getIndexesToAdd().size());
        assertEquals("ix_a", job.getNewIndexes().get(0).getIndexName());
    }

    @Test
    public void testAddIndexJobCopyForPersist() {
        Index ix = new Index(5L, "ix_b", Collections.singletonList(ColumnId.create("c2")),
                IndexDef.IndexType.NGRAMBF, "", new java.util.HashMap<>());
        TOlapTableIndex thrift = new TOlapTableIndex();
        thrift.setIndex_name("ix_b");
        thrift.setIndex_id(5L);
        thrift.setIndex_type(TIndexType.NGRAMBF);
        thrift.setColumns(Collections.singletonList("c2"));

        LakeTableAddIndexJob job = new LakeTableAddIndexJob(7L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(ix), Collections.singletonList(thrift));

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
        TDropIndexInfo info = new TDropIndexInfo();
        info.setIndex_id(201L);
        info.setCol_unique_id(7);
        info.setIndex_type(TIndexType.BITMAP);

        LakeTableDropIndexJob job = new LakeTableDropIndexJob(10L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(201L), Collections.singletonList(info));

        assertEquals(AlterJobV2.JobState.PENDING, job.getJobState());
        assertEquals(1, job.getDropIndexIds().size());
        assertEquals(1, job.getDropInfos().size());
        assertEquals(201L, job.getDropIndexIds().get(0));
    }

    @Test
    public void testDropIndexJobCopyForPersist() {
        List<TDropIndexInfo> infos = new ArrayList<>();
        TDropIndexInfo info = new TDropIndexInfo();
        info.setIndex_id(201L);
        info.setCol_unique_id(7);
        info.setIndex_type(TIndexType.NGRAMBF);
        infos.add(info);

        LakeTableDropIndexJob job = new LakeTableDropIndexJob(11L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(201L), infos);

        AlterJobV2 copy = job.copyForPersist();
        assertInstanceOf(LakeTableDropIndexJob.class, copy);
        LakeTableDropIndexJob lc = (LakeTableDropIndexJob) copy;
        assertEquals(job.getJobId(), lc.getJobId());
        assertEquals(job.getDropInfos().size(), lc.getDropInfos().size());
        assertTrue(lc.getDropInfos() != job.getDropInfos());
    }

    @Test
    public void testTransactionIdNotSetBeforeWatershed() {
        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L,
                Collections.emptyList(), Collections.emptyList());
        // watershedTxnId defaults to -1 before runPendingJob allocates it.
        assertTrue(job.getTransactionId().isEmpty());
    }

    // -----------------------------------------------------------------
    // applyCatalogMutation: ADD INDEX path (no BF columns)
    // -----------------------------------------------------------------

    @Test
    public void testAddIndexJob_ApplyCatalogMutation_AppendsIndex() {
        Index ix = new Index(101L, "ix_a", Collections.singletonList(ColumnId.create("c1")),
                IndexDef.IndexType.BITMAP, "", new java.util.HashMap<>());
        TOlapTableIndex thrift = new TOlapTableIndex();
        thrift.setIndex_type(TIndexType.BITMAP);
        thrift.setColumns(Collections.singletonList("c1"));

        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(ix), Collections.singletonList(thrift));

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

    // -----------------------------------------------------------------
    // applyCatalogMutation: ADD plain BF columns
    // -----------------------------------------------------------------

    @Test
    public void testAddIndexJob_ApplyCatalogMutation_MergesAddBfColumns() {
        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L,
                new ArrayList<>(), new ArrayList<>(),
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
                new ArrayList<>(), new ArrayList<>(),
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
                new ArrayList<>(), new ArrayList<>(),
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
        TDropIndexInfo info = new TDropIndexInfo();
        info.setIndex_id(201L);
        info.setIndex_type(TIndexType.BITMAP);
        info.setCol_unique_id(7);

        LakeTableDropIndexJob job = new LakeTableDropIndexJob(1L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(201L), Collections.singletonList(info));

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
        TDropIndexInfo info = new TDropIndexInfo();
        info.setIndex_id(-1L);
        info.setIndex_type(TIndexType.BITMAP);
        info.setCol_unique_id(7);

        LakeTableDropIndexJob job = new LakeTableDropIndexJob(1L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(-1L),
                Collections.singletonList("A_bm"),
                Collections.singletonList(info));

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
                new ArrayList<Long>(), new ArrayList<String>(), new ArrayList<TDropIndexInfo>(),
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
                new ArrayList<Long>(), new ArrayList<String>(), new ArrayList<TDropIndexInfo>(),
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
                new ArrayList<Long>(), new ArrayList<String>(), new ArrayList<TDropIndexInfo>(),
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
                new ArrayList<>(), new ArrayList<>(),
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
                new ArrayList<Long>(), new ArrayList<String>(), new ArrayList<TDropIndexInfo>(),
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
        TOlapTableIndex t = new TOlapTableIndex();
        t.setIndex_type(TIndexType.BITMAP);
        LakeTableAddIndexJob job = new LakeTableAddIndexJob(1L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(ix), Collections.singletonList(t),
                List.of("c1"));
        assertEquals(1, job.getNewIndexes().size());
        assertEquals(1, job.getIndexesToAdd().size());
        assertEquals(1, job.getAddBfColumns().size());
    }

    @Test
    public void testDropIndexJob_AccessorsExposeFields() {
        TDropIndexInfo info = new TDropIndexInfo();
        info.setIndex_id(1L);
        info.setIndex_type(TIndexType.BITMAP);
        info.setCol_unique_id(0);
        LakeTableDropIndexJob job = new LakeTableDropIndexJob(1L, 2L, 3L, "t", 60_000L,
                Collections.singletonList(1L), Collections.singletonList("idx_name"),
                Collections.singletonList(info), List.of("c1"));
        assertEquals(1, job.getDropIndexIds().size());
        assertEquals(1, job.getDropInfos().size());
        assertEquals(1, job.getDropBfColumns().size());
    }

    @Test
    public void testAddIndexJob_DefaultCtorForGsonDeserialization() {
        // Default constructor used by Gson; required to keep the class
        // round-trippable through the editlog.
        LakeTableAddIndexJob job = new LakeTableAddIndexJob();
        assertEquals(AlterJobV2.JobType.SCHEMA_CHANGE, job.getType());
        assertNotNull(job.getNewIndexes());
        assertNotNull(job.getIndexesToAdd());
        assertNotNull(job.getAddBfColumns());
    }

    @Test
    public void testDropIndexJob_DefaultCtorForGsonDeserialization() {
        LakeTableDropIndexJob job = new LakeTableDropIndexJob();
        assertEquals(AlterJobV2.JobType.SCHEMA_CHANGE, job.getType());
        assertNotNull(job.getDropIndexIds());
        assertNotNull(job.getDropInfos());
        assertNotNull(job.getDropBfColumns());
    }
}
