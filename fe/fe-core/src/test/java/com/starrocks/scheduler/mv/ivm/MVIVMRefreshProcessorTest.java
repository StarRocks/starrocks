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

package com.starrocks.scheduler.mv.ivm;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.tvr.TvrDeltaStats;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrTableDeltaTrait;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.scheduler.mv.BaseTableSnapshotInfo;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.TableRelation;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MVIVMRefreshProcessorTest {

    private static final long MAX_BYTES = Long.MAX_VALUE;
    private static final long MAX_ROWS = Long.MAX_VALUE;
    private static final BaseTableInfo TABLE = Mockito.mock(BaseTableInfo.class);

    /**
     * Build the trait list {@code IcebergMetadata.listTableDeltaTraits} emits for an all-append chain
     * {@code base(excl) -> base+1 -> ... -> base+rows.length}: trait k carries delta
     * {@code (base+k+1, base+k+2]} with snapshot {@code base+k+1}'s added rows, and the newest trait is
     * the degenerate {@code (head, head]}. The per-trait stat is shifted one snapshot off its delta range,
     * but the cumulative is exact, which is what the cap accounting relies on.
     */
    private static List<TvrTableDeltaTrait> appendChain(long base, long... rows) {
        List<TvrTableDeltaTrait> traits = new ArrayList<>();
        int n = rows.length;
        for (int k = 0; k < n; k++) {
            long from = base + k + 1;
            long to = (k + 1 < n) ? base + k + 2 : base + n;
            traits.add(TvrTableDeltaTrait.ofMonotonic(TvrTableDelta.of(from, to), new TvrDeltaStats(rows[k], 0L)));
        }
        return traits;
    }

    @Test
    public void fineGrainedCommitsBatchToCap() {
        List<TvrTableDeltaTrait> traits = appendChain(10, 1, 1, 1, 1, 1, 1);
        MVIVMRefreshProcessor.AdaptiveDelta r =
                MVIVMRefreshProcessor.computeAdaptiveDelta(TABLE, traits, TvrTableDelta.of(10L, 16L), 2, MAX_BYTES);
        assertEquals(TvrTableDelta.of(10L, 12L), r.delta);
        assertTrue(r.hasNext);
    }

    @Test
    public void singleOversizedFirstCommitFormsItsOwnBatch() {
        // Regression: the first commit alone exceeds the cap; it must become its own batch (10,12] with a
        // follow-up, not swallow the whole (10,14] in one run.
        List<TvrTableDeltaTrait> traits = appendChain(10, 5, 1, 1, 1);
        MVIVMRefreshProcessor.AdaptiveDelta r =
                MVIVMRefreshProcessor.computeAdaptiveDelta(TABLE, traits, TvrTableDelta.of(10L, 14L), 2, MAX_BYTES);
        assertEquals(TvrTableDelta.of(10L, 12L), r.delta);
        assertTrue(r.hasNext);
    }

    @Test
    public void singleCommitOverCapWithNothingElseRefreshesWholeRange() {
        // A single snapshot cannot be subdivided; refresh it whole, no follow-up.
        List<TvrTableDeltaTrait> traits = appendChain(10, 10);
        MVIVMRefreshProcessor.AdaptiveDelta r =
                MVIVMRefreshProcessor.computeAdaptiveDelta(TABLE, traits, TvrTableDelta.of(10L, 11L), 2, MAX_BYTES);
        assertEquals(TvrTableDelta.of(10L, 11L), r.delta);
        assertFalse(r.hasNext);
    }

    @Test
    public void wholeRangeUnderCapRefreshesAtOnce() {
        List<TvrTableDeltaTrait> traits = appendChain(10, 1, 1, 1);
        MVIVMRefreshProcessor.AdaptiveDelta r =
                MVIVMRefreshProcessor.computeAdaptiveDelta(TABLE, traits, TvrTableDelta.of(10L, 13L), 100, MAX_BYTES);
        assertEquals(TvrTableDelta.of(10L, 13L), r.delta);
        assertFalse(r.hasNext);
    }

    @Test
    public void byteCapSchedulesFollowUp() {
        // Regression: the byte cap must also schedule a follow-up (previously only the row cap did).
        List<TvrTableDeltaTrait> traits = List.of(
                TvrTableDeltaTrait.ofMonotonic(TvrTableDelta.of(11L, 12L), new TvrDeltaStats(0L, 100L)),
                TvrTableDeltaTrait.ofMonotonic(TvrTableDelta.of(12L, 13L), new TvrDeltaStats(0L, 100L)),
                TvrTableDeltaTrait.ofMonotonic(TvrTableDelta.of(13L, 13L), new TvrDeltaStats(0L, 100L)));
        MVIVMRefreshProcessor.AdaptiveDelta r =
                MVIVMRefreshProcessor.computeAdaptiveDelta(TABLE, traits, TvrTableDelta.of(10L, 13L), MAX_ROWS, 150L);
        assertEquals(TvrTableDelta.of(10L, 12L), r.delta);
        assertTrue(r.hasNext);
    }

    @Test
    public void emptyStatsSingleTraitRefreshesWholeRange() {
        // Mirrors today's cloud-native bookmark source (single trait, empty stats): the cap cannot bite,
        // so the whole range refreshes in one run. The cloud-native source fix is a separate change.
        List<TvrTableDeltaTrait> traits =
                List.of(TvrTableDeltaTrait.ofMonotonic(TvrTableDelta.of(10L, 16L), TvrDeltaStats.EMPTY));
        MVIVMRefreshProcessor.AdaptiveDelta r =
                MVIVMRefreshProcessor.computeAdaptiveDelta(TABLE, traits, TvrTableDelta.of(10L, 16L), 2, MAX_BYTES);
        assertEquals(TvrTableDelta.of(10L, 16L), r.delta);
        assertFalse(r.hasNext);
    }

    @Test
    public void nonAppendOnlyDeltaThrows() {
        List<TvrTableDeltaTrait> traits = List.of(TvrTableDeltaTrait.ofRetractable(TvrTableDelta.of(11L, 12L)));
        assertThrows(SemanticException.class,
                () -> MVIVMRefreshProcessor.computeAdaptiveDelta(TABLE, traits, TvrTableDelta.of(10L, 12L), 2, MAX_BYTES));
    }

    // Same-named base tables across databases used to collide under a name key.
    @Test
    public void joinOfSameNamedBaseTablesAcrossDatabasesBindsEachByIdentifier() {
        TvrTableDelta rangeA = TvrTableDelta.of(0L, 1L);
        TvrTableDelta rangeB = TvrTableDelta.of(0L, 2L);
        List<BaseTableSnapshotInfo> snapshotInfos = List.of(
                mockSnapshotInfo("tj", "tj:uuid_a", "db_a", rangeA),
                mockSnapshotInfo("tj", "tj:uuid_b", "db_b", rangeB));

        TableRelation relationA = mockRelation("tj", "tj:uuid_a");
        TableRelation relationB = mockRelation("tj", "tj:uuid_b");
        Multimap<String, TableRelation> tableRelations = ArrayListMultimap.create();
        tableRelations.put("tj", relationA);
        tableRelations.put("tj", relationB);

        MVIVMRefreshProcessor.bindBaseTableTvrVersionRanges(snapshotInfos, tableRelations);

        Mockito.verify(relationA).setTvrVersionRange(rangeA);
        Mockito.verify(relationB).setTvrVersionRange(rangeB);
    }

    @Test
    public void relationWithoutMatchingChangedRangeThrows() {
        List<BaseTableSnapshotInfo> snapshotInfos =
                List.of(mockSnapshotInfo("tj", "tj:uuid_a", "db_a", TvrTableDelta.of(0L, 1L)));
        Multimap<String, TableRelation> tableRelations = ArrayListMultimap.create();
        tableRelations.put("tj", mockRelation("tj", "tj:uuid_absent"));
        assertThrows(SemanticException.class,
                () -> MVIVMRefreshProcessor.bindBaseTableTvrVersionRanges(snapshotInfos, tableRelations));
    }

    @Test
    public void baseTableWithoutTvrRangeThrows() {
        List<BaseTableSnapshotInfo> snapshotInfos = List.of(mockSnapshotInfo("tj", "tj:uuid_a", "db_a", null));
        assertThrows(SemanticException.class, () ->
                MVIVMRefreshProcessor.bindBaseTableTvrVersionRanges(snapshotInfos, ArrayListMultimap.create()));
    }

    private static BaseTableSnapshotInfo mockSnapshotInfo(String tableName, String tableIdentifier,
                                                          String dbName, TvrVersionRange tvrSnapshot) {
        BaseTableInfo baseTableInfo = Mockito.mock(BaseTableInfo.class);
        Mockito.when(baseTableInfo.getTableName()).thenReturn(tableName);
        Mockito.when(baseTableInfo.getTableIdentifier()).thenReturn(tableIdentifier);
        Mockito.when(baseTableInfo.getDbName()).thenReturn(dbName);
        TvrTableSnapshotInfo snapshotInfo = Mockito.mock(TvrTableSnapshotInfo.class);
        Mockito.when(snapshotInfo.getBaseTableInfo()).thenReturn(baseTableInfo);
        Mockito.when(snapshotInfo.getTvrSnapshot()).thenReturn(tvrSnapshot);
        return snapshotInfo;
    }

    private static TableRelation mockRelation(String tableName, String tableIdentifier) {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.getName()).thenReturn(tableName);
        Mockito.when(table.getTableIdentifier()).thenReturn(tableIdentifier);
        TableRelation relation = Mockito.mock(TableRelation.class);
        Mockito.when(relation.getTable()).thenReturn(table);
        Mockito.when(relation.getName()).thenReturn(new TableName("db", tableName));
        return relation;
    }
}
