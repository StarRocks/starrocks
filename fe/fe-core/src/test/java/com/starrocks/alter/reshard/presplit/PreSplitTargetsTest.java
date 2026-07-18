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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.metric.MetricRepo;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.type.ArrayType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PreSplitTargets#findEligibleTable}, the table-level
 * structural eligibility gate shared by both load hooks and the multi-partition
 * coordinator's defensive re-check, plus {@link PreSplitTargets#findEligibleTarget}
 * and {@link PreSplitTargets#resolveVisibleIndexTargets}.
 *
 * <p>Each {@code findEligibleTable} test stubs an {@link OlapTable} that passes
 * every check up to the one under test, then asserts the gate returns exactly
 * that branch's {@link SkipReason}. The {@code null} (eligible) path is also
 * pinned as a positive control.
 */
public class PreSplitTargetsTest {

    private static final long BASE_INDEX_META_ID = 10L;
    private static final long ROLLUP_INDEX_META_ID = 20L;

    private static OlapTable tableThatPassesUpTo() {
        OlapTable table = mock(OlapTable.class);
        when(table.isCloudNativeTableOrMaterializedView()).thenReturn(true);
        when(table.isRangeDistribution()).thenReturn(true);
        when(table.getState()).thenReturn(OlapTable.OlapTableState.NORMAL);
        MaterializedIndexMeta baseMeta = mock(MaterializedIndexMeta.class);
        when(baseMeta.getIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        when(table.getVisibleIndexMetas()).thenReturn(List.of(baseMeta));
        return table;
    }

    @Test
    public void rejectsNonCloudNativeTable() {
        OlapTable table = mock(OlapTable.class);
        when(table.isCloudNativeTableOrMaterializedView()).thenReturn(false);
        when(table.isRangeDistribution()).thenReturn(true);
        Assertions.assertEquals(SkipReason.NOT_CLOUD_NATIVE,
                PreSplitTargets.findEligibleTable(mock(Database.class), table));
    }

    @Test
    public void rejectsNonRangeDistribution() {
        OlapTable table = mock(OlapTable.class);
        when(table.isCloudNativeTableOrMaterializedView()).thenReturn(true);
        when(table.isRangeDistribution()).thenReturn(false);
        Assertions.assertEquals(SkipReason.NOT_RANGE_DISTRIBUTION,
                PreSplitTargets.findEligibleTable(mock(Database.class), table));
    }

    @Test
    public void rejectsNonNormalTableState() {
        OlapTable table = mock(OlapTable.class);
        when(table.isCloudNativeTableOrMaterializedView()).thenReturn(true);
        when(table.isRangeDistribution()).thenReturn(true);
        when(table.getState()).thenReturn(OlapTable.OlapTableState.SCHEMA_CHANGE);
        Assertions.assertEquals(SkipReason.TABLE_NOT_NORMAL,
                PreSplitTargets.findEligibleTable(mock(Database.class), table));
    }

    @Test
    public void rejectsEmptySortKey() {
        OlapTable table = tableThatPassesUpTo();
        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class)) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table, BASE_INDEX_META_ID))
                    .thenReturn(List.of());
            Assertions.assertEquals(SkipReason.UNSUPPORTED_SORT_KEY,
                    PreSplitTargets.findEligibleTable(mock(Database.class), table));
        }
    }

    @Test
    public void rejectsNonScalarSortKeyColumn() {
        OlapTable table = tableThatPassesUpTo();
        Column arrayColumn = new Column("arr", new ArrayType(IntegerType.INT));
        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class)) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table, BASE_INDEX_META_ID))
                    .thenReturn(List.of(arrayColumn));
            Assertions.assertEquals(SkipReason.UNSUPPORTED_SORT_KEY,
                    PreSplitTargets.findEligibleTable(mock(Database.class), table));
        }
    }

    @Test
    public void findEligibleTable_singleIndex_ok() {
        // Positive control: range-distribution, NORMAL, single index, scalar sort key -> eligible (null).
        OlapTable table = tableThatPassesUpTo();
        Column scalarColumn = new Column("k", IntegerType.BIGINT);
        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class)) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table, BASE_INDEX_META_ID))
                    .thenReturn(List.of(scalarColumn));
            Assertions.assertNull(PreSplitTargets.findEligibleTable(mock(Database.class), table),
                    "fully eligible table must return null (no skip reason)");
        }
    }

    @Test
    public void findEligibleTable_baseAndScalarRollup_ok() {
        // A visible rollup with its own scalar sort key no longer disqualifies the table --
        // the `getVisibleIndexMetas().size() != 1` gate is gone; every visible index's sort
        // key is checked independently instead.
        OlapTable table = mock(OlapTable.class);
        when(table.isCloudNativeTableOrMaterializedView()).thenReturn(true);
        when(table.isRangeDistribution()).thenReturn(true);
        when(table.getState()).thenReturn(OlapTable.OlapTableState.NORMAL);
        MaterializedIndexMeta baseMeta = mock(MaterializedIndexMeta.class);
        when(baseMeta.getIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        MaterializedIndexMeta rollupMeta = mock(MaterializedIndexMeta.class);
        when(rollupMeta.getIndexMetaId()).thenReturn(ROLLUP_INDEX_META_ID);
        when(table.getVisibleIndexMetas()).thenReturn(List.of(baseMeta, rollupMeta));

        Column baseSortKey = new Column("k", IntegerType.BIGINT);
        Column rollupSortKey = new Column("k2", IntegerType.BIGINT);
        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class)) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table, BASE_INDEX_META_ID))
                    .thenReturn(List.of(baseSortKey));
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table, ROLLUP_INDEX_META_ID))
                    .thenReturn(List.of(rollupSortKey));
            Assertions.assertNull(PreSplitTargets.findEligibleTable(mock(Database.class), table),
                    "base + scalar-sort-key rollup must be eligible (null)");
        }
    }

    @Test
    public void findEligibleTable_rollupNonScalarSortKey_unsupported() {
        // Base index is fine; the rollup's sort key has a non-scalar column -> UNSUPPORTED_SORT_KEY.
        OlapTable table = mock(OlapTable.class);
        when(table.isCloudNativeTableOrMaterializedView()).thenReturn(true);
        when(table.isRangeDistribution()).thenReturn(true);
        when(table.getState()).thenReturn(OlapTable.OlapTableState.NORMAL);
        MaterializedIndexMeta baseMeta = mock(MaterializedIndexMeta.class);
        when(baseMeta.getIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        MaterializedIndexMeta rollupMeta = mock(MaterializedIndexMeta.class);
        when(rollupMeta.getIndexMetaId()).thenReturn(ROLLUP_INDEX_META_ID);
        when(table.getVisibleIndexMetas()).thenReturn(List.of(baseMeta, rollupMeta));

        Column baseSortKey = new Column("k", IntegerType.BIGINT);
        Column rollupArrayColumn = new Column("arr", new ArrayType(IntegerType.INT));
        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class)) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table, BASE_INDEX_META_ID))
                    .thenReturn(List.of(baseSortKey));
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table, ROLLUP_INDEX_META_ID))
                    .thenReturn(List.of(rollupArrayColumn));
            Assertions.assertEquals(SkipReason.UNSUPPORTED_SORT_KEY,
                    PreSplitTargets.findEligibleTable(mock(Database.class), table));
        }
    }

    /**
     * Stub an {@link OlapTable} whose single physical partition's base index
     * holds {@code tabletCount} tablets. {@code tabletCount < 0} stubs a missing
     * base index instead.
     */
    private static OlapTable tableWithSinglePartition(int tabletCount) {
        OlapTable table = mock(OlapTable.class);
        when(table.getBaseIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        PhysicalPartition partition = mock(PhysicalPartition.class);
        when(partition.getId()).thenReturn(100L);
        when(table.getPhysicalPartitions()).thenReturn(List.of(partition));
        if (tabletCount < 0) {
            when(partition.getIndex(BASE_INDEX_META_ID)).thenReturn(null);
        } else {
            MaterializedIndex baseIndex = mock(MaterializedIndex.class);
            when(partition.getIndex(BASE_INDEX_META_ID)).thenReturn(baseIndex);
            when(baseIndex.getMetaId()).thenReturn(BASE_INDEX_META_ID);
            List<Tablet> tablets = new ArrayList<>();
            for (int i = 0; i < tabletCount; i++) {
                Tablet tablet = mock(Tablet.class);
                when(tablet.getId()).thenReturn(1000L + i);
                tablets.add(tablet);
            }
            when(baseIndex.getTablets()).thenReturn(tablets);
            when(partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE))
                    .thenReturn(List.of(baseIndex));
        }
        return table;
    }

    @Test
    public void resolvesSinglePartitionSingleTabletTarget() {
        // Positive control: exactly one physical partition with one base tablet -> target.
        OlapTable table = tableWithSinglePartition(1);
        Column sortKey = new Column("k", IntegerType.BIGINT);
        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class)) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table, BASE_INDEX_META_ID))
                    .thenReturn(List.of(sortKey));
            PreSplitTargets.EligibleTarget target =
                    PreSplitTargets.findEligibleTarget(mock(Database.class), table);
            Assertions.assertNotNull(target);
            Assertions.assertEquals(100L, target.partitionId());
            Assertions.assertEquals(1000L, target.oldTabletId());
        }
    }

    @Test
    public void skipsWhenNoUniquePhysicalPartition() {
        // Zero or multiple physical partitions cannot resolve a single target.
        OlapTable table = mock(OlapTable.class);
        when(table.getPhysicalPartitions()).thenReturn(List.of(
                mock(PhysicalPartition.class), mock(PhysicalPartition.class)));
        assertResolveSkips(table, SkipReason.METADATA_NOT_RESOLVED);
    }

    @Test
    public void skipsWhenBaseIndexMissing() {
        // Base index gone (catalog raced an alter) -> metadata-not-resolved.
        assertResolveSkips(tableWithSinglePartition(-1), SkipReason.METADATA_NOT_RESOLVED);
    }

    @Test
    public void skipsWhenPartitionAlreadySplit() {
        // The common re-load case: the partition was already split into multiple tablets.
        assertResolveSkips(tableWithSinglePartition(4), SkipReason.MULTIPLE_BASE_INDEX_TABLETS);
    }

    @Test
    public void skipsWhenPartitionHasNoTablet() {
        // Zero base tablets also folds into the multi-tablet bucket (index present, count != 1).
        assertResolveSkips(tableWithSinglePartition(0), SkipReason.MULTIPLE_BASE_INDEX_TABLETS);
    }

    /**
     * Asserts {@code findEligibleTarget} returns {@code null} and bumps the
     * {@code eligibility_skipped} counter under {@code expectedReason} exactly
     * once — the recording is internal to the resolver, so callers (the
     * single-partition hooks) need not remember it.
     */
    private static void assertResolveSkips(OlapTable table, SkipReason expectedReason) {
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            long baseline = skipBucket(expectedReason);
            Assertions.assertNull(PreSplitTargets.findEligibleTarget(mock(Database.class), table),
                    "ineligible target must resolve to null");
            Assertions.assertEquals(baseline + 1L, skipBucket(expectedReason),
                    expectedReason.name().toLowerCase() + " bucket must increment by one");
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    private static long skipBucket(SkipReason reason) {
        return MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                .getMetric(reason.name().toLowerCase()).getValue().longValue();
    }

    // ---------- resolveVisibleIndexTargets ----------

    private static MaterializedIndex mockIndexWithOneTablet(long metaId, long tabletId) {
        MaterializedIndex index = mock(MaterializedIndex.class);
        when(index.getMetaId()).thenReturn(metaId);
        Tablet tablet = mock(Tablet.class);
        when(tablet.getId()).thenReturn(tabletId);
        when(index.getTablets()).thenReturn(List.of(tablet));
        return index;
    }

    @Test
    public void resolveVisibleIndexTargets_baseOnly_resolvesSingleTarget() {
        OlapTable table = mock(OlapTable.class);
        when(table.getBaseIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        MaterializedIndex baseIndex = mockIndexWithOneTablet(BASE_INDEX_META_ID, 1001L);
        PhysicalPartition partition = mock(PhysicalPartition.class);
        when(partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE))
                .thenReturn(List.of(baseIndex));

        Column sortKey = new Column("k", IntegerType.BIGINT);
        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class)) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table, BASE_INDEX_META_ID))
                    .thenReturn(List.of(sortKey));
            List<IndexPreSplitTarget> targets = PreSplitTargets.resolveVisibleIndexTargets(table, partition);
            Assertions.assertNotNull(targets);
            Assertions.assertEquals(1, targets.size());
            Assertions.assertEquals(BASE_INDEX_META_ID, targets.get(0).indexMetaId());
            Assertions.assertEquals(1001L, targets.get(0).oldTabletId());
            Assertions.assertEquals(List.of(sortKey), targets.get(0).sortKey());
        }
    }

    @Test
    public void resolveVisibleIndexTargets_baseAndRollup_baseFirst() {
        OlapTable table = mock(OlapTable.class);
        when(table.getBaseIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        MaterializedIndex rollupIndex = mockIndexWithOneTablet(ROLLUP_INDEX_META_ID, 2001L);
        MaterializedIndex baseIndex = mockIndexWithOneTablet(BASE_INDEX_META_ID, 1001L);
        PhysicalPartition partition = mock(PhysicalPartition.class);
        // Rollup listed before base in the source list -- base must still resolve first.
        when(partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE))
                .thenReturn(List.of(rollupIndex, baseIndex));

        Column baseSortKey = new Column("k", IntegerType.BIGINT);
        Column rollupSortKey = new Column("k2", IntegerType.BIGINT);
        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class)) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table, BASE_INDEX_META_ID))
                    .thenReturn(List.of(baseSortKey));
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table, ROLLUP_INDEX_META_ID))
                    .thenReturn(List.of(rollupSortKey));
            List<IndexPreSplitTarget> targets = PreSplitTargets.resolveVisibleIndexTargets(table, partition);
            Assertions.assertNotNull(targets);
            Assertions.assertEquals(2, targets.size());
            Assertions.assertEquals(BASE_INDEX_META_ID, targets.get(0).indexMetaId(), "base index must be first");
            Assertions.assertEquals(ROLLUP_INDEX_META_ID, targets.get(1).indexMetaId());
        }
    }

    @Test
    public void resolveVisibleIndexTargets_rollupMultiTablet_null() {
        OlapTable table = mock(OlapTable.class);
        when(table.getBaseIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        MaterializedIndex baseIndex = mockIndexWithOneTablet(BASE_INDEX_META_ID, 1001L);
        MaterializedIndex rollupIndex = mock(MaterializedIndex.class);
        when(rollupIndex.getMetaId()).thenReturn(ROLLUP_INDEX_META_ID);
        when(rollupIndex.getTablets()).thenReturn(List.of(mock(Tablet.class), mock(Tablet.class)));
        PhysicalPartition partition = mock(PhysicalPartition.class);
        when(partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE))
                .thenReturn(List.of(baseIndex, rollupIndex));

        Column baseSortKey = new Column("k", IntegerType.BIGINT);
        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class)) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table, BASE_INDEX_META_ID))
                    .thenReturn(List.of(baseSortKey));
            Assertions.assertNull(PreSplitTargets.resolveVisibleIndexTargets(table, partition),
                    "a multi-tablet rollup must fail the whole resolution");
        }
    }

    @Test
    public void resolveVisibleIndexTargets_rollupNonScalarSortKey_null() {
        OlapTable table = mock(OlapTable.class);
        when(table.getBaseIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        MaterializedIndex baseIndex = mockIndexWithOneTablet(BASE_INDEX_META_ID, 1001L);
        MaterializedIndex rollupIndex = mockIndexWithOneTablet(ROLLUP_INDEX_META_ID, 2001L);
        PhysicalPartition partition = mock(PhysicalPartition.class);
        when(partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE))
                .thenReturn(List.of(baseIndex, rollupIndex));

        Column baseSortKey = new Column("k", IntegerType.BIGINT);
        Column rollupArrayColumn = new Column("arr", new ArrayType(IntegerType.INT));
        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class)) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table, BASE_INDEX_META_ID))
                    .thenReturn(List.of(baseSortKey));
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table, ROLLUP_INDEX_META_ID))
                    .thenReturn(List.of(rollupArrayColumn));
            Assertions.assertNull(PreSplitTargets.resolveVisibleIndexTargets(table, partition),
                    "a non-scalar rollup sort-key column must fail the whole resolution");
        }
    }

    @Test
    public void resolveVisibleIndexTargets_baseNonEmptyRowCount_stillResolves() {
        // Row-count emptiness is a separate PARTITION_NOT_EMPTY gate elsewhere; the resolver
        // itself must not reject a non-empty base index.
        OlapTable table = mock(OlapTable.class);
        when(table.getBaseIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        MaterializedIndex baseIndex = mockIndexWithOneTablet(BASE_INDEX_META_ID, 1001L);
        when(baseIndex.getRowCount()).thenReturn(999L);
        PhysicalPartition partition = mock(PhysicalPartition.class);
        when(partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE))
                .thenReturn(List.of(baseIndex));

        Column sortKey = new Column("k", IntegerType.BIGINT);
        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class)) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table, BASE_INDEX_META_ID))
                    .thenReturn(List.of(sortKey));
            Assertions.assertNotNull(PreSplitTargets.resolveVisibleIndexTargets(table, partition),
                    "row count must not be checked by the resolver");
        }
    }
}
