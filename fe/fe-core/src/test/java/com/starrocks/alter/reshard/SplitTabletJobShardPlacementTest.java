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

package com.starrocks.alter.reshard;

import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateRangeUtils;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.sql.common.MetaUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntSupplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SplitTabletJob#addShardPlacementsForTablet}, which builds the SPREAD/PACK
 * shard-group assignment for each new split shard. The pre-split root fix (PR #76608) omits the PACK
 * colocate group for pre-split (empty-source) shards so StarOS spreads the fresh batch across CNs at
 * creation instead of herding it onto one node; the per-bucket PACK groups are established afterwards
 * by the post-publish reconcile. These tests pin the three branches directly (no cluster needed).
 */
public class SplitTabletJobShardPlacementTest {

    private static final long OLD_TABLET_ID = 100L;
    private static final long NEW_TABLET_A = 201L;
    private static final long NEW_TABLET_B = 202L;
    private static final long SPREAD_GROUP = 5000L;
    private static final long PACK_GROUP = 9000L;
    private static final int COLOCATE_COL_COUNT = 2;

    private ReshardingTablet twoChildSplit() {
        SplittingTablet splittingTablet = mock(SplittingTablet.class);
        // Empty per-child ranges -> the PACK lookup (online-split branch only) falls back to the old
        // tablet's range, which the static lookup below ignores.
        when(splittingTablet.getNewTabletRanges()).thenReturn(List.of());
        ReshardingTablet rt = mock(ReshardingTablet.class);
        when(rt.getFirstOldTabletId()).thenReturn(OLD_TABLET_ID);
        when(rt.getNewTabletIds()).thenReturn(List.of(NEW_TABLET_A, NEW_TABLET_B));
        when(rt.getSplittingTablet()).thenReturn(splittingTablet);
        return rt;
    }

    private MaterializedIndex newIndexWithSpreadGroup() {
        MaterializedIndex newIndex = mock(MaterializedIndex.class);
        when(newIndex.getShardGroupId()).thenReturn(SPREAD_GROUP);
        return newIndex;
    }

    private MaterializedIndex oldIndexWithRange() {
        Tablet oldTablet = mock(Tablet.class);
        TabletRange tabletRange = mock(TabletRange.class);
        // Consulted only in the PACK branch, where the static lookup is stubbed to ignore the range.
        when(tabletRange.getRange()).thenReturn(null);
        when(oldTablet.getRange()).thenReturn(tabletRange);
        MaterializedIndex oldIndex = mock(MaterializedIndex.class);
        when(oldIndex.getTablet(OLD_TABLET_ID)).thenReturn(oldTablet);
        return oldIndex;
    }

    @Test
    public void testPreSplitColocateOmitsPackGroup() {
        Map<Long, Long> newToOld = new LinkedHashMap<>();
        Map<Long, List<Long>> groups = new LinkedHashMap<>();
        List<ColocateRange> colocateRanges = List.of(mock(ColocateRange.class));

        SplitTabletJob.addShardPlacementsForTablet(twoChildSplit(), oldIndexWithRange(),
                newIndexWithSpreadGroup(), colocateRanges, COLOCATE_COL_COUNT,
                /* spreadNewShards */ true, newToOld, groups);

        // Pre-split: only the SPREAD group, NO PACK group -> the fresh batch spreads at creation.
        Assertions.assertEquals(List.of(SPREAD_GROUP), groups.get(NEW_TABLET_A));
        Assertions.assertEquals(List.of(SPREAD_GROUP), groups.get(NEW_TABLET_B));
        Assertions.assertEquals(OLD_TABLET_ID, newToOld.get(NEW_TABLET_A).longValue());
    }

    @Test
    public void testOnlineSplitColocateKeepsPackGroup() {
        Map<Long, Long> newToOld = new LinkedHashMap<>();
        Map<Long, List<Long>> groups = new LinkedHashMap<>();
        List<ColocateRange> colocateRanges = List.of(mock(ColocateRange.class));

        try (MockedStatic<ColocateRangeUtils> mocked = mockStatic(ColocateRangeUtils.class)) {
            mocked.when(() -> ColocateRangeUtils.lookupPackShardGroupId(any(), any(), anyInt()))
                    .thenReturn(PACK_GROUP);

            SplitTabletJob.addShardPlacementsForTablet(twoChildSplit(), oldIndexWithRange(),
                    newIndexWithSpreadGroup(), colocateRanges, COLOCATE_COL_COUNT,
                    /* spreadNewShards */ false, newToOld, groups);
        }

        // Online split (non-empty source): SPREAD group + the per-range PACK group (unchanged behavior).
        Assertions.assertEquals(List.of(SPREAD_GROUP, PACK_GROUP), groups.get(NEW_TABLET_A));
        Assertions.assertEquals(List.of(SPREAD_GROUP, PACK_GROUP), groups.get(NEW_TABLET_B));
    }

    @Test
    public void testNonColocateGetsOnlySpreadGroupRegardlessOfSpreadFlag() {
        // Non-colocate table (colocateRanges == null): only the SPREAD group, whether pre-split or not.
        for (boolean spread : new boolean[] {true, false}) {
            Map<Long, Long> newToOld = new LinkedHashMap<>();
            Map<Long, List<Long>> groups = new LinkedHashMap<>();

            SplitTabletJob.addShardPlacementsForTablet(twoChildSplit(), mock(MaterializedIndex.class),
                    newIndexWithSpreadGroup(), /* colocateRanges */ null, 0, spread, newToOld, groups);

            Assertions.assertEquals(List.of(SPREAD_GROUP), groups.get(NEW_TABLET_A),
                    "non-colocate must never get a PACK group (spread=" + spread + ")");
            Assertions.assertEquals(List.of(SPREAD_GROUP), groups.get(NEW_TABLET_B),
                    "non-colocate must never get a PACK group (spread=" + spread + ")");
        }
    }

    private static final long INDEX_META_ID = 77L;
    private static final IntSupplier COMPUTE_NODES = () -> 3;

    private MaterializedIndex sourceIndex(long rowCount, int tabletCount) {
        MaterializedIndex index = mock(MaterializedIndex.class);
        when(index.getRowCount()).thenReturn(rowCount);
        when(index.getTablets()).thenReturn(Collections.nCopies(tabletCount, mock(Tablet.class)));
        return index;
    }

    private MaterializedIndex targetIndex() {
        MaterializedIndex index = mock(MaterializedIndex.class);
        when(index.getMetaId()).thenReturn(INDEX_META_ID);
        return index;
    }

    private OlapTable bundlingTable() {
        OlapTable table = mock(OlapTable.class);
        when(table.isFileBundling()).thenReturn(true);
        return table;
    }

    @Test
    public void testPreSplitAlwaysUnpins() {
        // Empty source: no warm cache to preserve, unpin regardless of sort key or tablet count.
        OlapTable table = mock(OlapTable.class);
        Assertions.assertTrue(SplitTabletJob.shouldUnpinPlacement(
                table, sourceIndex(0L, 64), targetIndex(), COMPUTE_NODES));
    }

    @Test
    public void testOrdinaryOnlineSplitKeepsPin() {
        // Non-empty source without a separate sort key: no UNSHARE rewrite follows, so the warm-cache
        // pin is still the right trade.
        OlapTable table = bundlingTable();
        try (MockedStatic<MetaUtils> mocked = mockStatic(MetaUtils.class)) {
            mocked.when(() -> MetaUtils.hasSeparateSortKey(any(), anyLong())).thenReturn(false);
            Assertions.assertFalse(SplitTabletJob.shouldUnpinPlacement(
                    table, sourceIndex(1_000L, 1), targetIndex(), COMPUTE_NODES));
        }
    }

    @Test
    public void testSeparateSortKeySplitUnpinsWhileIndexIsSmall() {
        // ORDER BY != PK, index still below 2 shards per node (1 < 6): unpin so the UNSHARE rewrite
        // does not land entirely on the source worker.
        OlapTable table = bundlingTable();
        try (MockedStatic<MetaUtils> mocked = mockStatic(MetaUtils.class)) {
            mocked.when(() -> MetaUtils.hasSeparateSortKey(any(), anyLong())).thenReturn(true);
            Assertions.assertTrue(SplitTabletJob.shouldUnpinPlacement(
                    table, sourceIndex(1_000L, 1), targetIndex(), COMPUTE_NODES));
            Assertions.assertTrue(SplitTabletJob.shouldUnpinPlacement(
                    table, sourceIndex(1_000L, 5), targetIndex(), COMPUTE_NODES));
        }
    }

    @Test
    public void testSeparateSortKeySplitKeepsPinOnceIndexIsWideEnough() {
        // At 2 shards per node the children already inherit a spread placement, so the pin goes back
        // to earning its warm cache on the (much larger) rewrite.
        OlapTable table = bundlingTable();
        try (MockedStatic<MetaUtils> mocked = mockStatic(MetaUtils.class)) {
            mocked.when(() -> MetaUtils.hasSeparateSortKey(any(), anyLong())).thenReturn(true);
            Assertions.assertFalse(SplitTabletJob.shouldUnpinPlacement(
                    table, sourceIndex(1_000L, 6), targetIndex(), COMPUTE_NODES));
            Assertions.assertFalse(SplitTabletJob.shouldUnpinPlacement(
                    table, sourceIndex(1_000L, 22), targetIndex(), COMPUTE_NODES));
        }
    }

    @Test
    public void testSeparateSortKeyWithoutFileBundlingKeepsPin() {
        // No file bundling -> CompactionScheduler never schedules the UNSHARE rewrite, so there is no
        // rewrite to spread.
        OlapTable table = mock(OlapTable.class);
        when(table.isFileBundling()).thenReturn(false);
        try (MockedStatic<MetaUtils> mocked = mockStatic(MetaUtils.class)) {
            mocked.when(() -> MetaUtils.hasSeparateSortKey(any(), anyLong())).thenReturn(true);
            Assertions.assertFalse(SplitTabletJob.shouldUnpinPlacement(
                    table, sourceIndex(1_000L, 1), targetIndex(), COMPUTE_NODES));
        }
    }
}
