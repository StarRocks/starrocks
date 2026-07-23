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
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
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
}
