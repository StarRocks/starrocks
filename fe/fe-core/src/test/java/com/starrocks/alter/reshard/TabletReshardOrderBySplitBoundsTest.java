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

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Bounds that apply only when a split drags a full UNSHARE rewrite behind it -- a range-distributed
 * primary-key table whose ORDER BY key differs from the primary key. An ordinary split leaves its
 * children sharing the parent's segments and must stay unbounded.
 */
public class TabletReshardOrderBySplitBoundsTest {

    private final int origFanOut = Config.tablet_reshard_orderby_max_split_count;
    private final int origPerJob = Config.tablet_reshard_orderby_max_split_tablets_per_job;
    private final int origMaxSplit = Config.tablet_reshard_max_split_count;

    @AfterEach
    public void restore() {
        Config.tablet_reshard_orderby_max_split_count = origFanOut;
        Config.tablet_reshard_orderby_max_split_tablets_per_job = origPerJob;
        Config.tablet_reshard_max_split_count = origMaxSplit;
    }

    private OlapTable bundlingTable() {
        OlapTable t = mock(OlapTable.class);
        when(t.isFileBundling()).thenReturn(true);
        when(t.getBaseIndexMetaId()).thenReturn(7L);
        return t;
    }

    private MockedStatic<MetaUtils> separateSortKey(boolean value) {
        MockedStatic<MetaUtils> mocked = mockStatic(MetaUtils.class);
        mocked.when(() -> MetaUtils.hasSeparateSortKey(any(), anyLong())).thenReturn(value);
        return mocked;
    }

    @Test
    public void testFanOutClampedOnlyForUnshareSplits() {
        Config.tablet_reshard_max_split_count = 1024;
        Config.tablet_reshard_orderby_max_split_count = 2;
        OlapTable table = bundlingTable();

        try (MockedStatic<MetaUtils> ignored = separateSortKey(true)) {
            Assertions.assertEquals(2, TabletReshardUtils.effectiveMaxSplitCount(table));
        }
        try (MockedStatic<MetaUtils> ignored = separateSortKey(false)) {
            Assertions.assertEquals(1024, TabletReshardUtils.effectiveMaxSplitCount(table));
        }
    }

    @Test
    public void testFanOutClampCapsCalcSplitCount() {
        // 100 GiB against a 4 GiB target would otherwise fan out to 25 children.
        long dataSize = 100L * 1024 * 1024 * 1024;
        long target = 4L * 1024 * 1024 * 1024;
        Assertions.assertEquals(25, TabletReshardUtils.calcSplitCount(dataSize, target, 1024));
        Assertions.assertEquals(2, TabletReshardUtils.calcSplitCount(dataSize, target, 2));
    }

    @Test
    public void testFanOutClampDisabledWhenNotAboveOne() {
        Config.tablet_reshard_max_split_count = 1024;
        Config.tablet_reshard_orderby_max_split_count = 1;
        OlapTable table = bundlingTable();
        try (MockedStatic<MetaUtils> ignored = separateSortKey(true)) {
            Assertions.assertEquals(1024, TabletReshardUtils.effectiveMaxSplitCount(table),
                    "a value <= 1 must disable the clamp, not wedge splits at one child");
        }
    }

    @Test
    public void testPerJobTabletBudget() {
        OlapTable table = bundlingTable();
        ComputeResource cr = mock(ComputeResource.class);
        // The resource reaches StarMgr through the warehouse availability probe, so the budget must
        // ask for it only when the answer actually depends on it.
        AtomicLong resourceReads = new AtomicLong();
        Supplier<ComputeResource> resource = () -> {
            resourceReads.incrementAndGet();
            return cr;
        };

        try (MockedStatic<MetaUtils> ignored = separateSortKey(false)) {
            Assertions.assertEquals(Integer.MAX_VALUE, TabletReshardUtils.maxSplitTabletsPerJob(table, resource),
                    "an ordinary split must stay unbounded");
        }
        try (MockedStatic<MetaUtils> ignored = separateSortKey(true)) {
            Config.tablet_reshard_orderby_max_split_tablets_per_job = 5;
            Assertions.assertEquals(5, TabletReshardUtils.maxSplitTabletsPerJob(table, resource));
        }
        Assertions.assertEquals(0, resourceReads.get(),
                "neither an ordinary split nor a configured bound may pay for the warehouse probe");
    }

    private static SplitTabletJobFactory.SplitCandidate candidate(Tablet tablet) {
        return new SplitTabletJobFactory.SplitCandidate(1000L, null, tablet, 2);
    }

    private static List<Long> electedIds(List<SplitTabletJobFactory.SplitCandidate> candidates, int budget) {
        return SplitTabletJobFactory.electLargestWithinBudget(candidates, budget).stream()
                .map(c -> c.oldTabletId).collect(Collectors.toList());
    }

    @Test
    public void testLargestTabletsSelectedFirst() {
        Tablet small = mock(Tablet.class);
        when(small.getDataSize(true)).thenReturn(1L);
        when(small.getId()).thenReturn(1L);
        Tablet big = mock(Tablet.class);
        when(big.getDataSize(true)).thenReturn(100L);
        when(big.getId()).thenReturn(2L);
        Tablet mid = mock(Tablet.class);
        when(mid.getDataSize(true)).thenReturn(50L);
        when(mid.getId()).thenReturn(3L);

        List<SplitTabletJobFactory.SplitCandidate> candidates =
                List.of(candidate(small), candidate(big), candidate(mid));
        Assertions.assertEquals(List.of(2L, 3L), electedIds(candidates, 2),
                "a capped job must spend its budget on the tablets that most need splitting");
        Assertions.assertEquals(List.of(1L, 2L, 3L), electedIds(candidates, 3),
                "a budget that covers every candidate must take them all");
    }

    /**
     * The budget is job-wide but the candidates used to be ordered only within one index, so a small
     * tablet in the partition the traversal reached first could crowd out a much larger one in a later
     * partition -- the opposite of the largest-first rule the budget exists to serve.
     */
    @Test
    public void testBudgetIsSpentAcrossPartitionsNotWithinTheFirstOne() {
        Tablet smallInFirstPartition = mock(Tablet.class);
        when(smallInFirstPartition.getDataSize(true)).thenReturn(1L);
        when(smallInFirstPartition.getId()).thenReturn(11L);
        Tablet bigInSecondPartition = mock(Tablet.class);
        when(bigInSecondPartition.getDataSize(true)).thenReturn(100L);
        when(bigInSecondPartition.getId()).thenReturn(22L);

        List<SplitTabletJobFactory.SplitCandidate> candidates = List.of(
                new SplitTabletJobFactory.SplitCandidate(1000L, null, smallInFirstPartition, 2),
                new SplitTabletJobFactory.SplitCandidate(2000L, null, bigInSecondPartition, 2));

        Assertions.assertEquals(List.of(22L), electedIds(candidates, 1),
                "the largest tablet wins the budget wherever it lives");
    }

    /**
     * TabletStatMgr rewrites LakeTablet.dataSize from its own thread without the table lock. A
     * comparator that re-read it mid-sort would stop being transitive, and TimSort answers a
     * non-transitive comparator by throwing rather than by returning a slightly-off order. So a
     * candidate reads its size exactly once, when it is planned.
     */
    @Test
    public void testTabletSizesAreReadOnceSoASizeUpdateCannotBreakTheSort() {
        // Above TimSort's 32-element threshold, so the merge path -- the one that detects and throws
        // on a non-transitive comparator -- actually runs.
        final int tabletCount = 40;
        List<Tablet> tablets = new ArrayList<>();
        for (int i = 0; i < tabletCount; i++) {
            long id = i + 1;
            long sizeAtEntry = tabletCount - i;
            long sizeAfterTheUpdate = i;
            // The first read sees the size planning is entitled to act on; every later read sees a stat
            // update that has since inverted the order, the way a concurrent update would.
            AtomicLong reads = new AtomicLong();
            Tablet tablet = mock(Tablet.class);
            when(tablet.getId()).thenReturn(id);
            when(tablet.getDataSize(true)).thenAnswer(
                    invocation -> reads.getAndIncrement() == 0 ? sizeAtEntry : sizeAfterTheUpdate);
            tablets.add(tablet);
        }
        List<SplitTabletJobFactory.SplitCandidate> candidates =
                tablets.stream().map(TabletReshardOrderBySplitBoundsTest::candidate).collect(Collectors.toList());

        List<Long> elected = electedIds(candidates, tabletCount / 2);

        Assertions.assertEquals(
                tablets.subList(0, tabletCount / 2).stream().map(Tablet::getId).collect(Collectors.toList()),
                elected,
                "the order must follow the sizes as they read at entry, not as they drift mid-sort");
        for (Tablet tablet : tablets) {
            verify(tablet, times(1)).getDataSize(true);
        }
    }
}
