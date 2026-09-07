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

import com.google.common.collect.Lists;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.StarRocksException;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.WarehouseManager;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TabletReshardUtilsTest {

    private long savedTargetSize;
    private int savedMaxSplitCount;
    private long savedMinSplitSize;

    @BeforeEach
    public void setup() {
        savedTargetSize = Config.tablet_reshard_target_size;
        savedMaxSplitCount = Config.tablet_reshard_max_split_count;
        savedMinSplitSize = Config.tablet_reshard_min_split_size;
        Config.tablet_reshard_target_size = 10L * 1024 * 1024 * 1024; // 10G
        Config.tablet_reshard_max_split_count = 1024;
        Config.tablet_reshard_min_split_size = 2L * 1024 * 1024 * 1024; // 2G
    }

    @AfterEach
    public void teardown() {
        Config.tablet_reshard_target_size = savedTargetSize;
        Config.tablet_reshard_max_split_count = savedMaxSplitCount;
        Config.tablet_reshard_min_split_size = savedMinSplitSize;
    }

    @Test
    public void splitThreshold_ceil1Point5Times() {
        assertEquals(15L, TabletReshardUtils.splitThreshold(10L));
        assertEquals(2L, TabletReshardUtils.splitThreshold(1L));        // ceil(1.5*1) = 2
        assertEquals(0L, TabletReshardUtils.splitThreshold(0L));
    }

    @Test
    public void mergePairThreshold_ceil0Point8Times() {
        // Exact multiples of 5 have ceil == floor.
        assertEquals(8L, TabletReshardUtils.mergePairThreshold(10L));
        assertEquals(4L, TabletReshardUtils.mergePairThreshold(5L));
        // Non-multiples round up so strict-< accepts the full half-open interval [0, 0.8T).
        assertEquals(6L, TabletReshardUtils.mergePairThreshold(7L));    // ceil(0.8*7) = ceil(5.6) = 6
        assertEquals(1L, TabletReshardUtils.mergePairThreshold(1L));    // ceil(0.8*1) = 1
        assertEquals(2L, TabletReshardUtils.mergePairThreshold(2L));    // ceil(0.8*2) = ceil(1.6) = 2
        assertEquals(3L, TabletReshardUtils.mergePairThreshold(3L));    // ceil(0.8*3) = ceil(2.4) = 3
        assertEquals(4L, TabletReshardUtils.mergePairThreshold(4L));    // ceil(0.8*4) = ceil(3.2) = 4
        assertEquals(0L, TabletReshardUtils.mergePairThreshold(0L));
    }

    @Test
    public void splitThresholdOverflows_alignsWithActualBoundary() {
        // Anything T such that T + T/2 + (T&1) fits in long is safe.
        assertFalse(TabletReshardUtils.splitThresholdOverflows(0L));
        assertFalse(TabletReshardUtils.splitThresholdOverflows(1L));
        assertFalse(TabletReshardUtils.splitThresholdOverflows(Long.MAX_VALUE / 2));
        // Codex example: target around 5e18 still has splitThreshold 7.5e18 which fits.
        assertFalse(TabletReshardUtils.splitThresholdOverflows(5_000_000_000_000_000_000L));
        // Largest exact safe value: floor(2 * Long.MAX_VALUE / 3).
        long maxSafe = (Long.MAX_VALUE - 1) / 3 * 2 + ((Long.MAX_VALUE - 1) % 3) * 2 / 3;
        assertFalse(TabletReshardUtils.splitThresholdOverflows(maxSafe));
        // One past the safe boundary must report overflow.
        assertTrue(TabletReshardUtils.splitThresholdOverflows(maxSafe + 1));
        assertTrue(TabletReshardUtils.splitThresholdOverflows(Long.MAX_VALUE));
    }

    @Test
    public void calcSplitCount_largeButSafeTargetStillSplits() {
        // Codex regression: target=5e18, dataSize=8e18 must still produce a valid split.
        // splitThreshold(5e18) = 7.5e18 fits in long; dataSize 8e18 >= 7.5e18 → split=2.
        long target = 5_000_000_000_000_000_000L;
        long dataSize = 8_000_000_000_000_000_000L;
        assertEquals(2, TabletReshardUtils.calcSplitCount(dataSize, target));
    }

    @Test
    public void calcSplitCount_overflowingTargetReturnsOne() {
        // Targets above the splitThreshold overflow boundary must not produce a positive
        // split: splitThreshold would wrap around and the lower-bounded Math.max(2, ...)
        // would otherwise emit a bogus count for any input.
        long unsafeTarget = Long.MAX_VALUE; // far past the 6.15-EB overflow boundary
        assertEquals(1, TabletReshardUtils.calcSplitCount(0L, unsafeTarget));
        assertEquals(1, TabletReshardUtils.calcSplitCount(Long.MAX_VALUE, unsafeTarget));
    }

    @Test
    public void mergeGroupCap_equalsTarget() {
        assertEquals(10L, TabletReshardUtils.mergeGroupCap(10L));
    }

    @Test
    public void needSplit_threshold() {
        long t = Config.tablet_reshard_target_size;
        long splitTrigger = TabletReshardUtils.splitThreshold(t);
        assertFalse(TabletReshardUtils.needSplit(t));
        assertFalse(TabletReshardUtils.needSplit(splitTrigger - 1));
        assertTrue(TabletReshardUtils.needSplit(splitTrigger));
        assertTrue(TabletReshardUtils.needSplit(t * 100));
    }

    @Test
    public void needMerge_strictlyLess() {
        long t = Config.tablet_reshard_target_size;
        long pairThresh = TabletReshardUtils.mergePairThreshold(t);
        assertTrue(TabletReshardUtils.needMerge(pairThresh - 1));
        assertFalse(TabletReshardUtils.needMerge(pairThresh));    // strict <
        assertFalse(TabletReshardUtils.needMerge(t));
    }

    @Test
    public void needMerge_targetZeroDisabled() {
        Config.tablet_reshard_target_size = 0L;
        assertFalse(TabletReshardUtils.needMerge(0));
        assertFalse(TabletReshardUtils.needMerge(100));
    }

    @Test
    public void calcSplitCount_belowTriggerReturnsOne() {
        long t = Config.tablet_reshard_target_size;
        assertEquals(1, TabletReshardUtils.calcSplitCount(0L, t));
        assertEquals(1, TabletReshardUtils.calcSplitCount(t, t));
        assertEquals(1, TabletReshardUtils.calcSplitCount(TabletReshardUtils.splitThreshold(t) - 1, t));
    }

    @Test
    public void calcSplitCount_buckets() {
        long t = Config.tablet_reshard_target_size;
        // boundary 1.5T → 2 pieces
        assertEquals(2, TabletReshardUtils.calcSplitCount(TabletReshardUtils.splitThreshold(t), t));
        // 2T → 2
        assertEquals(2, TabletReshardUtils.calcSplitCount(t * 2, t));
        // 2.49T → 2 (round-to-nearest, just under 2.5)
        assertEquals(2, TabletReshardUtils.calcSplitCount(t * 5 / 2 - 1, t));
        // 2.5T → 3
        assertEquals(3, TabletReshardUtils.calcSplitCount(t * 5 / 2, t));
        // 3T → 3
        assertEquals(3, TabletReshardUtils.calcSplitCount(t * 3, t));
        // 3.5T → 4
        assertEquals(4, TabletReshardUtils.calcSplitCount(t * 7 / 2, t));
    }

    @Test
    public void calcSplitCount_capsAtMaxSplitCount() {
        long t = 100L;
        long huge = t * (Config.tablet_reshard_max_split_count + 100L);
        assertEquals(Config.tablet_reshard_max_split_count, TabletReshardUtils.calcSplitCount(huge, t));
    }

    @Test
    public void calcSplitCount_negativeTargetForcedSplitCount() {
        // negative-target test mode: -k means "force split into k pieces"
        assertEquals(7, TabletReshardUtils.calcSplitCount(0, -7));
        assertEquals(0, TabletReshardUtils.calcSplitCount(0, -(Config.tablet_reshard_max_split_count + 1L)));
    }

    @Test
    public void calcSplitCount_overflowSafe_largeTarget() {
        // very large target (1 PiB), well below Long.MAX/2
        long t = 1L << 50;
        long data = 5L * t;
        assertEquals(5, TabletReshardUtils.calcSplitCount(data, t));
        // Helpers themselves don't overflow at this magnitude
        assertTrue(TabletReshardUtils.splitThreshold(t) > 0);
        assertTrue(TabletReshardUtils.mergePairThreshold(t) > 0);
        assertTrue(TabletReshardUtils.mergeGroupCap(t) > 0);
    }

    @Test
    public void calcSplitCount_overflowSafe_largeData() {
        // dataSize close to Long.MAX, small target — exercises division-then-remainder
        long t = 100L;
        // pick a large multiple of t that fits in long
        long data = (Long.MAX_VALUE / t) * t;
        // expected to cap at max_split_count
        assertEquals(Config.tablet_reshard_max_split_count, TabletReshardUtils.calcSplitCount(data, t));
    }

    @Test
    public void safeComputeNodeCountForTable_returnsZeroWhenResolutionFails() {
        // The resolution goes through the warehouse manager and can throw (e.g. the warehouse no
        // longer exists, or has no usable worker). It must swallow that and fall back to 0 so a single
        // table's warehouse error cannot abort the scan; 0 in turn means "no floor" for auto-merge and
        // no adaptive signal from that scan. The planner does NOT come through here -- it resolves via
        // adaptiveSplitBoundForTable, which propagates instead.
        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeResource getBackgroundComputeResource(long tableId) {
                throw new RuntimeException("warehouse unavailable");
            }
        };
        assertEquals(0, TabletReshardUtils.safeComputeNodeCountForTable(123L));
    }



    @Test
    public void adaptiveTargetSize_aimsForOneTabletPerNodeWhileTheIndexIsNarrow() {
        long saved = Config.tablet_reshard_min_split_size;
        Config.tablet_reshard_min_split_size = 2L << 30;
        try {
            // 24 GiB over a bound of 8 wants 3 GiB tablets, which is below the steady target, so the
            // adaptive term wins and a single 24 GiB tablet splits into exactly the bound.
            assertEquals(3L << 30, TabletReshardUtils.adaptiveTargetSize(24L << 30, 10L << 30, 8));
            assertEquals(8, TabletReshardUtils.calcSplitCount(24L << 30, 3L << 30),
                    "one step lands on the bound, so nothing needs to stop it overshooting");

            // Enough data that one tablet per node would be larger than the steady target: the steady
            // target wins and the rule is the size rule, unchanged.
            assertEquals(10L << 30, TabletReshardUtils.adaptiveTargetSize(100L << 30, 10L << 30, 8));

            // Nearly empty index: the floor stops it being carved into slivers.
            assertEquals(2L << 30, TabletReshardUtils.adaptiveTargetSize(100L << 20, 10L << 30, 8));

            // Unresolved warehouse leaves the steady target alone.
            assertEquals(10L << 30, TabletReshardUtils.adaptiveTargetSize(24L << 30, 10L << 30, 0));
        } finally {
            Config.tablet_reshard_min_split_size = saved;
        }
    }

    @Test
    public void adaptiveTargetSize_isDisabledByRaisingTheMinimumToTheTarget() {
        long saved = Config.tablet_reshard_min_split_size;
        try {
            // The floor is clamped to the target, so a minimum at or above it collapses the whole
            // expression to the target -- that clamp is the off switch, and it is also what stops a
            // large minimum raising the target above its configured value.
            Config.tablet_reshard_min_split_size = 10L << 30;
            assertEquals(10L << 30, TabletReshardUtils.adaptiveTargetSize(24L << 30, 10L << 30, 8));
            Config.tablet_reshard_min_split_size = 40L << 30;
            assertEquals(10L << 30, TabletReshardUtils.adaptiveTargetSize(24L << 30, 10L << 30, 8),
                    "a minimum above the target must not raise the target");
        } finally {
            Config.tablet_reshard_min_split_size = saved;
        }
    }

    @Test
    public void theBoundIsDerivedFromTheCapItIsGivenNotTheLiveConfig() {
        int saved = Config.tablet_reshard_max_split_count;
        try {
            // A caller that derives the merge floor and this bound from one decision must sample the
            // cap once and hand it to both. If this re-read the live config instead, a change landing
            // between the two reads would put the floor above the bound -- an index could then be
            // mergeable and under-provisioned at the same time, which is the overlap the bound exists
            // to prevent. The two values below are the ones that make that visible.
            Config.tablet_reshard_max_split_count = 2;
            assertEquals(50, TabletReshardUtils.adaptiveSplitBound(50, 100),
                    "the bound must come from the cap it was given");
            assertEquals(2, TabletReshardUtils.parallelismFloor(50, Config.tablet_reshard_max_split_count),
                    "and this is the floor the live config would have produced -- above that bound");
        } finally {
            Config.tablet_reshard_max_split_count = saved;
        }
    }

    @Test
    public void anUnresolvableWarehouseIsFatalToThePlannerAndSurvivableToTheScan() {
        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeResource getBackgroundComputeResource(long tableId) {
                throw ErrorReportException.report(ErrorCode.ERR_WAREHOUSE_UNAVAILABLE, "wh");
            }
        };

        // The planner must not read "warehouse temporarily unavailable" as "this index needs nothing":
        // that produces an empty plan, which its caller is entitled to latch as deterministic, and the
        // fingerprint would never move again on an unchanged layout.
        assertThrows(ErrorReportException.class, () -> TabletReshardUtils.adaptiveSplitBoundForTable(1L),
                "the planner's resolution must propagate");

        // The scan is the caller that should degrade instead -- it has a whole cluster left to walk,
        // and its output is only a signal the planner re-decides.
        assertEquals(0, TabletReshardUtils.safeComputeNodeCountForTable(1L),
                "the scan's resolution must fall back");
    }


    @Test
    public void parallelismFloor_clampsAndBounds() {
        // typical: floor follows compute node count
        assertEquals(4, TabletReshardUtils.parallelismFloor(4, 1024));
        // upper clamp at max split count
        assertEquals(1024, TabletReshardUtils.parallelismFloor(2000, 1024));
        // lower bound at 2 (matches pre-split's clamp(...,2,...))
        assertEquals(2, TabletReshardUtils.parallelismFloor(1, 1024));
        assertEquals(2, TabletReshardUtils.parallelismFloor(2, 1024));
        // zero-node edge: computeNodeCount guarantees >= 1 in practice, but floor still holds
        assertEquals(2, TabletReshardUtils.parallelismFloor(0, 1024));
        // max split count < 2 => pre-split disabled => no floor (degrades to 1)
        assertEquals(1, TabletReshardUtils.parallelismFloor(5, 1));
        assertEquals(1, TabletReshardUtils.parallelismFloor(5, 0));
    }

    /**
     * Static check of the convergence invariants. These two inequalities are what prevent
     * split↔merge oscillation. If they ever fail, the algorithm can ping-pong indefinitely.
     */
    @Test
    public void invariants_holdAtCurrentRatios() {
        long t = Config.tablet_reshard_target_size;
        long splitTrigger = TabletReshardUtils.splitThreshold(t);
        long pairThresh = TabletReshardUtils.mergePairThreshold(t);
        long mergeCap = TabletReshardUtils.mergeGroupCap(t);

        // 1. Two adjacent post-split pieces (each >= splitTrigger / 2 under uniform row width)
        //    must NOT satisfy needMerge: their pair sum >= splitTrigger > pairThresh required.
        long minPiece = splitTrigger / 2;
        assertFalse(TabletReshardUtils.needMerge(minPiece + minPiece),
                "split output pair must not be a merge candidate");

        // 2. Merged group cap must be strictly below split trigger (otherwise merge output
        //    immediately re-triggers split).
        assertTrue(mergeCap < splitTrigger, "mergeGroupCap must be < splitThreshold");

        // 3. Strict inequality on (1): pairThresh < splitTrigger
        assertTrue(pairThresh < splitTrigger);
    }

    private MaterializedIndex indexWithTablets(long indexId, Map<Long, Long> tabletIdToVibv) {
        MaterializedIndex index = new MaterializedIndex(indexId);
        for (Map.Entry<Long, Long> e : tabletIdToVibv.entrySet()) {
            LakeTablet tablet = new LakeTablet(e.getKey());
            tablet.setVectorIndexBuiltVersion(e.getValue());
            index.addTablet(tablet, null, false);
        }
        return index;
    }

    // minVectorIndexBuiltVersion: split (single parent) inherits the parent; merge (multiple
    // sources) takes the min; an empty source list yields 0 (non-vector tables are all 0).
    @Test
    public void testMinVectorIndexBuiltVersion() {
        Map<Long, Long> ids = new HashMap<>();
        ids.put(101L, 100L);
        ids.put(102L, 50L);
        MaterializedIndex index = indexWithTablets(1L, ids);

        // merge: min across sources
        assertEquals(50L, TabletReshardUtils.minVectorIndexBuiltVersion(index, Lists.newArrayList(101L, 102L)));
        // split / identical: single parent
        assertEquals(100L, TabletReshardUtils.minVectorIndexBuiltVersion(index, Lists.newArrayList(101L)));
        // empty -> 0
        assertEquals(0L, TabletReshardUtils.minVectorIndexBuiltVersion(index, Collections.emptyList()));
    }

    // A reshard whose source tablet lives in a materialized index that an earlier reshard already
    // superseded must be rejected at admission. Such an index passes every other check -- it is
    // still returned by getIndex, its state is still NORMAL, and it still owns the tablet -- so
    // without this gate the job is admitted, its publish resolves no live tablet, and it spins in
    // RUNNING forever with an empty error message. Easy to hit because SHOW TABLET keeps listing
    // the superseded tablets (after a split the old parent is still the first row).
    @Test
    public void checkIndexNotSuperseded_rejectsSupersededIndex() {
        MaterializedIndex oldIndex = new MaterializedIndex(100L, 100L, IndexState.NORMAL, 0L);
        oldIndex.addTablet(new LakeTablet(1000L), null, false);
        PhysicalPartition partition = new PhysicalPartition(1L, 0L, oldIndex);

        // Nothing has superseded it yet: the check passes.
        assertDoesNotThrowStarRocks(() ->
                TabletReshardUtils.checkIndexNotSuperseded(partition, oldIndex, 1000L, "db", "t"));

        // A later reshard installs a new version of the same index meta.
        MaterializedIndex newIndex = new MaterializedIndex(200L, 100L, IndexState.NORMAL, 0L);
        newIndex.addTablet(new LakeTablet(2000L), null, false);
        partition.addMaterializedIndex(newIndex, true);

        StarRocksException e = assertThrows(StarRocksException.class, () ->
                TabletReshardUtils.checkIndexNotSuperseded(partition, oldIndex, 1000L, "db", "t"));
        assertTrue(e.getMessage().contains("superseded by index 200"), e.getMessage());
        assertTrue(e.getMessage().contains("1000"), e.getMessage());
        // The replacement tablet ids are spelled out: the partition-level SHOW PROC path this message
        // used to point at needs system-level OPERATE, which a user holding only ALTER on the table --
        // enough to trigger this rejection -- does not have.
        assertTrue(e.getMessage().contains("[2000]"), e.getMessage());

        // The live index is still accepted.
        assertDoesNotThrowStarRocks(() ->
                TabletReshardUtils.checkIndexNotSuperseded(partition, newIndex, 2000L, "db", "t"));
    }

    // A partition can hold far more tablets than belong in an error string, so the list is truncated
    // with the full count kept.
    @Test
    public void checkIndexNotSuperseded_truncatesLongTabletList() {
        MaterializedIndex oldIndex = new MaterializedIndex(100L, 100L, IndexState.NORMAL, 0L);
        oldIndex.addTablet(new LakeTablet(1000L), null, false);
        PhysicalPartition partition = new PhysicalPartition(1L, 0L, oldIndex);

        MaterializedIndex newIndex = new MaterializedIndex(200L, 100L, IndexState.NORMAL, 0L);
        for (int i = 0; i < 25; ++i) {
            newIndex.addTablet(new LakeTablet(2000L + i), null, false);
        }
        partition.addMaterializedIndex(newIndex, true);

        StarRocksException e = assertThrows(StarRocksException.class, () ->
                TabletReshardUtils.checkIndexNotSuperseded(partition, oldIndex, 1000L, "db", "t"));
        assertTrue(e.getMessage().contains("2019]"), e.getMessage());
        assertTrue(e.getMessage().contains("(first 20 of 25)"), e.getMessage());
        assertFalse(e.getMessage().contains("2020"), e.getMessage());
    }

    // Admission-time counterpart of the check above. The factory builds a job from a snapshot taken
    // under a lock it then releases, so a source index that was live at creation can be superseded --
    // or removed outright -- before the job reserves the table. Both leave the job with nothing live
    // to reshard, so both are rejected.
    @Test
    public void checkIndexStillLatest_rejectsSupersededOrRemovedIndex() {
        MaterializedIndex oldIndex = new MaterializedIndex(100L, 100L, IndexState.NORMAL, 0L);
        oldIndex.addTablet(new LakeTablet(1000L), null, false);
        PhysicalPartition partition = new PhysicalPartition(1L, 0L, oldIndex);

        // Still the latest version of its meta: admission proceeds.
        assertDoesNotThrowStarRocks(() ->
                TabletReshardUtils.checkIndexStillLatest(partition, 100L, 100L, "db", "t"));

        // A reshard job admitted first installs a new version of the same index meta.
        MaterializedIndex newIndex = new MaterializedIndex(200L, 100L, IndexState.NORMAL, 0L);
        newIndex.addTablet(new LakeTablet(2000L), null, false);
        partition.addMaterializedIndex(newIndex, true);

        StarRocksException e = assertThrows(StarRocksException.class, () ->
                TabletReshardUtils.checkIndexStillLatest(partition, 100L, 100L, "db", "t"));
        assertTrue(e.getMessage().contains("superseded by index 200"), e.getMessage());
        assertTrue(e.getMessage().contains("[2000]"), e.getMessage());

        // The winner of that race is of course still admissible.
        assertDoesNotThrowStarRocks(() ->
                TabletReshardUtils.checkIndexStillLatest(partition, 200L, 100L, "db", "t"));

        // Index meta gone from the partition entirely: there is nothing left to reshard, and
        // installing a successor would fail addMaterializedIndex's precondition well past the job's
        // no-abort boundary.
        partition.deleteMaterializedIndexByMetaId(100L);
        e = assertThrows(StarRocksException.class, () ->
                TabletReshardUtils.checkIndexStillLatest(partition, 200L, 100L, "db", "t"));
        assertTrue(e.getMessage().contains("was removed after"), e.getMessage());
    }

    private interface ThrowingRunnable {
        void run() throws StarRocksException;
    }

    private static void assertDoesNotThrowStarRocks(ThrowingRunnable r) {
        try {
            r.run();
        } catch (StarRocksException e) {
            throw new AssertionError("unexpected StarRocksException: " + e.getMessage(), e);
        }
    }
}
