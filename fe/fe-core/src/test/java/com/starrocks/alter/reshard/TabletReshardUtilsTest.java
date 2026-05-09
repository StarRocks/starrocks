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

import com.starrocks.common.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TabletReshardUtilsTest {

    private long savedTargetSize;
    private int savedMaxSplitCount;

    @BeforeEach
    public void setup() {
        savedTargetSize = Config.tablet_reshard_target_size;
        savedMaxSplitCount = Config.tablet_reshard_max_split_count;
        Config.tablet_reshard_target_size = 10L * 1024 * 1024 * 1024; // 10G
        Config.tablet_reshard_max_split_count = 1024;
    }

    @AfterEach
    public void teardown() {
        Config.tablet_reshard_target_size = savedTargetSize;
        Config.tablet_reshard_max_split_count = savedMaxSplitCount;
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
}
