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

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.common.Config;

public class TabletReshardUtils {

    // Reshard size invariants — DO NOT change individually.
    //
    // Let T = Config.tablet_reshard_target_size.
    //   splitThreshold     = ceil(1.5 * T)   (split fires when dataSize >= splitThreshold)
    //   mergePairThreshold = ceil(0.8 * T)   (merge fires when pair sum < mergePairThreshold)
    //   mergeGroupCap      = T               (merge groups packed up to mergeGroupCap)
    //
    // The thresholds use exclusive ceilings so the strict-`<` and strict-`>=`
    // comparisons accept the full half-open intervals implied by the design
    // (pair sum < 0.8*T, dataSize >= 1.5*T) for non-integer multiples too.
    //
    // Convergence requires (assuming BE row-count split approximately preserves byte balance):
    //   2 * (1 - 0.5 / 1.5) > 4/5    // post-split min piece pair > merge pair threshold
    //   1                  < 1.5     // merge group cap < split threshold
    //
    // Helpers below are overflow-safe for T <= MAX_SAFE_TARGET (~4.6 EB),
    // far above any plausible target. calcSplitCount() guards larger targets
    // by returning 1 (no split) so the wrap-around in splitThreshold can't
    // produce a bogus split decision.
    private static final long MAX_SAFE_TARGET = Long.MAX_VALUE / 2;

    @VisibleForTesting
    static long splitThreshold(long target) {
        // ceil(1.5T) = T + ceil(T/2). Overflow-safe for T <= MAX_SAFE_TARGET.
        return target + target / 2 + (target & 1L);
    }

    @VisibleForTesting
    static long mergePairThreshold(long target) {
        // ceil(0.8T) = (T/5)*4 + ceil(((T%5)*4)/5). Avoids T*4 overflow.
        return (target / 5) * 4 + (((target % 5) * 4) + 4) / 5;
    }

    @VisibleForTesting
    static long mergeGroupCap(long target) {
        return target;
    }

    public static boolean needSplit(long dataSize) {
        // Reuse calcSplitCount so we never trigger when no actual split would be produced
        // (e.g., tablet_reshard_max_split_count <= 1).
        return calcSplitCount(dataSize, Config.tablet_reshard_target_size) > 1;
    }

    public static boolean needMerge(long minAdjacentTabletPairSize) {
        long target = Config.tablet_reshard_target_size;
        if (target <= 0) {
            return false;
        }
        return minAdjacentTabletPairSize < mergePairThreshold(target);
    }

    /*
     * Return value > 1 if need split
     * Return value = 1 if not need split
     * Return value <= 0 if exception occurs
     */
    public static int calcSplitCount(long dataSize, long targetSize) {
        if (targetSize <= 0) {
            // A value less than 0 indicates the specified split count,
            // for internal testing.
            long splitCount = -targetSize;
            if (splitCount > Config.tablet_reshard_max_split_count) {
                return 0;
            }
            return (int) splitCount;
        }

        if (targetSize > MAX_SAFE_TARGET) {
            // splitThreshold(targetSize) would wrap around; no plausible long
            // dataSize can exceed ceil(1.5T) at this magnitude. Treat as "no split"
            // rather than risking the lower-bounded `Math.max(2, ...)` below
            // producing a bogus positive split count for any input.
            return 1;
        }

        if (dataSize < splitThreshold(targetSize)) {
            return 1;
        }

        // round-to-nearest, lower-bounded at 2, fully overflow-safe
        long quotient = dataSize / targetSize;
        long remainder = dataSize - quotient * targetSize;
        long halfTargetCeil = targetSize / 2 + (targetSize & 1L);
        long n = (remainder >= halfTargetCeil) ? quotient + 1 : quotient;
        n = Math.max(2L, n);
        return (int) Math.min((long) Config.tablet_reshard_max_split_count, n);
    }
}
