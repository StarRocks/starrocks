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
    // mergePairThreshold and mergeGroupCap are overflow-safe for any positive long T.
    // splitThreshold = T + T/2 + (T & 1) overflows when T > floor(2/3 * Long.MAX_VALUE)
    // (~6.15 EB). calcSplitCount() detects that exact boundary algebraically and falls
    // back to "no split" for inputs above it, so the wrap-around can't produce a
    // bogus positive split count.

    @VisibleForTesting
    static long splitThreshold(long target) {
        // ceil(1.5T) = T + ceil(T/2). Caller must check splitThresholdOverflows(target) first.
        return target + target / 2 + (target & 1L);
    }

    /**
     * True iff splitThreshold(target) would overflow long for the given non-negative target.
     * Algebraically: T + T/2 + (T & 1) > Long.MAX_VALUE.
     * Computed via the rearrangement T > Long.MAX_VALUE - T/2 - (T & 1), which never
     * underflows because T/2 and (T & 1) are themselves non-negative and <= Long.MAX_VALUE.
     */
    @VisibleForTesting
    static boolean splitThresholdOverflows(long target) {
        return target > Long.MAX_VALUE - target / 2 - (target & 1L);
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

        if (splitThresholdOverflows(targetSize)) {
            // ceil(1.5 * targetSize) does not fit in long; no possible dataSize can
            // satisfy the threshold so treat as "no split" instead of letting the
            // lower-bounded Math.max(2, ...) below produce a bogus positive count.
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
