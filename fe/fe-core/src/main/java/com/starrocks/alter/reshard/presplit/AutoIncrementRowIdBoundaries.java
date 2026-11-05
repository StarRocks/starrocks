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
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Derives range-split boundaries for a table whose sort key is a single BIGINT AUTO_INCREMENT
 * hidden row-id column, reading no data at all: the ids come from one FE counter that starts at
 * 1 and normally only moves forward, so an estimated row count is enough to carve the id space,
 * starting at the counter, into equal-sized shares. A counter rewritten by RESTORE or failover can
 * leave cached ids above it; their rows land wherever those ids fall, which affects balance only.
 *
 * <p>The derivation is useful only while each tablet's share of rows dwarfs the ids that
 * node-side caching leaves unused. {@link #plan} explains how that bound is computed where it
 * enforces it.
 */
final class AutoIncrementRowIdBoundaries {

    /**
     * First id {@code LocalMetastore#allocateAutoIncrementId} hands out for a table whose counter
     * has never been touched: the low end of the span in that case.
     */
    private static final long FIRST_ROW_ID = 1;

    /**
     * How many times larger a tablet's target row count must be than the ids node-side caching can
     * leave unused. Ten keeps the worst-case per-tablet deviation around 10%.
     */
    private static final long GAP_HEADROOM_MULTIPLE = 10;

    private AutoIncrementRowIdBoundaries() {
    }

    /**
     * @param currentAutoIncrementId FE's auto-increment counter for the target table, {@code null}
     *                               when no id has ever been allocated for it
     * @param totalRows              estimated number of rows the load will write
     * @param requestedTabletCount   tablet count the caller's byte-based sizing asked for
     * @param activeComputeNodeCount compute nodes that can hold cached id blocks; the caller
     *                               resolves it through {@code TabletReshardUtils#computeNodeCount},
     *                               which never returns less than 1
     * @param sortKeyColumn          the row-id column the cuts are expressed in
     */
    static DerivedBoundarySource.Result plan(Long currentAutoIncrementId, long totalRows,
                                            int requestedTabletCount, int activeComputeNodeCount,
                                            Column sortKeyColumn) {
        Objects.requireNonNull(sortKeyColumn, "sortKeyColumn");

        int idCacheSize = Config.auto_increment_cache_size;
        if (idCacheSize <= 0) {
            return DerivedBoundarySource.Result.skipped(SkipReason.DERIVATION_FAILED);
        }
        if (totalRows <= 0) {
            return DerivedBoundarySource.Result.skipped(SkipReason.ESTIMATE_UNAVAILABLE);
        }

        // The span starts at the counter: every id a later grant hands out is at or above it. A
        // counter that has never allocated proves no node holds an id for this table (table ids are
        // never reused and a node's cache is only ever filled by the allocation RPC), so the span
        // starts at FIRST_ROW_ID and no leftovers exist.
        long spanStart = currentAutoIncrementId == null ? FIRST_ROW_ID : currentAutoIncrementId;

        // Ids are dense apart from bounded gaps: each node consumes its cached blocks fully except
        // its last one, so at most activeComputeNodeCount * auto_increment_cache_size ids go unused.
        // A gap carries no rows, making whichever tablet contains it light and the final, unbounded
        // tablet correspondingly heavy; keeping a tablet's target row count an order of magnitude
        // above the total gap size bounds that imbalance. The arithmetic saturates because an absurd
        // node count or cache size would otherwise wrap into a small denominator and under-clamp.
        //
        // A used counter doubles that mass: each node may also hold the unconsumed tail of its last
        // grant (at most auto_increment_cache_size - 1 ids below the counter), whose rows land in the
        // first tablet. The bound assumes those tails were granted under the cache size in force now;
        // after the setting is lowered a larger old tail can skew the first tablet until it is used
        // up. Only balance depends on this, since the ranges are open at both ends.
        long gapMass = saturatingMultiply(
                currentAutoIncrementId == null ? activeComputeNodeCount : 2L * activeComputeNodeCount, idCacheSize);
        long denominator = saturatingMultiply(GAP_HEADROOM_MULTIPLE, gapMass);
        long maxUsefulTabletCount = totalRows / denominator;
        int effectiveTabletCount = (int) Math.min(requestedTabletCount, maxUsefulTabletCount);
        if (effectiveTabletCount < 2) {
            return DerivedBoundarySource.Result.skipped(SkipReason.ROW_ID_SPAN_TOO_SMALL);
        }

        long stride = rowIdStride(totalRows, effectiveTabletCount);
        List<Tuple> cuts = new ArrayList<>(effectiveTabletCount - 1);
        long cut = spanStart;
        for (int cutOrdinal = 1; cutOrdinal < effectiveTabletCount; cutOrdinal++) {
            // Out of reach for any estimate a load can produce unless the counter is near Long.MAX_VALUE,
            // but the accumulator must not wrap into a cut that sits below its predecessor.
            if (cut > Long.MAX_VALUE - stride) {
                break;
            }
            cut += stride;
            // The variant carries the schema's own type: the BE comparator orders boundaries against
            // the stored key, so a mismatched type would mis-place rows across tablets.
            cuts.add(new Tuple(List.of(Variant.of(sortKeyColumn.getType(), Long.toString(cut)))));
        }
        if (cuts.isEmpty()) {
            return DerivedBoundarySource.Result.skipped(SkipReason.ROW_ID_SPAN_TOO_SMALL);
        }
        return DerivedBoundarySource.Result.of(new BoundaryPlannerResult(cuts));
    }

    /**
     * Rows per tablet: {@code ceil(totalRows / tabletCount)} written so it cannot overflow and never
     * returns 0, which keeps the cuts strictly increasing even for an estimate smaller than the
     * tablet count. Requires both arguments to be positive.
     */
    static long rowIdStride(long totalRows, int tabletCount) {
        return (totalRows - 1) / tabletCount + 1;
    }

    /** Product of two non-negative values, clamped to {@link Long#MAX_VALUE} instead of wrapping. */
    private static long saturatingMultiply(long left, long right) {
        try {
            return Math.multiplyExact(left, right);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }
}
