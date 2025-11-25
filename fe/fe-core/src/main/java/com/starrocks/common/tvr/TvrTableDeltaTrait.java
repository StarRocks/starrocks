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

package com.starrocks.common.tvr;

import java.util.EnumSet;
import java.util.Objects;

/**
 * TvrTableDeltaTrait represents the delta and associated trait infos of a Time Varying Relation (TVR).
 */
public class TvrTableDeltaTrait {

    public static final TvrTableDeltaTrait DEFAULT = new TvrTableDeltaTrait(TvrTableDelta.empty(),
            TvrChangeType.MONOTONIC, TvrDeltaStats.EMPTY);

    /**
     * TvrChangeType indicates the type of change in the TVR delta.
     */
    public enum TvrChangeType {
        MONOTONIC,   // Indicates that the tvr version is monotonically increasing (INSERT only)
        RETRACTABLE  // Indicates that the tvr version can be retracted (supports DELETE/UPDATE)
    }

    /**
     * StreamRowOp represents the operation type for each row in incremental computation.
     * This mirrors the BE's StreamRowOp enum in stream_chunk.h.
     */
    public enum StreamRowOp {
        OP_INSERT(0),        // Add a new row
        OP_DELETE(1),        // Delete an existing row
        OP_UPDATE_BEFORE(2), // Previous value of an UPDATE
        OP_UPDATE_AFTER(3);  // New value of an UPDATE

        private final int value;

        StreamRowOp(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    /**
     * Default possible ops for MONOTONIC (INSERT only)
     */
    public static final EnumSet<StreamRowOp> MONOTONIC_OPS = EnumSet.of(StreamRowOp.OP_INSERT);

    /**
     * Default possible ops for RETRACTABLE (all ops)
     */
    public static final EnumSet<StreamRowOp> RETRACTABLE_OPS = EnumSet.allOf(StreamRowOp.class);

    private final TvrTableDelta tvrTableDelta;
    private final TvrChangeType tvrChangeType;
    private final TvrDeltaStats tvrDeltaStats;
    // The set of possible operations that may appear in this delta
    private final EnumSet<StreamRowOp> possibleOps;

    public TvrTableDeltaTrait(TvrTableDelta tvrTableDelta,
                              TvrChangeType tvrChangeType,
                              TvrDeltaStats tvrDeltaStats) {
        this(tvrTableDelta, tvrChangeType, tvrDeltaStats,
             tvrChangeType == TvrChangeType.MONOTONIC ? MONOTONIC_OPS : RETRACTABLE_OPS);
    }

    public TvrTableDeltaTrait(TvrTableDelta tvrTableDelta,
                              TvrChangeType tvrChangeType,
                              TvrDeltaStats tvrDeltaStats,
                              EnumSet<StreamRowOp> possibleOps) {
        this.tvrTableDelta = tvrTableDelta;
        this.tvrChangeType = tvrChangeType;
        this.tvrDeltaStats = tvrDeltaStats;
        this.possibleOps = possibleOps;
    }

    public static TvrTableDeltaTrait ofMonotonic(TvrTableDelta tvrTableDelta) {
        return new TvrTableDeltaTrait(tvrTableDelta, TvrChangeType.MONOTONIC, TvrDeltaStats.EMPTY, MONOTONIC_OPS);
    }

    public static TvrTableDeltaTrait ofMonotonic(TvrTableDelta tvrTableDelta,
                                                 TvrDeltaStats tvrDeltaStats) {
        return new TvrTableDeltaTrait(tvrTableDelta, TvrChangeType.MONOTONIC, tvrDeltaStats, MONOTONIC_OPS);
    }

    public static TvrTableDeltaTrait ofRetractable(TvrTableDelta tvrTableDelta) {
        return new TvrTableDeltaTrait(tvrTableDelta, TvrChangeType.RETRACTABLE, TvrDeltaStats.EMPTY, RETRACTABLE_OPS);
    }

    public static TvrTableDeltaTrait ofRetractable(TvrTableDelta tvrTableDelta,
                                                   TvrDeltaStats tvrDeltaStats) {
        return new TvrTableDeltaTrait(tvrTableDelta, TvrChangeType.RETRACTABLE, tvrDeltaStats, RETRACTABLE_OPS);
    }

    /**
     * Create a retractable trait with specific ops (e.g., only DELETE ops)
     */
    public static TvrTableDeltaTrait ofRetractable(TvrTableDelta tvrTableDelta,
                                                   TvrDeltaStats tvrDeltaStats,
                                                   EnumSet<StreamRowOp> possibleOps) {
        return new TvrTableDeltaTrait(tvrTableDelta, TvrChangeType.RETRACTABLE, tvrDeltaStats, possibleOps);
    }

    public boolean isAppendOnly() {
        return tvrChangeType == TvrChangeType.MONOTONIC;
    }

    public boolean isRetractable() {
        return tvrChangeType == TvrChangeType.RETRACTABLE;
    }

    /**
     * Check if this delta may contain DELETE operations
     */
    public boolean hasDeletes() {
        return possibleOps.contains(StreamRowOp.OP_DELETE);
    }

    /**
     * Check if this delta may contain UPDATE operations
     */
    public boolean hasUpdates() {
        return possibleOps.contains(StreamRowOp.OP_UPDATE_BEFORE) ||
               possibleOps.contains(StreamRowOp.OP_UPDATE_AFTER);
    }

    /**
     * Check if this delta may contain INSERT operations
     */
    public boolean hasInserts() {
        return possibleOps.contains(StreamRowOp.OP_INSERT);
    }

    public TvrTableDelta getTvrDelta() {
        return tvrTableDelta;
    }

    public TvrChangeType getTvrChangeType() {
        return tvrChangeType;
    }

    public TvrDeltaStats getTvrDeltaStats() {
        return tvrDeltaStats;
    }

    public EnumSet<StreamRowOp> getPossibleOps() {
        return possibleOps;
    }

    /**
     * Merge two traits to compute the output trait of a binary operator (e.g., JOIN).
     * The result is RETRACTABLE if either input is RETRACTABLE.
     * The possible ops is the union of both inputs' possible ops.
     */
    public static TvrTableDeltaTrait merge(TvrTableDeltaTrait left, TvrTableDeltaTrait right) {
        TvrChangeType mergedType = (left.isRetractable() || right.isRetractable())
                ? TvrChangeType.RETRACTABLE
                : TvrChangeType.MONOTONIC;

        EnumSet<StreamRowOp> mergedOps = EnumSet.copyOf(left.possibleOps);
        mergedOps.addAll(right.possibleOps);

        // Use left's delta as the base (could also combine them)
        return new TvrTableDeltaTrait(left.tvrTableDelta, mergedType, TvrDeltaStats.EMPTY, mergedOps);
    }

    /**
     * Create a new trait that marks output as RETRACTABLE due to OUTER JOIN.
     * OUTER JOIN can produce NULL rows that may need to be retracted later.
     */
    public TvrTableDeltaTrait withOuterJoinRetractable() {
        if (isRetractable()) {
            return this;
        }
        // OUTER JOIN output may have DELETE ops (retracting NULL rows)
        EnumSet<StreamRowOp> newOps = EnumSet.copyOf(possibleOps);
        newOps.add(StreamRowOp.OP_DELETE);
        return new TvrTableDeltaTrait(tvrTableDelta, TvrChangeType.RETRACTABLE, tvrDeltaStats, newOps);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TvrTableDeltaTrait that = (TvrTableDeltaTrait) o;
        return Objects.equals(tvrChangeType, that.tvrChangeType) &&
                Objects.equals(tvrTableDelta, that.tvrTableDelta) &&
                Objects.equals(possibleOps, that.possibleOps);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tvrChangeType, tvrTableDelta, possibleOps);
    }

    @Override
    public String toString() {
        return "DeltaTrait{" +
                "delta=" + tvrTableDelta +
                ", changeType=" + tvrChangeType +
                ", possibleOps=" + possibleOps +
                ", stats=" + tvrDeltaStats +
                '}';
    }
}
