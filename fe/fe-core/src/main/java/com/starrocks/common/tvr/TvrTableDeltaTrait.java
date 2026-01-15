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
        MONOTONIC, // Indicates that the tvr version is monotonically increasing
        RETRACTABLE // Indicates that the tvr version can be retracted
    }

    private final TvrTableDelta tvrTableDelta;
    private final TvrChangeType tvrChangeType;
    private final TvrDeltaStats tvrDeltaStats;

    public TvrTableDeltaTrait(TvrTableDelta tvrTableDelta,
                              TvrChangeType tvrChangeType,
                              TvrDeltaStats tvrDeltaStats) {
        this.tvrTableDelta = tvrTableDelta;
        this.tvrChangeType = tvrChangeType;
        this.tvrDeltaStats = tvrDeltaStats;
    }

    public static TvrTableDeltaTrait ofMonotonic(TvrTableDelta tvrTableDelta) {
        return new TvrTableDeltaTrait(tvrTableDelta, TvrChangeType.MONOTONIC, TvrDeltaStats.EMPTY);
    }

    public static TvrTableDeltaTrait ofMonotonic(TvrTableDelta tvrTableDelta,
                                                 TvrDeltaStats tvrDeltaStats) {
        return new TvrTableDeltaTrait(tvrTableDelta, TvrChangeType.MONOTONIC, tvrDeltaStats);
    }

    public static TvrTableDeltaTrait ofRetractable(TvrTableDelta tvrTableDelta) {
        return new TvrTableDeltaTrait(tvrTableDelta, TvrChangeType.RETRACTABLE, TvrDeltaStats.EMPTY);
    }

    public static TvrTableDeltaTrait ofRetractable(TvrTableDelta tvrTableDelta,
                                                   TvrDeltaStats tvrDeltaStats) {
        return new TvrTableDeltaTrait(tvrTableDelta, TvrChangeType.RETRACTABLE, tvrDeltaStats);
    }

    public boolean isAppendOnly() {
        return tvrChangeType == TvrChangeType.MONOTONIC;
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
                Objects.equals(tvrTableDelta, that.tvrTableDelta);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tvrChangeType, tvrTableDelta);
    }

    @Override
    public String toString() {
        return "DeltaTrait{" +
                "delta=" + tvrTableDelta +
                ", changeType=" + tvrChangeType +
                ", stats=" + tvrDeltaStats +
                '}';
    }
}
