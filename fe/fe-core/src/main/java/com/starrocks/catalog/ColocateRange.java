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

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Range;

import java.util.Objects;

/**
 * Represents a colocate range interval and its corresponding PACK shard group.
 *
 * <p>In a range distribution colocate group, data is partitioned into colocate ranges
 * based on colocate column (sort key prefix) values. Each colocate range is mapped to
 * a PACK shard group, and all tablets within the same colocate range across different
 * partitions/tables are scheduled to the same compute node by StarOS.
 *
 * <p>Implements {@code Comparable<Range<Tuple>>} by delegating to the internal range's
 * {@link Range#compareTo(Range)}, which returns 0 for overlapping ranges. This enables
 * direct use of {@link java.util.Collections#binarySearch} on a sorted list of
 * ColocateRange with a point range as the search key.
 */
public class ColocateRange implements Comparable<Range<Tuple>> {

    @SerializedName("r")
    private final Range<Tuple> range;

    @SerializedName("sg")
    private final long shardGroupId;

    public ColocateRange(Range<Tuple> range, long shardGroupId) {
        this.range = Objects.requireNonNull(range, "range must not be null");
        this.shardGroupId = shardGroupId;
    }

    public Range<Tuple> getRange() {
        return range;
    }

    public long getShardGroupId() {
        return shardGroupId;
    }

    @Override
    public int compareTo(Range<Tuple> other) {
        return range.compareTo(other);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        ColocateRange other = (ColocateRange) object;
        return shardGroupId == other.shardGroupId && Objects.equals(range, other.range);
    }

    @Override
    public int hashCode() {
        return Objects.hash(range, shardGroupId);
    }

    @Override
    public String toString() {
        return range.toString() + " -> shardGroup=" + shardGroupId;
    }
}
