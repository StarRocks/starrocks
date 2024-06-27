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

package com.starrocks.sql.common;

import com.google.common.collect.Range;
import com.starrocks.catalog.PartitionKey;

import java.util.Objects;

/**
 * {@link PRangeCell} contains the range partition's value which contains a `PartitionKey` range.
 */
public final class PRangeCell extends PCell implements Comparable<PRangeCell> {
    private final Range<PartitionKey> range;

    public PRangeCell(Range<PartitionKey> partitionKeyRange) {
        this.range = partitionKeyRange;
    }

    public Range<PartitionKey> getRange() {
        return range;
    }

    /**
     * {@link PRangeCell}'s compareTo method is not an exact comparator, but it can work for Partition Ranges:
     *   1. Partitions are serial un-connected ranges which are not interact between each other, so we can just compare
     * `lowerEndPoint` directly.
     *   2. Choose two interact partition ranges as `equal` to let callers handle it directly.
     */
    @Override
    public int compareTo(PRangeCell o) {
        if (isIntersected(o)) {
            return 0;
        }
        return this.range.lowerEndpoint().compareTo(o.range.lowerEndpoint());
    }

    /**
     * Check two partition range is `interact` which is a bit different from Range's `isConnected` method, eg:
     * [2, 4) and [4, 6) are not interact;
     * [2, 4) and [4, 6) are connected, because both enclose the empty range [4, 4).
     *
     * public boolean isConnected(Range<C> other) {
     *     return lowerBound.compareTo(other.upperBound) <= 0
     *         && other.lowerBound.compareTo(upperBound) <= 0;
     *   }
     */
    public boolean isIntersected(PRangeCell o) {
        return this.range.upperEndpoint().compareTo(o.range.lowerEndpoint()) > 0 &&
                this.range.lowerEndpoint().compareTo(o.range.upperEndpoint()) < 0;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o == null || !(o instanceof PRangeCell)) {
            return false;
        }
        PRangeCell range = (PRangeCell) o;
        return this.range.equals(range.range);
    }

    @Override
    public int hashCode() {
        return Objects.hash(range);
    }

    @Override
    public String toString() {
        return "PRangeCell{" +
                "range=" + range +
                '}';
    }
}
