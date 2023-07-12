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
 * `PartitionRange` contains a `PartitionKey` range and the partition's name to represent a table's partition range info.
 */
public class PartitionRange implements Comparable<PartitionRange> {
    private final Range<PartitionKey> partitionKeyRange;
    private final String partitionName;

    public PartitionRange(String partitionName, Range<PartitionKey> partitionKeyRange) {
        this.partitionName = partitionName;
        this.partitionKeyRange = partitionKeyRange;
    }

    public Range<PartitionKey> getPartitionKeyRange() {
        return partitionKeyRange;
    }

    public String getPartitionName() {
        return partitionName;
    }

    /**
     * `PartitionRange`'s compareTo method is not an exact comparator, but it can work for Partition Ranges:
     *   1. Partitions are serial un-connected ranges which are not interact between each other, so we can just compare
     * `lowerEndPoint` directly.
     *   2. Choose two interact partition ranges as `equal` to let callers handle it directly.
     */
    @Override
    public int compareTo(PartitionRange o) {
        if (isInteract(o)) {
            return 0;
        }
        return this.partitionKeyRange.lowerEndpoint().compareTo(o.partitionKeyRange.lowerEndpoint());
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
    public boolean isInteract(PartitionRange o) {
        return this.partitionKeyRange.upperEndpoint().compareTo(o.partitionKeyRange.lowerEndpoint()) > 0 &&
                this.partitionKeyRange.lowerEndpoint().compareTo(o.partitionKeyRange.upperEndpoint()) < 0;
    }

    @Override
    public boolean equals(Object o) {

        if (o == this) {
            return true;
        }
        if (o == null || !(o instanceof PartitionRange)) {
            return false;
        }
        PartitionRange range = (PartitionRange) o;

        return this.partitionName.equals(((PartitionRange) o).partitionName) &&
                this.partitionKeyRange.equals(range.partitionKeyRange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionName, partitionKeyRange);
    }
}
