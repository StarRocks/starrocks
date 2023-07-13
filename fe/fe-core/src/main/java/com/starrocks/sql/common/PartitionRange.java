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

import java.util.Comparator;
import java.util.Objects;

public class PartitionRange implements Comparable<PartitionRange> {
    private final Range<PartitionKey> partitionKeyRange;
    private final String partitionName;

    public static final Comparator<PartitionRange> PARTITION_RANGE_COMPARATOR = new Comparator<PartitionRange>() {
        @Override
        public int compare(PartitionRange a, PartitionRange b) {
            return a.compareTo(b);
        }
    };

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

    @Override
    public int compareTo(PartitionRange o) {
        if (isInteract(o)) {
            return 0;
        } else {
            return this.partitionKeyRange.lowerEndpoint().compareTo(o.partitionKeyRange.lowerEndpoint());
        }
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

    public boolean isInteract(PartitionRange o) {
        return this.partitionKeyRange.upperEndpoint().compareTo(o.partitionKeyRange.lowerEndpoint()) > 0 &&
                this.partitionKeyRange.lowerEndpoint().compareTo(o.partitionKeyRange.upperEndpoint()) < 0;
    }
}