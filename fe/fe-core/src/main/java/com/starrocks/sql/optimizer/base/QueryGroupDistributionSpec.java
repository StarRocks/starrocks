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


package com.starrocks.sql.optimizer.base;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;

/** Query-local complete-C ownership. This is only an aggregate proof, never a JOIN or exchange contract. */
public final class QueryGroupDistributionSpec extends DistributionSpec {
    private final long tableId;
    private final List<DistributionCol> orderedCColumns;
    private final List<Integer> orderedPhysicalCUniqueIds;
    private final EquivalentDescriptor equivalentDescriptor;

    public QueryGroupDistributionSpec(long tableId, List<DistributionCol> orderedCColumns,
                                      List<Integer> orderedPhysicalCUniqueIds, EquivalentDescriptor descriptor) {
        super(DistributionType.RANGE_QUERY_GROUP);
        Preconditions.checkArgument(orderedCColumns.size() == 3 && orderedPhysicalCUniqueIds.size() == 3,
                "Grouped range scan requires all three C columns");
        this.tableId = tableId;
        this.orderedCColumns = List.copyOf(orderedCColumns);
        this.orderedPhysicalCUniqueIds = List.copyOf(orderedPhysicalCUniqueIds);
        this.equivalentDescriptor = Objects.requireNonNull(descriptor);
    }

    public long getTableId() {
        return tableId;
    }

    public List<DistributionCol> getOrderedCColumns() {
        return orderedCColumns;
    }

    public List<Integer> getOrderedPhysicalCUniqueIds() {
        return orderedPhysicalCUniqueIds;
    }

    public EquivalentDescriptor getEquivalentDescriptor() {
        return equivalentDescriptor;
    }

    @Override
    public boolean isSatisfy(DistributionSpec required) {
        if (required.getType() == DistributionType.ANY) {
            return true;
        }
        if (!(required instanceof HashDistributionSpec hash)
                || hash.getHashDistributionDesc().getSourceType() != HashDistributionDesc.SourceType.SHUFFLE_AGG) {
            return false;
        }
        return orderedCColumns.stream().allMatch(column -> hash.getShuffleColumns().stream()
                .anyMatch(key -> equivalentDescriptor.isConnected(key.getNullStrictCol(), column)));
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof QueryGroupDistributionSpec other && tableId == other.tableId
                && orderedCColumns.equals(other.orderedCColumns)
                && orderedPhysicalCUniqueIds.equals(other.orderedPhysicalCUniqueIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, tableId, orderedCColumns, orderedPhysicalCUniqueIds);
    }

    @Override
    public String toString() {
        return "RANGE_QUERY_GROUP(" + orderedCColumns + ")";
    }
}
