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

package com.starrocks.rowstore;

import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.planner.tupledomain.ColumnDomain;
import com.starrocks.planner.tupledomain.ColumnValue;
import com.starrocks.planner.tupledomain.SortedRanges;
import com.starrocks.planner.tupledomain.TupleDomain;
import com.starrocks.planner.tupledomain.TupleDomainUnion;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * similar to PartitionKey, it describes a tuple of RowStore primary key in lexicographical order
 * for simplicity, the value of every column must be specified in a tuple, ColumnValue.MIN_VALUE/MAX_VALUE can be used
 */
public class RowStoreKeyTuple implements Comparable<RowStoreKeyTuple> {
    private final List<ColumnValue> values;
    public RowStoreKeyTuple(List<ColumnValue> values) {
        this.values = values;
    }

    /**
     * converts a tuple domain into non-overlapping ranges for the given keys
     * this may not be equivalent to the original tuple domain, e.g. TupleDomain{b=1} will be interval (-oo,1) .. (+oo, 1)
     * there can be expansion, e.g. a in [1,2] and b in [3,4] will be expanded into 4 disjoint singletons
     */
    public static SortedRanges<RowStoreKeyTuple> extractDisjointRanges(TupleDomain domain, List<Column> keys) {

        if (domain.isNone()) {
            return new SortedRanges<>(); // no range
        }

        if (domain.isAll()) {
            Builder minTupleBuilder = builder();
            Builder maxTupleBuilder = builder();
            for (Column key : keys) {
                minTupleBuilder.add(ColumnValue.MIN_VALUE(key.getType()));
                maxTupleBuilder.add(ColumnValue.MAX_VALUE(key.getType()));
            }
            Range<RowStoreKeyTuple> minToMax = Range.closed(minTupleBuilder.build(), maxTupleBuilder.build());
            return new SortedRanges<>(minToMax); // trivial whole domain
        }

        Map<String, ColumnDomain> columnDomains = domain.getDomains().get();

        List<Builder> lowerBounds = new ArrayList<>(Collections.singleton(builder()));
        List<Builder> upperBounds = new ArrayList<>(Collections.singleton(builder()));
        boolean lowerBoundsClosed = true;
        boolean upperBoundsClosed = true;
        boolean canListValues = true;

        // to guarantee non-overlapping, the following strategy is used
        // as long as we can list prefix values, the values are non-overlapping because of sets
        // whenever a column has range domain, we only maintain one range for each prefix

        for (Column key : keys) {
            if (!columnDomains.containsKey(key.getName())) {
                // the column is all(), can skip listing all remaining values
                canListValues = false;
                lowerBounds.forEach(builder -> builder.add(ColumnValue.MIN_VALUE(key.getType())));
                upperBounds.forEach(builder -> builder.add(ColumnValue.MAX_VALUE(key.getType())));
                continue;
            }
            ColumnDomain columnDomain = columnDomains.get(key.getName());
            canListValues &= columnDomain.canListValues();
            if (canListValues) {
                // for each value of the current column, create a separate range
                List<Builder> newLowerBounds = new ArrayList<>();
                List<Builder> newUpperBounds = new ArrayList<>();
                for (ColumnValue value : columnDomain.getValues()) {
                    for (Builder lower : lowerBounds) {
                        Builder newLower = new Builder(lower);
                        newLower.add(value);
                        newLowerBounds.add(newLower);
                    }
                    for (Builder upper : upperBounds) {
                        Builder newUpper = new Builder(upper);
                        newUpper.add(value);
                        newUpperBounds.add(newUpper);
                    }
                }
                lowerBounds = newLowerBounds;
                upperBounds = newUpperBounds;
            } else {
                // only append the min and max for the current column
                // todo nulls should also be handled here
                Range<ColumnValue> columnRange = columnDomain.getRange();
                ColumnValue lowerValue = (columnRange.hasLowerBound()) ? columnRange.lowerEndpoint() :
                        ColumnValue.MIN_VALUE(key.getType());
                boolean lowerClosed =
                        !columnRange.hasLowerBound() || columnRange.lowerBoundType().equals(BoundType.CLOSED);
                ColumnValue upperValue = (columnRange.hasUpperBound()) ? columnRange.upperEndpoint() :
                        ColumnValue.MAX_VALUE(key.getType());
                boolean upperClosed =
                        !columnRange.hasUpperBound() || columnRange.upperBoundType().equals(BoundType.CLOSED);
                lowerBounds.forEach(builder -> builder.add(lowerValue));
                upperBounds.forEach(builder -> builder.add(upperValue));
                lowerBoundsClosed &= lowerClosed;
                upperBoundsClosed &= upperClosed;
            }
        }

        SortedRanges<RowStoreKeyTuple> result = new SortedRanges<>();
        for (int i = 0; i < lowerBounds.size(); ++i) {
            result.add(Range.range(
                    lowerBounds.get(i).build(), lowerBoundsClosed ? BoundType.CLOSED : BoundType.OPEN,
                    upperBounds.get(i).build(), upperBoundsClosed ? BoundType.CLOSED : BoundType.OPEN));
        }
        return result;
    }

    public static SortedRanges<RowStoreKeyTuple> extractDisjointRanges(TupleDomainUnion domain, List<Column> keys) {
        if (domain.isNone()) {
            return extractDisjointRanges(TupleDomain.none(), keys); // no range
        }

        if (domain.isAll()) {
            return extractDisjointRanges(TupleDomain.all(), keys);
        }

        Set<TupleDomain> domains = domain.getTupleDomains().get();
        SortedRanges<RowStoreKeyTuple> result = new SortedRanges<>();

        for (TupleDomain tupleDomain : domains) {
            SortedRanges<RowStoreKeyTuple> newRanges = extractDisjointRanges(tupleDomain, keys);
            // merge the new ranges with existing ranges and ensure non-overlapping
            for (Range<RowStoreKeyTuple> newRange : newRanges) {
                // find all overlapping ranges with the new Range
                List<Range<RowStoreKeyTuple>> overlapping = new ArrayList<>();
                for (Range<RowStoreKeyTuple> existing : result) {
                    if (newRange.isConnected(existing)) {
                        overlapping.add(existing);
                    }
                }
                // union with the new range
                for (Range<RowStoreKeyTuple> overlap : overlapping) {
                    newRange = newRange.span(overlap);
                }
                overlapping.forEach(result::remove);
                result.add(newRange);
            }
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowStoreKeyTuple that = (RowStoreKeyTuple) o;
        return Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values);
    }

    @Override
    public int compareTo(@NotNull RowStoreKeyTuple o) {
        Preconditions.checkArgument(this.values.size() == o.values.size(), "cannot compare two tuples with different size");
        for (int i = 0; i < this.values.size(); ++i) {
            int cmp = this.values.get(i).compareTo(o.values.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "(" + values.stream().map(ColumnValue::toString).collect(Collectors.joining(",")) + ")";
    }

    public static class Builder {
        private List<ColumnValue> valueBuffer;

        public Builder() {
            valueBuffer = new ArrayList<>();
        }

        public Builder(Builder other) {
            valueBuffer = new ArrayList<>(other.valueBuffer);
        }

        public Builder add(ColumnValue value) {
            valueBuffer.add(value);
            return this;
        }
        public RowStoreKeyTuple build() {
            return new RowStoreKeyTuple(valueBuffer);
        }
    }

    public List<ColumnValue> getValues() {
        return values;
    }
}
