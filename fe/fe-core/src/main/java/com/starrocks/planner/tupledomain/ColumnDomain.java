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

package com.starrocks.planner.tupledomain;

import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public final class ColumnDomain {
    /**
     * Describes the domain of column values extracted from its filter conditions
     * There can be two possible representations, currently combined
     * - When values are nonempty, where range=none and hasNulls=false, meaning a discrete domain of values, e.g. from in
     * - When values are empty, then either range.isPresent or hasNulls, meaning a continuous domain of values or nulls
     * none() is always represented as Set.empty() + Optional.empty() + notNulls;
     * all() is always represented as Set.empty() + Range.all() + nulls
     */
    private final SortedSet<ColumnValue> values;
    private final Optional<Range<ColumnValue>> range;
    private final boolean hasNulls;
    private ColumnDomain(SortedSet<ColumnValue> values, Optional<Range<ColumnValue>> range, boolean hasNulls)
    {
        if (!values.isEmpty()) {
            Preconditions.checkArgument(!range.isPresent(), "domains should either be values or range");
            Preconditions.checkArgument(!hasNulls, "values domain does not support nulls");
        }
        if (range.isPresent() /* && values.isEmpty()*/) {
            if (range.get().isEmpty()) {
                range = Optional.empty();
            } else if (TupleDomainUtils.isSingleton(range.get()) && !hasNulls) { // check if range can be converted to a single value
                values = new TreeSet<>();
                values.add(range.get().lowerEndpoint());
                range = Optional.empty();
            }
        }
        this.values = values;
        this.range = range;
        this.hasNulls = hasNulls;
    }

    public boolean canListValues() {
        return !range.isPresent();
    }

    public boolean isNone() {
        return values.isEmpty() && !range.isPresent() && !hasNulls;
    }

    public boolean isAll() {
        return range.isPresent() && range.get().equals(Range.all()) && hasNulls;
    }

    public static ColumnDomain none() {
        return new ColumnDomain(new TreeSet<>(), Optional.empty(), false);
    }

    public static ColumnDomain all() {
        return new ColumnDomain(new TreeSet<>(), Optional.of(Range.all()), true);
    }

    public static ColumnDomain onlyNull() {
        return new ColumnDomain(new TreeSet<>(), Optional.empty(), true);
    }

    public static ColumnDomain notNull() {
        return new ColumnDomain(new TreeSet<>(), Optional.of(Range.all()), false);
    }

    public static ColumnDomain equalTo(ColumnValue value) {
        SortedSet<ColumnValue> valueSet = new TreeSet<>();
        valueSet.add(value);
        return new ColumnDomain(valueSet, Optional.empty(), false);
    }

    public static ColumnDomain in(Collection<ColumnValue> values) {
        return new ColumnDomain(new TreeSet<>(values), Optional.empty(), false);
    }

    public static ColumnDomain between(ColumnValue lowerBound, ColumnValue upperBound) {
        return new ColumnDomain(new TreeSet<>(), Optional.of(Range.closed(lowerBound, upperBound)), false);
    }

    public static ColumnDomain lessThan(ColumnValue value) {
        return new ColumnDomain(new TreeSet<>(), Optional.of(Range.upTo(value, BoundType.OPEN)), false);
    }

    public static ColumnDomain lessThanOrEqualTo(ColumnValue value) {
        return new ColumnDomain(new TreeSet<>(), Optional.of(Range.upTo(value, BoundType.CLOSED)), false);
    }

    public static ColumnDomain greaterThan(ColumnValue value) {
        return new ColumnDomain(new TreeSet<>(), Optional.of(Range.downTo(value, BoundType.OPEN)), false);
    }

    public static ColumnDomain greaterThanOrEqualTo(ColumnValue value) {
        return new ColumnDomain(new TreeSet<>(), Optional.of(Range.downTo(value, BoundType.CLOSED)), false);
    }

    public SortedSet<ColumnValue> getValues() {
        return values;
    }

    public Range<ColumnValue> getRange() {
        return range.orElseGet(() -> Range.closed(values.first(), values.last()));
    }

    private boolean hasNulls()
    {
        return hasNulls;
    }

    public ColumnDomain intersect(ColumnDomain other) {
        if (this == other)
            return this;
        if (this.isNone() || other.isNone())
            return none();
        if (this.isAll() || this.getRange().encloses(other.getRange()))
            return other;
        if (other.isAll() || other.getRange().encloses(this.getRange()))
            return this;

        // intersect values
        if (this.canListValues() && other.canListValues()) {
            SortedSet<ColumnValue> intersection = new TreeSet<>(this.getValues());
            intersection.retainAll(other.getValues());
            return in(intersection);
        }

        if (!this.getRange().isConnected(other.getRange())) {
            return none();
        }
        Range<ColumnValue> intersection = this.getRange().intersection(other.getRange());
        boolean intersectionHasNulls = this.hasNulls() && other.hasNulls();
        return new ColumnDomain(new TreeSet<>(), Optional.of(intersection), intersectionHasNulls);
    }

    public ColumnDomain union(ColumnDomain other) {
        if (this.isAll() || other.isAll())
            return all();
        if (this.isNone())
            return other;
        if (other.isNone())
            return this;

        if (this.canListValues() && other.canListValues()) {
            SortedSet<ColumnValue> unite = new TreeSet<>(this.getValues());
            unite.addAll(other.getValues());
            return in(unite);
        }

        Range<ColumnValue> unite = this.getRange().span(other.getRange());
        boolean unionHasNulls = this.hasNulls() || other.hasNulls();
        return new ColumnDomain(new TreeSet<>(), Optional.of(unite), unionHasNulls);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values, range, hasNulls);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ColumnDomain other = (ColumnDomain) obj;
        return Objects.equals(this.values, other.values) &&
                Objects.equals(this.range, other.range) &&
                this.hasNulls == other.hasNulls;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("Domain");
        if (isNone()) {
            builder.append("[None]");
        } else if (isAll()) {
            builder.append("[All]");
        } else if (canListValues()) {
            builder.append("{");
            builder.append(values.stream().map(ColumnValue::toString).collect(Collectors.joining(",")));
            builder.append("}");
        } else if (!range.isPresent() && hasNulls) {
            builder.append("[Nulls]");
        } else {
            builder.append(range.get());
            if (!hasNulls) {
                builder.append(" not null");
            }
        }
        return builder.toString();
    }

}
