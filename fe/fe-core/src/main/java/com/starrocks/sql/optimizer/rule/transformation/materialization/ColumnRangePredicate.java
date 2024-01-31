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


package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeSet;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

// all ranges about one column ref
// eg: a > 10 and a < 100 => a -> (10, 100)
// (a > 10 and a < 100) or (a > 1000 and a <= 10000) => a -> (10, 100) or (1000, 10000]
public class ColumnRangePredicate extends RangePredicate {
    private ColumnRefOperator columnRef;
    // the relation between each Range in RangeSet is 'or'
    private TreeRangeSet<ConstantOperator> columnRanges;

    private TreeRangeSet<ConstantOperator> canonicalColumnRanges;

    public ColumnRangePredicate(ColumnRefOperator columnRef, TreeRangeSet<ConstantOperator> columnRanges) {
        this.columnRef = columnRef;
        this.columnRanges = columnRanges;
        List<Range<ConstantOperator>> canonicalRanges = new ArrayList<>();
        if (ConstantOperatorDiscreteDomain.isSupportedType(columnRef.getType())) {
            for (Range range : this.columnRanges.asRanges()) {
                Range canonicalRange = range.canonical(new ConstantOperatorDiscreteDomain());
                canonicalRanges.add(canonicalRange);
            }
            this.canonicalColumnRanges = TreeRangeSet.create(canonicalRanges);
        } else {
            this.canonicalColumnRanges = columnRanges;
        }
    }

    public ColumnRefOperator getColumnRef() {
        return columnRef;
    }

    public static ColumnRangePredicate andRange(
            ColumnRangePredicate rangePredicate, ColumnRangePredicate otherRangePredicate) {
        List<Range<ConstantOperator>> ranges = new ArrayList<>();
        for (Range<ConstantOperator> range : rangePredicate.columnRanges.asRanges()) {
            if (otherRangePredicate.columnRanges.intersects(range)) {
                for (Range<ConstantOperator> otherRange : otherRangePredicate.columnRanges.asRanges()) {
                    if (range.isConnected(otherRange)) {
                        Range<ConstantOperator> intersection = range.intersection(otherRange);
                        if (!intersection.isEmpty()) {
                            ranges.add(intersection);
                        }
                    }
                }
            }
        }
        return new ColumnRangePredicate(rangePredicate.columnRef, TreeRangeSet.create(ranges));
    }

    public static ColumnRangePredicate orRange(
            ColumnRangePredicate rangePredicate, ColumnRangePredicate otherRangePredicate) {
        TreeRangeSet<ConstantOperator> result = TreeRangeSet.create();
        result.addAll(rangePredicate.columnRanges);
        result.addAll(otherRangePredicate.columnRanges);
        return new ColumnRangePredicate(rangePredicate.getColumnRef(), result);
    }

    public boolean isUnbounded() {
        return columnRanges.asRanges().stream().allMatch(range -> !range.hasUpperBound() && !range.hasLowerBound());
    }

    @Override
    public boolean enclose(RangePredicate other) {
        if (!(other instanceof ColumnRangePredicate)) {
            return false;
        }
        ColumnRangePredicate columnRangePredicate = other.cast();
        return canonicalColumnRanges.enclosesAll(columnRangePredicate.canonicalColumnRanges);
    }

    @Override
    public ScalarOperator toScalarOperator() {
        if (canonicalColumnRanges.isEmpty()) {
            return ConstantOperator.FALSE;
        }

        if (isUnbounded()) {
            return ConstantOperator.TRUE;
        }
        // process ne
        // for a < 100 || a > 100
        RangeSet<ConstantOperator> complement = columnRanges.complement();
        if (complement.asRanges().size() == 1 && isEqualRange(complement.asRanges().iterator().next())) {
            // complement will be [100, 100]
            Range<ConstantOperator> range = complement.asRanges().iterator().next();
            return BinaryPredicateOperator.ne(expression, range.lowerEndpoint());
        }

        return toScalarOperatorImpl(columnRanges);
    }

    private ScalarOperator toScalarOperatorImpl(TreeRangeSet<ConstantOperator> ranges) {
        List<ScalarOperator> orOperators = Lists.newArrayList();
        // process in predicates
        Set<Range<ConstantOperator>> rangeSet = ranges.asRanges();
        Set<Range<ConstantOperator>> equalRangeSet = Sets.newHashSet();
        rangeSet.forEach(r -> {
            if (isEqualRange(r)) {
                equalRangeSet.add(r);
            }
        });
        if (equalRangeSet.size() > 1) {
            List<ConstantOperator> constants = equalRangeSet.stream()
                    .map(this::getValue)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .sorted()
                    .collect(Collectors.toList());

            List<ScalarOperator> arguments = Lists.newLinkedList();
            arguments.add(expression);
            arguments.addAll(constants);

            InPredicateOperator inPredicateOperator = new InPredicateOperator(false, arguments);
            orOperators.add(inPredicateOperator);
            rangeSet.removeAll(equalRangeSet);
        }
        for (Range<ConstantOperator> range : rangeSet) {
            List<ScalarOperator> andOperators = Lists.newArrayList();
            if (range.hasLowerBound() && range.hasUpperBound()) {
                if (range.lowerBoundType() == BoundType.CLOSED
                        && range.upperBoundType() == BoundType.CLOSED
                        && range.upperEndpoint().equals(range.lowerEndpoint())) {
                    orOperators.add(BinaryPredicateOperator.eq(columnRef, range.lowerEndpoint()));
                    continue;
                } else if (range.lowerBoundType() == BoundType.CLOSED
                        && range.upperBoundType() == BoundType.OPEN
                        && range.lowerEndpoint().successor().isPresent()
                        && range.upperEndpoint().equals(range.lowerEndpoint().successor().get())) {
                    orOperators.add(BinaryPredicateOperator.eq(columnRef, range.lowerEndpoint()));
                    continue;
                }
            }
            if (range.hasLowerBound()) {
                if (range.lowerBoundType() == BoundType.CLOSED) {
                    andOperators.add(BinaryPredicateOperator.ge(columnRef, range.lowerEndpoint()));
                } else {
                    andOperators.add(BinaryPredicateOperator.gt(columnRef, range.lowerEndpoint()));
                }
            }

            if (range.hasUpperBound()) {
                if (range.upperBoundType() == BoundType.CLOSED) {
                    andOperators.add(BinaryPredicateOperator.le(columnRef, range.upperEndpoint()));
                } else {
                    andOperators.add(BinaryPredicateOperator.lt(columnRef, range.upperEndpoint()));
                }
            }
            orOperators.add(Utils.compoundAnd(andOperators));
        }
        return Utils.compoundOr(orOperators);
    }

    @Override
    public ScalarOperator simplify(RangePredicate other) {
        if (this.equals(other)) {
            return ConstantOperator.TRUE;
        }
        if (other instanceof ColumnRangePredicate) {
            ColumnRangePredicate otherColumnRangePredicate = (ColumnRangePredicate) other;
            if (!columnRef.equals(otherColumnRangePredicate.getColumnRef())) {
                return null;
            }
            if (columnRanges.equals(otherColumnRangePredicate.columnRanges)) {
                return ConstantOperator.TRUE;
            } else {
                if (other.enclose(this)) {
                    return toScalarOperator();
                }
                return null;
            }
        } else if (other instanceof AndRangePredicate) {
            return null;
        } else {
            OrRangePredicate orRangePredicate = (OrRangePredicate) other;
            for (RangePredicate rangePredicate : orRangePredicate.getChildPredicates()) {
                ScalarOperator simplied = simplify(rangePredicate);
                if (simplied != null) {
                    return toScalarOperator();
                }
            }
            return null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnRangePredicate that = (ColumnRangePredicate) o;
        return Objects.equals(columnRef, that.columnRef) && Objects.equals(columnRanges, that.columnRanges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnRef, columnRanges);
    }

    private boolean isEqualRange(Range<ConstantOperator> range) {
        if (!range.hasUpperBound() || !range.hasLowerBound()) {
            return false;
        }
        if (range.lowerBoundType() == BoundType.CLOSED
                && range.upperBoundType() == BoundType.CLOSED
                && range.upperEndpoint().equals(range.lowerEndpoint())) {
            // 8 <= a <= 8
            return true;
        } else if (range.lowerBoundType() == BoundType.CLOSED
                && range.upperBoundType() == BoundType.OPEN
                && range.lowerEndpoint().successor().isPresent()
                && range.upperEndpoint().equals(range.lowerEndpoint().successor().get())) {
            // 8 <= a < 9
            return true;
        } else if (range.lowerBoundType() == BoundType.OPEN
                && range.upperBoundType() == BoundType.CLOSED
                && range.upperEndpoint().predecessor().isPresent()
                && range.upperEndpoint().predecessor().get().equals(range.lowerEndpoint())) {
            // 7 < a <= 8
            return true;
        } else if (range.lowerBoundType() == BoundType.OPEN
                && range.upperBoundType() == BoundType.OPEN
                && range.upperEndpoint().predecessor().isPresent()
                && range.lowerEndpoint().successor().isPresent()) {
            // 7 < a < 9
            return range.upperEndpoint().predecessor().equals(range.lowerEndpoint().successor().get());
        }
        return false;
    }

    private Optional<ConstantOperator> getValue(Range<ConstantOperator> range) {
        if (range.lowerBoundType() == BoundType.CLOSED
                && range.upperBoundType() == BoundType.CLOSED
                && range.upperEndpoint().equals(range.lowerEndpoint())) {
            // 8 <= a <= 8
            return Optional.of(range.lowerEndpoint());
        } else if (range.lowerBoundType() == BoundType.CLOSED
                && range.upperBoundType() == BoundType.OPEN
                && range.lowerEndpoint().successor().isPresent()
                && range.upperEndpoint().equals(range.lowerEndpoint().successor().get())) {
            // 8 <= a < 9
            return Optional.of(range.lowerEndpoint());
        } else if (range.lowerBoundType() == BoundType.OPEN
                && range.upperBoundType() == BoundType.CLOSED
                && range.upperEndpoint().predecessor().isPresent()
                && range.upperEndpoint().predecessor().get().equals(range.lowerEndpoint())) {
            // 7 < a <= 8
            return Optional.of(range.upperEndpoint());
        } else if (range.lowerBoundType() == BoundType.OPEN
                && range.upperBoundType() == BoundType.OPEN
                && range.upperEndpoint().predecessor().isPresent()
                && range.lowerEndpoint().successor().isPresent()) {
            // 7 < a < 9
            return Optional.of(range.lowerEndpoint().successor().get());
        }
        return Optional.empty();
    }
}
