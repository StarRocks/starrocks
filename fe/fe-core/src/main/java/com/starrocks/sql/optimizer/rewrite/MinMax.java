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

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;

import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class MinMax {
    private final Range<ConstantOperator> range;

    public static final MinMax EMPTY = new MinMax(Range.openClosed(ConstantOperator.FALSE, ConstantOperator.FALSE));
    public static final MinMax ALL = new MinMax();

    private MinMax(Range<ConstantOperator> range) {
        this.range = range;
    }

    private MinMax() {
        this(Range.all());
    }

    public static MinMax of(Range<ConstantOperator> range) {
        Optional<ConstantOperator> optMinValue =
                Optional.ofNullable(range.hasLowerBound() ? range.lowerEndpoint() : null);
        Optional<ConstantOperator> optMaxValue =
                Optional.ofNullable(range.hasUpperBound() ? range.upperEndpoint() : null);
        boolean minValueInclusive = range.hasLowerBound() && range.lowerBoundType().equals(BoundType.CLOSED);
        boolean maxValueInclusive = range.hasUpperBound() && range.upperBoundType().equals(BoundType.CLOSED);
        return of(optMinValue, minValueInclusive, optMaxValue, maxValueInclusive);
    }

    public static MinMax of(Optional<ConstantOperator> optMinValue, boolean minValueInclusive,
                            Optional<ConstantOperator> optMaxValue, boolean maxValueInclusive) {

        BoundType lbType = minValueInclusive ? BoundType.CLOSED : BoundType.OPEN;
        BoundType ubType = maxValueInclusive ? BoundType.CLOSED : BoundType.OPEN;
        if (!optMinValue.isPresent() && !optMaxValue.isPresent()) {
            return MinMax.ALL;
        } else if (!optMinValue.isPresent()) {
            return new MinMax(Range.upTo(optMaxValue.get(), ubType));
        } else if (!optMaxValue.isPresent()) {
            return new MinMax(Range.downTo(optMinValue.get(), lbType));
        }

        ConstantOperator minValue = optMinValue.get();
        ConstantOperator maxValue = optMaxValue.get();
        int r = minValue.compareTo(maxValue);
        if (r > 0) {
            return EMPTY;
        }
        if (r == 0 && (!minValueInclusive || !maxValueInclusive)) {
            return EMPTY;
        }
        Optional<ConstantOperator> optMinSucc = minValue.successor();
        if (optMinSucc.isPresent() && optMinSucc.get().equals(maxValue) && !minValueInclusive && !maxValueInclusive) {
            return EMPTY;
        }
        Optional<ConstantOperator> optMaxPred = maxValue.predecessor();
        if (optMaxPred.isPresent() && optMaxPred.get().equals(minValue) && !minValueInclusive && !maxValueInclusive) {
            return EMPTY;
        }
        Range<ConstantOperator> range = Range.range(minValue, lbType, maxValue, ubType);
        if (range.isEmpty()) {
            return EMPTY;
        } else {
            return new MinMax(range);
        }
    }

    public static MinMax union(MinMax lhs, MinMax rhs) {
        if (lhs == EMPTY) {
            return rhs;
        }

        if (rhs == EMPTY) {
            return lhs;
        }

        if (lhs == ALL || rhs == ALL) {

            return ALL;
        }
        return new MinMax(lhs.range.span(rhs.range));
    }

    public static MinMax intersection(MinMax lhs, MinMax rhs) {
        if (lhs == ALL) {
            return rhs;
        }

        if (rhs == ALL) {
            return lhs;
        }

        if (lhs == EMPTY || rhs == EMPTY) {
            return EMPTY;
        }

        if (lhs.range.isConnected(rhs.range)) {
            Range<ConstantOperator> newRange = lhs.range.intersection(rhs.range);
            return of(newRange);
        } else {
            return EMPTY;
        }
    }

    public Optional<ConstantOperator> getMin() {
        return Optional.ofNullable(range.hasLowerBound() ? range.lowerEndpoint() : null);
    }

    public Optional<ConstantOperator> getMax() {
        return Optional.ofNullable(range.hasUpperBound() ? range.upperEndpoint() : null);
    }

    public boolean isMinInclusive() {
        return range.hasLowerBound() && range.lowerBoundType().equals(BoundType.CLOSED);
    }

    public boolean isMaxInclusive() {
        return range.hasUpperBound() && range.upperBoundType().equals(BoundType.CLOSED);
    }

    public static Collector<MinMax, ?, MinMax> unionAll() {
        return Collectors.reducing(EMPTY, MinMax::union);
    }

    public static Collector<MinMax, ?, MinMax> intersectionAll() {
        return Collectors.reducing(ALL, MinMax::intersection);
    }
}
