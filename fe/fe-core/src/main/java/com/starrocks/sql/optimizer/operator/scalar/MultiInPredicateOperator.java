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

package com.starrocks.sql.optimizer.operator.scalar;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.operator.OperatorType.MULTI_IN;

/**
 * Represents an IN subquery with multiple columns e.g. WHERE (x, y) IN (SELECT a, b FROM T): (x, y) is a tuple that is
 * matched against tuple (a, b) from table T. This operator is never translated to an execution expression; instead, we
 * rewrite it as a semi-join or anti-join during logical transformations.
 */
public class MultiInPredicateOperator extends PredicateOperator {
    private final boolean isNotIn;
    // Size of the tuple (number of columns involved in the IN subquery).
    private final int tupleSize;

    public MultiInPredicateOperator(boolean isNotIn, List<ScalarOperator> leftArgs, List<ColumnRefOperator> rightArgs) {
        super(MULTI_IN);
        getChildren().addAll(leftArgs);
        getChildren().addAll(rightArgs);
        this.tupleSize = leftArgs.size();
        this.isNotIn = isNotIn;
    }

    public MultiInPredicateOperator(boolean isNotIn, List<ScalarOperator> arguments, int tupleSize) {
        super(MULTI_IN, arguments);
        this.isNotIn = isNotIn;
        this.tupleSize = tupleSize;
    }

    public boolean isNotIn() {
        return isNotIn;
    }

    public int getTupleSize() {
        return tupleSize;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(" (");
        sb.append(getChildren().stream().limit(tupleSize).map(ScalarOperator::toString).collect(Collectors.joining(", ")));
        sb.append(") ");
        if (isNotIn) {
            sb.append("NOT ");
        }

        sb.append("IN (");
        sb.append(getChildren().stream().skip(tupleSize).map(ScalarOperator::toString).collect(Collectors.joining(", ")));
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String debugString() {
        return toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        MultiInPredicateOperator that = (MultiInPredicateOperator) o;
        return isNotIn == that.isNotIn;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isNotIn);
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitMultiInPredicate(this, context);
    }
}
