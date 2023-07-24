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

import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.checkerframework.com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;

public class OrRangePredicate extends RangePredicate {
    public OrRangePredicate(List<RangePredicate> rangePredicates) {
        this.childPredicates = rangePredicates;
    }

    public List<RangePredicate> getChildPredicates() {
        return childPredicates;
    }

    @Override
    public boolean enclose(RangePredicate other) {
        if (other instanceof ColumnRangePredicate) {
            return childPredicates.stream().anyMatch(rangePredicate -> rangePredicate.enclose(other));
        } else if (other instanceof AndRangePredicate) {
            if (this.equals(other)) {
                return true;
            }
            // check any range predicate in OrRangePredicate enclose other
            return childPredicates.stream().anyMatch(rangePredicate -> rangePredicate.enclose(other));
        } else {
            // OrRangePredicate
            OrRangePredicate orRangePredicate = other.cast();
            // for every range predicate in other should be enclosed by this OrRangePredicate
            return orRangePredicate.getChildPredicates().stream().allMatch(otherRange -> this.enclose(otherRange));
        }
    }

    @Override
    public ScalarOperator toScalarOperator() {
        List<ScalarOperator> children = Lists.newArrayList();
        for (RangePredicate rangePredicate : childPredicates) {
            children.add(rangePredicate.toScalarOperator());
        }
        return Utils.compoundOr(children);
    }

    // for
    // src: (A = 10 and B = 20) or (C = 10 and D = 20)
    // target: A = 10 or c = 10
    // the simplied result is (A = 10 and B = 20) or (C = 10 and D = 20)
    // not: B = 20 or D = 20
    @Override
    public ScalarOperator simplify(RangePredicate other) {
        if (this.equals(other)) {
            return ConstantOperator.TRUE;
        }

        for (RangePredicate childRangePredicate : childPredicates) {
            ScalarOperator child = childRangePredicate.simplify(other);
            if (child == null) {
                return null;
            }
        }
        return toScalarOperator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OrRangePredicate that = (OrRangePredicate) o;
        return Objects.equals(childPredicates, that.childPredicates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(childPredicates);
    }
}
