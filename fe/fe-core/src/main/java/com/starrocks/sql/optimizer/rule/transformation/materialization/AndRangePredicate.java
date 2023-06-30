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

public class AndRangePredicate extends RangePredicate {
    private List<RangePredicate> rangePredicates;

    public AndRangePredicate(List<RangePredicate> rangePredicates) {
        this.rangePredicates = rangePredicates;
    }

    public List<RangePredicate> getRangePredicates() {
        return rangePredicates;
    }

    @Override
    public boolean enclose(RangePredicate other) {
        if (other instanceof ColumnRangePredicate) {
            return rangePredicates.size() == 1 && rangePredicates.get(0).enclose(other);
        } else if (other instanceof AndRangePredicate) {
            if (this.equals(other)) {
                return true;
            }
            AndRangePredicate otherAnd = other.cast();
            if (otherAnd.getRangePredicates().size() < rangePredicates.size()) {
                return false;
            }
            // for every RangePredicate in this should enclose one range predicate in other
            return rangePredicates.stream().allMatch(thisPredicate -> {
                return otherAnd.getRangePredicates().stream().anyMatch(otherPredicate -> thisPredicate.enclose(otherPredicate));
            });
        } else {
            // OrRangePredicate
            OrRangePredicate orRangePredicate = other.cast();
            return orRangePredicate.getRangePredicates().stream().anyMatch(oneRange -> oneRange.enclose(this));
        }
    }

    @Override
    public ScalarOperator toScalarOperator() {
        List<ScalarOperator> children = Lists.newArrayList();
        for (RangePredicate rangePredicate : rangePredicates) {
            children.add(rangePredicate.toScalarOperator());
        }
        return Utils.compoundAnd(children);
    }

    @Override
    public ScalarOperator simplify(RangePredicate other) {
        if (this.equals(other)) {
            return ConstantOperator.TRUE;
        }
        List<ScalarOperator> simpliedPredicates = Lists.newArrayList();
        if (other instanceof ColumnRangePredicate) {
            boolean matched = false;
            for (RangePredicate childRangePredicate : rangePredicates) {
                ScalarOperator child = childRangePredicate.simplify(other);
                if (child != null) {
                    matched = true;
                    if (child.equals(ConstantOperator.TRUE)) {
                        continue;
                    }
                    simpliedPredicates.add(child);
                } else {
                    simpliedPredicates.add(childRangePredicate.toScalarOperator());
                }
            }
            if (!matched) {
                return null;
            }
        } else if (other instanceof AndRangePredicate) {
            List<RangePredicate> otherMatchedPredicates = Lists.newArrayList();
            AndRangePredicate otherRangePredicate = (AndRangePredicate) other;
            for (RangePredicate rangePredicate : rangePredicates) {
                boolean matched = false;
                for (RangePredicate otherChildRangePredicate : otherRangePredicate.rangePredicates) {
                    ScalarOperator simplied = rangePredicate.simplify(otherChildRangePredicate);
                    if (simplied != null) {
                        otherMatchedPredicates.add(otherChildRangePredicate);
                        simpliedPredicates.add(simplied);
                        matched = true;
                        break;
                    }
                }
                if (!matched) {
                    // not matched in target, just add it to result
                    simpliedPredicates.add(rangePredicate.toScalarOperator());
                }
            }
            if (otherRangePredicate.rangePredicates.stream().anyMatch(
                    predicate -> !otherMatchedPredicates.contains(predicate))) {
                // some predicates not matched in src, means can not be simplied
                return null;
            }
        } else {
            OrRangePredicate orRangePredicate = (OrRangePredicate) other;
            for (RangePredicate rangePredicate : orRangePredicate.getRangePredicates()) {
                ScalarOperator simplied = simplify(rangePredicate);
                if (simplied != null) {
                    simpliedPredicates.add(toScalarOperator());
                    break;
                }
            }
        }
        return Utils.compoundAnd(simpliedPredicates);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AndRangePredicate that = (AndRangePredicate) o;
        return Objects.equals(rangePredicates, that.rangePredicates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rangePredicates);
    }
}
