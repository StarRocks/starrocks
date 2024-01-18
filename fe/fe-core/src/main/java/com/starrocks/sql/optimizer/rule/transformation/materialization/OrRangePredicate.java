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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;
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

        ScalarOperator orPredicate = Utils.compoundOr(children);
        List<ScalarOperator> childPredicates = Utils.extractDisjunctive(orPredicate);
        Map<String, List<ScalarOperator>> columnPredicatesMap = Maps.newHashMap();
        for (ScalarOperator rangePredicate : childPredicates) {
            if (ScalarOperator.isColumnEqualConstant(rangePredicate)) {
                BinaryPredicateOperator binaryEqPredicate = (BinaryPredicateOperator) rangePredicate;
                ColumnRefOperator columnRef = binaryEqPredicate.getChild(0).cast();
                List<ScalarOperator> columnRangePredicates = columnPredicatesMap.computeIfAbsent(
                        columnRef.getName(), k -> Lists.newArrayList());
                columnRangePredicates.add(rangePredicate);
            } else if (rangePredicate instanceof InPredicateOperator && rangePredicate.getChild(0).isColumnRef()) {
                InPredicateOperator inPredicate = rangePredicate.cast();
                List<ScalarOperator> columnRangePredicates = columnPredicatesMap.computeIfAbsent(
                        ((ColumnRefOperator) inPredicate.getChild(0)).getName(), k -> Lists.newArrayList());
                columnRangePredicates.add(rangePredicate);
            }
        }
        for (List<ScalarOperator> value : columnPredicatesMap.values()) {
            if (value.size() > 1) {
                childPredicates.removeAll(value);
                // add InPredicateOperator
                List<ScalarOperator> arguments = Lists.newArrayList();
                arguments.add(value.get(0).getChild(0));
                for (ScalarOperator predicate : value) {
                    if (ScalarOperator.isColumnEqualConstant(predicate)) {
                        arguments.add(predicate.getChild(1));
                    } else {
                        // must be InPredicateOperator
                        Preconditions.checkState(predicate instanceof InPredicateOperator);
                        arguments.addAll(predicate.getChildren().subList(1, predicate.getChildren().size()));
                    }
                }
                InPredicateOperator inPredicateOperator = new InPredicateOperator(false, arguments);
                childPredicates.add(inPredicateOperator);
            }
        }

        return Utils.compoundOr(childPredicates);
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
