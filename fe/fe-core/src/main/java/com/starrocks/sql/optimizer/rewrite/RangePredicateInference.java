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
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.Optional;
import java.util.function.Predicate;

public class RangePredicateInference extends ScalarOperatorVisitor<MinMax, Void> {
    public static final RangePredicateInference INSTANCE = new RangePredicateInference();

    private RangePredicateInference() {
    }

    @Override
    public MinMax visit(ScalarOperator scalarOperator, Void context) {
        return MinMax.ALL;
    }

    @Override
    public MinMax visitInPredicate(InPredicateOperator predicate, Void context) {
        if (!predicate.getChild(0).isColumnRef() ||
                predicate.getChildren().stream().skip(1).anyMatch(Predicate.not(ScalarOperator::isConstant))) {
            return MinMax.ALL;
        }

        Optional<ConstantOperator> maxValue =
                predicate.getChildren().stream().skip(1).map(child -> (ConstantOperator) child)
                        .max(ConstantOperator::compareTo);
        Optional<ConstantOperator> minValue =
                predicate.getChildren().stream().skip(1).map(child -> (ConstantOperator) child)
                        .min(ConstantOperator::compareTo);
        return MinMax.of(minValue, true, maxValue, true);
    }

    @Override
    public MinMax visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
        if (!predicate.getChild(0).isColumnRef() || !predicate.getChild(1).isConstant()) {
            return MinMax.ALL;
        }
        ConstantOperator value = (ConstantOperator) predicate.getChild(1);
        switch (predicate.getBinaryType()) {
            case EQ -> {
                return MinMax.of(Range.closed(value, value));
            }
            case NE, EQ_FOR_NULL -> {
                return MinMax.ALL;
            }
            case LE -> {
                return MinMax.of(Range.atMost(value));
            }
            case GE -> {
                return MinMax.of(Range.atLeast(value));
            }
            case LT -> {
                return MinMax.of(Range.upTo(value, BoundType.OPEN));
            }
            case GT -> {
                return MinMax.of(Range.downTo(value, BoundType.OPEN));
            }
        }
        return MinMax.ALL;
    }

    @Override
    public MinMax visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
        if (predicate.isAnd()) {
            return Utils.extractConjuncts(predicate).stream().map(conjunct -> visit(conjunct, null))
                    .collect(MinMax.unionAll());
        } else if (predicate.isOr()) {
            return Utils.extractDisjunctive(predicate).stream().map(disjunct -> visit(disjunct, null))
                    .collect(MinMax.intersectionAll());
        } else {
            return MinMax.ALL;
        }
    }
}
