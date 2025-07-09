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

import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

public class RangePredicateInference extends ScalarOperatorVisitor<MinMax, Void> {
    public static final RangePredicateInference INSTANCE = new RangePredicateInference();

    private RangePredicateInference() {
    }

    private static final Map<String, Integer> DEVIATION_TABLE = ImmutableMap.<String, Integer>builder()
            .put("microsecond", 1)
            .put("millisecond", 1)
            .put("second", 1)
            .put("minute", 60)
            .put("hour", 3600)
            .put("day", 3600 * 24)
            .put("week", 3600 * 24 * 7)
            .put("month", 3600 * 24 * 31)
            .put("quarter", 3600 * 24 * 31 * 3)
            .put("year", 3600 * 24 * 366)
            .build();

    private static Optional<MinMax> getDeviation(ScalarOperator expr) {
        if (expr.isColumnRef()) {
            return Optional.of(
                    MinMax.of(Range.closed(ConstantOperator.createInt(0), ConstantOperator.createInt(0))));
        }
        if (!(expr instanceof CallOperator)) {
            return Optional.empty();
        }
        CallOperator call = expr.cast();
        if (call.getFnName().equalsIgnoreCase(FunctionSet.DATE_TRUNC)) {
            if (!call.getChild(0).isConstant() || !call.getChild(1).isColumnRef()) {
                return Optional.empty();
            }
            String timeUnit = ((ConstantOperator) call.getChild(0)).getVarchar().toLowerCase();
            return Optional.ofNullable(DEVIATION_TABLE.get(timeUnit))
                    .map(dev -> MinMax.of(
                            Range.closed(ConstantOperator.createInt(0), ConstantOperator.createInt(dev))));
        } else if (call.getFnName().equalsIgnoreCase(FunctionSet.TIME_SLICE)) {
            if (!call.getChild(0).isColumnRef() || !call.getChildren().stream().skip(1).allMatch(
                    ScalarOperator::isConstant)) {
                return Optional.empty();
            }
            int interval = ((ConstantOperator) call.getChild(1)).getInt();
            String timeUnit = ((ConstantOperator) call.getChild(2)).getVarchar();
            String boundary = ((ConstantOperator) call.getChild(3)).getVarchar();
            Optional<ConstantOperator> optDev = Optional.ofNullable(DEVIATION_TABLE.get(timeUnit))
                    .map(d -> ConstantOperator.createInt(d * interval));
            if (optDev.isEmpty()) {
                return Optional.empty();
            }
            ConstantOperator dev = optDev.get();
            if (boundary.equalsIgnoreCase("floor")) {
                return Optional.of(MinMax.of(Range.closed(ConstantOperator.createInt(0), dev)));
            } else if (boundary.equalsIgnoreCase("ceil")) {
                dev = ConstantOperator.createInt(-dev.getInt());
                return Optional.of(MinMax.of(Range.closed(dev, ConstantOperator.createInt(0))));
            } else {
                return Optional.empty();
            }
        }

        return Optional.empty();
    }

    @Override
    public MinMax visit(ScalarOperator scalarOperator, Void context) {
        return MinMax.ALL;
    }

    @Override
    public MinMax visitInPredicate(InPredicateOperator predicate, Void context) {
        Optional<MinMax> optDev = getDeviation(predicate.getChild(0));
        if (optDev.isEmpty() ||
                predicate.getChildren().stream().skip(1).anyMatch(Predicate.not(ScalarOperator::isConstant))) {
            return MinMax.ALL;
        }

        MinMax dev = optDev.get();
        Preconditions.checkState(dev.getMax().isPresent() && dev.getMin().isPresent());
        Optional<ConstantOperator> maxValue =
                predicate.getChildren().stream().skip(1).map(child -> (ConstantOperator) child)
                        .max(ConstantOperator::compareTo);
        Optional<ConstantOperator> minValue =
                predicate.getChildren().stream().skip(1).map(child -> (ConstantOperator) child)
                        .min(ConstantOperator::compareTo);
        maxValue = maxValue.map(v -> ScalarOperatorFunctions.secondsAdd(v, dev.getMax().get()));
        minValue = minValue.map(v -> ScalarOperatorFunctions.secondsAdd(v, dev.getMin().get()));
        return MinMax.of(minValue, true, maxValue, true);
    }

    @Override
    public MinMax visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
        Optional<MinMax> optDev = getDeviation(predicate.getChild(0));
        if (optDev.isEmpty() || !predicate.getChild(1).isConstant()) {
            return MinMax.ALL;
        }
        MinMax dev = optDev.get();
        Preconditions.checkState(dev.getMax().isPresent() && dev.getMin().isPresent());
        ConstantOperator value = (ConstantOperator) predicate.getChild(1);
        switch (predicate.getBinaryType()) {
            case EQ -> {
                ConstantOperator lower = ScalarOperatorFunctions.secondsAdd(value, dev.getMin().get());
                ConstantOperator upper = ScalarOperatorFunctions.secondsAdd(value, dev.getMax().get());
                return MinMax.of(Range.closed(lower, upper));
            }
            case NE, EQ_FOR_NULL -> {
                return MinMax.ALL;
            }
            case LE -> {
                ConstantOperator upper = ScalarOperatorFunctions.secondsAdd(value, dev.getMax().get());
                return MinMax.of(Range.atMost(upper));
            }
            case GE -> {
                ConstantOperator lower = ScalarOperatorFunctions.secondsAdd(value, dev.getMin().get());
                return MinMax.of(Range.atLeast(lower));
            }
            case LT -> {
                ConstantOperator upper = ScalarOperatorFunctions.secondsAdd(value, dev.getMax().get());
                return MinMax.of(Range.upTo(upper, BoundType.OPEN));
            }
            case GT -> {
                ConstantOperator lower = ScalarOperatorFunctions.secondsAdd(value, dev.getMin().get());
                return MinMax.of(Range.downTo(lower, BoundType.OPEN));
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
