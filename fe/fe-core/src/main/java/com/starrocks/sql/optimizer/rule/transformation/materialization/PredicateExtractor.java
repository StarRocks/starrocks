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

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import com.starrocks.analysis.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.List;
import java.util.Optional;

public class PredicateExtractor extends ScalarOperatorVisitor<RangePredicate, PredicateExtractor.PredicateExtractorContext> {
    private final List<ScalarOperator> columnEqualityPredicates = Lists.newArrayList();
    private final List<ScalarOperator> residualPredicates = Lists.newArrayList();

    public List<ScalarOperator> getColumnEqualityPredicates() {
        return columnEqualityPredicates;
    }

    public List<ScalarOperator> getResidualPredicates() {
        return residualPredicates;
    }

    public static class PredicateExtractorContext {
        private boolean isAnd = true;

        public boolean isAnd() {
            return isAnd;
        }

        public PredicateExtractorContext setAnd(boolean and) {
            isAnd = and;
            return this;
        }
    }

    @Override
    public RangePredicate visit(ScalarOperator scalarOperator, PredicateExtractorContext context) {
        return null;
    }

    @Override
    public RangePredicate visitBinaryPredicate(
            BinaryPredicateOperator predicate, PredicateExtractorContext context) {
        ScalarOperator left = predicate.getChild(0);
        ScalarOperator right = predicate.getChild(1);

        if (left.hasColumnRef() && right.isConstantRef()) {
            ConstantOperator constant = (ConstantOperator) right;
            TreeRangeSet<ConstantOperator> rangeSet = TreeRangeSet.create();
            rangeSet.addAll(range(predicate.getBinaryType(), constant));
            return new ColumnRangePredicate(left.cast(), rangeSet);
        } else if (left.isConstantRef() && right.hasColumnRef()) {
            ConstantOperator constant = (ConstantOperator) left;
            TreeRangeSet<ConstantOperator> rangeSet = TreeRangeSet.create();
            rangeSet.addAll(range(predicate.getBinaryType(), constant));
            return new ColumnRangePredicate(right.cast(), rangeSet);
        } else if (left.hasColumnRef() && right.hasColumnRef() && context.isAnd()) {
            if (predicate.getBinaryType().isEqual()) {
                columnEqualityPredicates.add(predicate);
            } else {
                residualPredicates.add(predicate);
            }
        } else if (context.isAnd()) {
            residualPredicates.add(predicate);
        }
        return null;
    }

    @Override
    public RangePredicate visitCompoundPredicate(
            CompoundPredicateOperator predicate, PredicateExtractorContext context) {

        if (predicate.isNot()) {
            // try to remove not
            ScalarOperator canonized = MvUtils.canonizePredicate(predicate);
            if (canonized == null || ((canonized instanceof CompoundPredicateOperator)
                    && ((CompoundPredicateOperator) canonized).isNot())) {
                return null;
            }
            return canonized.accept(this, context);
        }

        List<RangePredicate> rangePredicates = Lists.newArrayList();
        for (ScalarOperator child : predicate.getChildren()) {
            boolean isAndOrigin = context.isAnd();
            if (!predicate.isAnd()) {
                context.setAnd(false);
            }
            RangePredicate childRange = child.accept(this, context);
            context.setAnd(isAndOrigin);
            if (childRange == null) {
                if (!context.isAnd() || !predicate.isAnd()) {
                    return null;
                } else {
                    if (!(child instanceof BinaryPredicateOperator)) {
                        residualPredicates.add(child);
                    }
                    continue;
                }
            }
            if (predicate.isOr()) {
                if (childRange instanceof ColumnRangePredicate) {
                    ColumnRangePredicate childColumnRange = childRange.cast();
                    Optional<RangePredicate> rangePredicateOpt = findColumnRangePredicate(rangePredicates, childColumnRange);
                    if (rangePredicateOpt.isPresent()) {
                        ColumnRangePredicate matchedColumnRangePredicate = rangePredicateOpt.get().cast();
                        ColumnRangePredicate newPredicate =
                                matchedColumnRangePredicate.orRange(matchedColumnRangePredicate, childColumnRange);
                        rangePredicates.remove(matchedColumnRangePredicate);
                        if (!newPredicate.isUnbounded()) {
                            rangePredicates.add(newPredicate);
                        }
                    } else {
                        if (!((ColumnRangePredicate) childRange).isUnbounded()) {
                            rangePredicates.add(childRange);
                        }
                    }
                } else {
                    rangePredicates.add(childRange);
                }
            } else if (predicate.isAnd()) {
                if (childRange instanceof ColumnRangePredicate) {
                    ColumnRangePredicate childColumnRange = childRange.cast();
                    Optional<RangePredicate> rangePredicateOpt = findColumnRangePredicate(rangePredicates, childColumnRange);
                    if (rangePredicateOpt.isPresent()) {
                        ColumnRangePredicate newPredicate =
                                ColumnRangePredicate.andRange(rangePredicateOpt.get().cast(), childColumnRange);
                        rangePredicates.remove(rangePredicateOpt.get());
                        rangePredicates.add(newPredicate);
                    } else {
                        if (!childColumnRange.isUnbounded()) {
                            rangePredicates.add(childRange);
                        }
                    }
                } else if (childRange instanceof AndRangePredicate) {
                    for (RangePredicate subRangePredicate : childRange.getChildPredicates()) {
                        if (subRangePredicate instanceof ColumnRangePredicate) {
                            ColumnRangePredicate childColumnRange = subRangePredicate.cast();
                            Optional<RangePredicate> rangePredicateOpt =
                                    findColumnRangePredicate(rangePredicates, childColumnRange);
                            if (rangePredicateOpt.isPresent()) {
                                ColumnRangePredicate newPredicate =
                                        ColumnRangePredicate.andRange(rangePredicateOpt.get().cast(), childColumnRange);
                                rangePredicates.remove(rangePredicateOpt.get());
                                rangePredicates.add(newPredicate);
                            } else {
                                if (!childColumnRange.isUnbounded()) {
                                    rangePredicates.add(subRangePredicate);
                                }
                            }
                        } else {
                            rangePredicates.add(childRange);
                        }
                    }
                } else {
                    rangePredicates.add(childRange);
                }
            } else if (predicate.isNot()) {
                // it is normalized, can not be here
                return null;
            }
        }
        if (predicate.isAnd()) {
            if (rangePredicates.size() == 1 && (rangePredicates.get(0) instanceof ColumnRangePredicate)) {
                return rangePredicates.get(0);
            }
            return new AndRangePredicate(rangePredicates);
        } else {
            return new OrRangePredicate(rangePredicates);
        }
    }

    private Optional<RangePredicate> findColumnRangePredicate(
            List<RangePredicate> rangePredicates, ColumnRangePredicate toFind) {
        Optional<RangePredicate> rangePredicateOptional = rangePredicates.stream().filter(rangePredicate -> {
            if (!(rangePredicate instanceof ColumnRangePredicate)) {
                return false;
            }
            ColumnRangePredicate columnRangePredicate = rangePredicate.cast();
            return columnRangePredicate.getColumnRef().equals(toFind.getColumnRef());
        }).findFirst();
        return rangePredicateOptional;
    }

    private static  <C extends Comparable<C>> TreeRangeSet<C> range(BinaryType type, C value) {
        TreeRangeSet<C> rangeSet = TreeRangeSet.create();
        switch (type) {
            case EQ:
                rangeSet.add(Range.singleton(value));
                return rangeSet;
            case GE:
                rangeSet.add(Range.atLeast(value));
                return rangeSet;
            case GT:
                rangeSet.add(Range.greaterThan(value));
                return rangeSet;
            case LE:
                rangeSet.add(Range.atMost(value));
                return rangeSet;
            case LT:
                rangeSet.add(Range.lessThan(value));
                return rangeSet;
            case NE:
                rangeSet.add(Range.greaterThan(value));
                rangeSet.add(Range.lessThan(value));
                return rangeSet;
            default:
                throw new UnsupportedOperationException("unsupported type:" + type);
        }
    }
}
