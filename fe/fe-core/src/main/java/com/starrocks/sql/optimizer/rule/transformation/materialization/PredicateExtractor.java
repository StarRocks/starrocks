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
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions;

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
        if (left.isColumnRef() && right.isConstantRef()) {
            ConstantOperator constant = (ConstantOperator) right;
            TreeRangeSet<ConstantOperator> rangeSet = range(predicate.getBinaryType(), constant);
            if (rangeSet == null) {
                residualPredicates.add(predicate);
            } else {
                return new ColumnRangePredicate(left.cast(), rangeSet);
            }
        } else if (left.isConstantRef() && right.isColumnRef()) {
            ConstantOperator constant = (ConstantOperator) left;
            TreeRangeSet<ConstantOperator> rangeSet = range(predicate.getBinaryType(), constant);
            if (rangeSet == null) {
                residualPredicates.add(predicate);
            } else {
                return new ColumnRangePredicate(right.cast(), rangeSet);
            }
        } else if (left.isColumnRef() && right.isColumnRef() && context.isAnd()) {
            if (predicate.getBinaryType().isEqual()) {
                columnEqualityPredicates.add(predicate);
            } else {
                residualPredicates.add(predicate);
            }
        } else if (context.isAnd()) {
            if (checkDateTrunc(left, right)) {
                ConstantOperator constant = (ConstantOperator) right;
                TreeRangeSet<ConstantOperator> rangeSet = range(predicate.getBinaryType(), constant);
                if (rangeSet != null) {
                    return new ColumnRangePredicate(left.getChild(1).cast(), rangeSet);
                }
            } else if (checkDateTrunc(right, left)) {
                ConstantOperator constant = (ConstantOperator) left;
                TreeRangeSet<ConstantOperator> rangeSet = range(predicate.getBinaryType(), constant);
                if (rangeSet != null) {
                    return new ColumnRangePredicate(right.getChild(1).cast(), rangeSet);
                }
            }
            residualPredicates.add(predicate);
        }
        return null;
    }

    public static boolean checkDateTrunc(ScalarOperator op1, ScalarOperator op2) {
        if (op1 == null || op2 == null) {
            return false;
        }
        if (!(op1 instanceof CallOperator)) {
            return false;
        }
        if (!(op2 instanceof ConstantOperator)) {
            return false;
        }
        CallOperator func = (CallOperator) op1;
        ConstantOperator constantOperator = (ConstantOperator) op2;
        if (!func.getFnName().equalsIgnoreCase(FunctionSet.DATE_TRUNC)) {
            return false;
        }
        if (!(func.getChild(1) instanceof ColumnRefOperator)) {
            return false;
        }
        ConstantOperator sliced = ScalarOperatorFunctions.dateTrunc(
                ((ConstantOperator) op1.getChild(0)),
                constantOperator);
        return sliced.equals(constantOperator);
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

    private static TreeRangeSet<ConstantOperator> range(BinaryPredicateOperator.BinaryType type, ConstantOperator value) {
        TreeRangeSet<ConstantOperator> rangeSet = TreeRangeSet.create();
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
                Type valueType = value.getType();
                if (!valueType.isNumericType() && !valueType.isDateType()) {
                    return null;
                }
                rangeSet.add(Range.greaterThan(value));
                rangeSet.add(Range.lessThan(value));
                return rangeSet;
            default:
                throw new UnsupportedOperationException("unsupported type:" + type);
        }
    }
}
