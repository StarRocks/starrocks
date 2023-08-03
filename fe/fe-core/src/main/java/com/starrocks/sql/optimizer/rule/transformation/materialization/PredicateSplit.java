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
import com.google.common.collect.TreeRangeSet;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("UnstableApiUsage")
public class PredicateSplit {
    // column equality predicates conjuncts
    // a column equality predicate of the form (Ti.Cp =Tj.Cq)
    private final ScalarOperator equalPredicates;
    // range predicates conjuncts
    // a range predicate is any atomic predicate of the form (Ti.Cp op c)
    // where c is a constant and op is one of the operators “<”, “≤”, “=”, “≥”, “>”
    private final ScalarOperator rangePredicates;
    // residual predicates conjuncts
    // residual predicates containing all conjuncts except the two types above
    // eg: Ti.Cp like "%abc%"
    private final ScalarOperator residualPredicates;

    private PredicateSplit(ScalarOperator equalPredicates,
                           ScalarOperator rangePredicates,
                           ScalarOperator residualPredicates) {
        this.equalPredicates = equalPredicates;
        this.rangePredicates = rangePredicates;
        this.residualPredicates = residualPredicates;
    }

    public static PredicateSplit of(ScalarOperator equalPredicates,
                                    ScalarOperator rangePredicates,
                                    ScalarOperator residualPredicates) {
        return new PredicateSplit(equalPredicates, rangePredicates, residualPredicates);
    }

    public ScalarOperator getEqualPredicates() {
        return equalPredicates;
    }

    public ScalarOperator getRangePredicates() {
        return rangePredicates;
    }

    public ScalarOperator getResidualPredicates() {
        return residualPredicates;
    }

    public ScalarOperator toScalarOperator() {
        return Utils.compoundAnd(equalPredicates, rangePredicates, residualPredicates);
    }

    public List<ScalarOperator> getPredicates() {
        return Lists.newArrayList(equalPredicates, rangePredicates, residualPredicates);
    }

    // split predicate into three parts: equal columns predicates, range predicates, and residual predicates
    public static PredicateSplit splitPredicate(ScalarOperator predicate) {
        if (predicate == null) {
            return PredicateSplit.of(null, null, null);
        }
        List<ScalarOperator> rangePredicates = Lists.newArrayList();
        RangeExtractor extractor = new RangeExtractor();
        Map<ColumnRefOperator, TreeRangeSet<ConstantOperator>> rangeSet =
                predicate.accept(extractor, new RangeExtractorContext());
        if (rangeSet != null) {
            rangeSet.forEach((column, range) -> rangePredicates.add(rangeSetToExpr(range, column)));
        } else if (extractor.columnEqualityPredicates.isEmpty() && extractor.residualPredicates.isEmpty()) {
            extractor.residualPredicates.add(predicate);
        }
        return PredicateSplit.of(Utils.compoundAnd(extractor.columnEqualityPredicates), Utils.compoundAnd(rangePredicates),
            Utils.compoundAnd(extractor.residualPredicates));
    }

    public static class RangeExtractorContext {
        private boolean isAnd = true;

        public boolean isAnd() {
            return isAnd;
        }

        public RangeExtractorContext setAnd(boolean and) {
            isAnd = and;
            return this;
        }
    }

    public static class RangeExtractor
            extends ScalarOperatorVisitor<Map<ColumnRefOperator, TreeRangeSet<ConstantOperator>>, RangeExtractorContext> {

        List<ScalarOperator> columnEqualityPredicates = Lists.newArrayList();
        List<ScalarOperator> residualPredicates = Lists.newArrayList();

        @Override
        public Map<ColumnRefOperator, TreeRangeSet<ConstantOperator>> visit(
                ScalarOperator scalarOperator, RangeExtractorContext context) {
            return null;
        }

        @Override
        public Map<ColumnRefOperator, TreeRangeSet<ConstantOperator>> visitBinaryPredicate(
                BinaryPredicateOperator predicate, RangeExtractorContext context) {
            ScalarOperator left = predicate.getChild(0);
            ScalarOperator right = predicate.getChild(1);
            if (left.isColumnRef() && right.isConstantRef()) {
                ConstantOperator constant = (ConstantOperator) right;
                TreeRangeSet<ConstantOperator> rangeSet = TreeRangeSet.create();
                rangeSet.addAll(range(predicate.getBinaryType(), constant));
                return Collections.singletonMap(((ColumnRefOperator) left), rangeSet);
            } else if (left.isColumnRef() && right.isColumnRef() && context.isAnd()) {
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
        public Map<ColumnRefOperator, TreeRangeSet<ConstantOperator>> visitCompoundPredicate(
                CompoundPredicateOperator predicate, RangeExtractorContext context) {
            Map<ColumnRefOperator, TreeRangeSet<ConstantOperator>> columnRangeSet = new LinkedHashMap<>();
            for (ScalarOperator child : predicate.getChildren()) {
                boolean isAndOrigin = context.isAnd();
                if (!predicate.isAnd()) {
                    context.setAnd(false);
                }
                Map<ColumnRefOperator, TreeRangeSet<ConstantOperator>> childRange = child.accept(this, context);
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
                    // (a = 1 and b = 1) or (a = 2 and b = 2) can't convert to range
                    if (childRange.size() > 1) {
                        return null;
                    }
                    if (columnRangeSet.isEmpty()) {
                        columnRangeSet.putAll(childRange);
                    } else {
                        for (ColumnRefOperator column : childRange.keySet()) {
                            if (!columnRangeSet.containsKey(column)) {
                                return null;
                            } else {
                                TreeRangeSet<ConstantOperator> columnRange = columnRangeSet.get(column);
                                columnRange.addAll(childRange.get(column));
                            }
                        }
                    }
                } else if (predicate.isAnd()) {
                    childRange.forEach((column, rangeSet) -> {
                        if (!columnRangeSet.containsKey(column)) {
                            columnRangeSet.put(column, rangeSet);
                        } else {
                            TreeRangeSet<ConstantOperator> columnRange = columnRangeSet.get(column);
                            columnRangeSet.put(column, rangeSetIntersects(columnRange, rangeSet));
                        }
                    });
                } else if (predicate.isNot()) {
                    return null;
                }
            }
            return columnRangeSet;
        }
    }

    private static <C extends Comparable<C>> TreeRangeSet<C> range(BinaryPredicateOperator.BinaryType type, C value) {
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

    public static TreeRangeSet<ConstantOperator> rangeSetIntersects(TreeRangeSet<ConstantOperator> a,
                                                                    TreeRangeSet<ConstantOperator> b) {
        List<Range<ConstantOperator>> ranges = new ArrayList<>();
        for (Range<ConstantOperator> range : a.asRanges()) {
            if (b.intersects(range)) {
                for (Range<ConstantOperator> otherRange : b.asRanges()) {
                    if (range.isConnected(otherRange)) {
                        Range<ConstantOperator> intersection = range.intersection(otherRange);
                        if (!intersection.isEmpty()) {
                            ranges.add(intersection);
                        }
                    }
                }
            }
        }
        return TreeRangeSet.create(ranges);
    }

    public static ScalarOperator rangeSetToExpr(TreeRangeSet<ConstantOperator> rangeSet, ColumnRefOperator columnRef) {
        List<ScalarOperator> orOperators = Lists.newArrayList();
        for (Range<ConstantOperator> range : rangeSet.asRanges()) {
            List<ScalarOperator> andOperators = Lists.newArrayList();
            if (range.hasLowerBound() && range.hasUpperBound() && range.upperEndpoint().equals(range.lowerEndpoint())) {
                andOperators.add(BinaryPredicateOperator.eq(columnRef, range.upperEndpoint()));
            } else {
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
            }
            orOperators.add(Utils.compoundAnd(andOperators));
        }
        return Utils.compoundOr(orOperators);
    }
}
