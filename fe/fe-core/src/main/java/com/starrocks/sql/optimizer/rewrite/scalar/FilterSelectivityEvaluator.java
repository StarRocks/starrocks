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

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.google.common.collect.Lists;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.ConstantOperatorUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator.CompoundType;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.statistics.BinaryPredicateStatisticCalculator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.stream.Collectors;

public class FilterSelectivityEvaluator {

    public static final double UNKNOWN_SELECT_RATIO = 20;

    public static final double NON_SELECTIVITY = 100;

    public static final int IN_CHILDREN_THRESHOLD = 1024;

    private int unionNumLimit;

    private ScalarOperator predicate;

    private Statistics statistics;

    private boolean isDecomposePhase;

    public FilterSelectivityEvaluator(ScalarOperator predicate, Statistics statistics, boolean isDecomposePhase) {
        this.predicate = predicate;
        this.statistics = statistics;
        this.isDecomposePhase = isDecomposePhase;
        unionNumLimit = ConnectContext.get().getSessionVariable().getScanOrToUnionLimit();
    }

    public List<ColumnFilter> evaluate() {
        List<ScalarOperator> elements;

        // special process for a large amount of values in in(xx, xx, xx...) clause
        if (isDecomposePhase && predicate instanceof InPredicateOperator) {
            return decomposeInPredicate((InPredicateOperator) predicate);
        }

        if (!isDecomposePhase) {
            elements = Utils.extractConjuncts(predicate);
        } else {
            elements = Utils.extractDisjunctive(predicate);
            if (elements.size() > unionNumLimit) {
                return Lists.newArrayList(new ColumnFilter(NON_SELECTIVITY, predicate));
            }
        }
        return elements.stream().map(this::evaluateScalarOperator).sorted().collect(Collectors.toList());
    }

    private ColumnFilter evaluateScalarOperator(ScalarOperator scalarOperator) {
        ColumnFilterEvaluator evaluator = new ColumnFilterEvaluator();
        return evaluator.evaluate(scalarOperator);
    }

    private List<ColumnFilter> decomposeInPredicate(InPredicateOperator predicate) {
        List<ColumnFilter> inFilters = Lists.newArrayList();
        Set<ScalarOperator> inSet = predicate.getChildren().stream().skip(1).collect(Collectors.toSet());
        ColumnRefOperator column = (ColumnRefOperator) predicate.getChild(0);
        int totalSize = inSet.size();
        int numSubsets = (int) Math.ceil((double) totalSize / IN_CHILDREN_THRESHOLD);
        List<List<ScalarOperator>> smallInSets = Lists.newArrayList();
        for (int i = 0; i < numSubsets; i++) {
            smallInSets.add(Lists.newArrayList(column));
        }

        int currentIndex = 0;
        for (ScalarOperator element : inSet) {
            int subsetIndex = currentIndex / IN_CHILDREN_THRESHOLD;
            smallInSets.get(subsetIndex).add(element);
            currentIndex++;
        }
        for (int i = 0; i < numSubsets; i++) {
            InPredicateOperator newInPredicate = new InPredicateOperator(false, smallInSets.get(i));
            inFilters.add(evaluateScalarOperator(newInPredicate));
        }

        return inFilters;
    }

    public class ColumnFilterEvaluator extends ScalarOperatorVisitor<ColumnFilter, Void> {

        public ColumnFilter evaluate(ScalarOperator scalarOperator) {
            return scalarOperator.accept(this, null);
        }

        @Override
        public ColumnFilter visit(ScalarOperator scalarOperator, Void context) {
            return new ColumnFilter(NON_SELECTIVITY, scalarOperator);
        }

        @Override
        public ColumnFilter visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
            ScalarOperator left = predicate.getChild(0);
            ScalarOperator right = predicate.getChild(1);
            List<ColumnRefOperator> usedCols = left.getColumnRefs();

            if (!isOnlyRefOneCol(usedCols)) {
                return new ColumnFilter(NON_SELECTIVITY, predicate);
            }

            ColumnRefOperator column = usedCols.get(0);
            ColumnStatistic columnStatistic = statistics.getColumnStatistic(column);
            double selectRatio;
            if (left.isColumnRef() && right.isConstantRef()) {
                selectRatio =
                        estimateColToConstSelectRatio(column, columnStatistic, predicate, (ConstantOperator) right);
            } else if (left.isColumnRef()) {
                selectRatio = estimateColumnToExprSelectRatio(columnStatistic, predicate);
            } else {
                // TODO need to process left child is an expr contains only one col?
                selectRatio = NON_SELECTIVITY;
            }
            return new ColumnFilter(selectRatio, column, predicate);
        }

        @Override
        public ColumnFilter visitInPredicate(InPredicateOperator predicate, Void context) {
            if (predicate.isNotIn()) {
                return new ColumnFilter(NON_SELECTIVITY, predicate);
            } else {
                Set<ScalarOperator> inSet = predicate.getChildren().stream().skip(1).collect(Collectors.toSet());
                List<ColumnRefOperator> usedCols = predicate.getChild(0).getColumnRefs();
                if (isOnlyRefOneCol(usedCols) && inSet.stream().allMatch(e -> e.isConstantRef() && !e.isNullable())) {
                    ColumnRefOperator column = usedCols.get(0);
                    ColumnStatistic columnStatistic = statistics.getColumnStatistic(column);
                    double selectRatio;
                    if (inSet.size() <= IN_CHILDREN_THRESHOLD) {
                        if (columnStatistic.isUnknown() || Double.isNaN(columnStatistic.getDistinctValuesCount())) {
                            selectRatio = StatisticsEstimateCoefficient.IN_PREDICATE_DEFAULT_FILTER_COEFFICIENT;
                        } else if (columnStatistic.hasNaNValue()) {
                            selectRatio = Math.min(1.0, inSet.size() / adjustNDV(columnStatistic.getDistinctValuesCount()));
                        } else {
                            Pair<Double, Double> valueRange = extractInSetValueRange(inSet);
                            boolean hasOverlap = Math.max(columnStatistic.getMinValue(), valueRange.first)
                                    <= Math.min(columnStatistic.getMaxValue(), valueRange.second);
                            selectRatio = hasOverlap ?
                                    Math.min(1.0, inSet.size() / adjustNDV(columnStatistic.getDistinctValuesCount())) : 0.0;
                        }
                    } else if (inSet.size() <= IN_CHILDREN_THRESHOLD * unionNumLimit) {
                        selectRatio = UNKNOWN_SELECT_RATIO;
                    } else {
                        selectRatio = NON_SELECTIVITY;
                    }
                    return new ColumnFilter(selectRatio, usedCols.get(0), predicate);
                } else {
                    return new ColumnFilter(NON_SELECTIVITY, predicate);
                }
            }
        }

        @Override
        public ColumnFilter visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
            CompoundType compoundType = predicate.getCompoundType();
            switch (compoundType) {
                case OR:
                    return new ColumnFilter(UNKNOWN_SELECT_RATIO, predicate);
                case AND:
                    ColumnFilter leftChild = predicate.getChild(0).accept(this, null);
                    ColumnFilter rightChild = predicate.getChild(1).accept(this, null);
                    // choose the child with smaller selectRation
                    return leftChild.compareTo(rightChild) < 0 ? new ColumnFilter(leftChild.selectRatio, predicate) :
                            new ColumnFilter(rightChild.selectRatio, predicate);

                default:
                    return new ColumnFilter(NON_SELECTIVITY, predicate);
            }
        }

        @Override
        public ColumnFilter visitIsNullPredicate(IsNullPredicateOperator predicate, Void context) {
            ScalarOperator child = predicate.getChild(0);
            boolean isNotNull = predicate.isNotNull();
            double selectRatio;
            if (!child.isColumnRef()) {
                selectRatio = NON_SELECTIVITY;
            } else {
                ColumnRefOperator column = (ColumnRefOperator) child;
                if (!statistics.getColumnStatistic(column).isUnknown()) {
                    ColumnStatistic columnStatistic = statistics.getColumnStatistic(column);
                    selectRatio = isNotNull ? 1 - columnStatistic.getNullsFraction() : columnStatistic.getNullsFraction();
                } else {
                    selectRatio = NON_SELECTIVITY;
                }
            }
            return new ColumnFilter(selectRatio, predicate);
        }

        private boolean isOnlyRefOneCol(List<ColumnRefOperator> usedCols) {
            if (usedCols.size() != 1) {
                return false;
            }

            ColumnRefOperator col = usedCols.get(0);

            // float, double is not supported as index and storage filter
            switch (col.getType().getPrimitiveType()) {
                case BOOLEAN:
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                case LARGEINT:
                case DATE:
                case DATETIME:
                case CHAR:
                case VARCHAR:
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                    return true;
                default:
                    return false;
            }
        }

        private Pair<Double, Double> extractInSetValueRange(Set<ScalarOperator> inSet) {
            List<Double> values = Lists.newArrayList();
            for (ScalarOperator operator : inSet) {
                ConstantOperator constant = (ConstantOperator) operator;
                if (constant.isNull()) {
                    continue;
                }
                OptionalDouble optionalDouble = ConstantOperatorUtils.doubleValueFromConstant(constant);
                if (optionalDouble.isPresent()) {
                    values.add(optionalDouble.getAsDouble());
                } else {
                    return Pair.create(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
                }
            }

            if (values.isEmpty()) {
                // return an empty range
                return Pair.create(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
            } else {
                values.sort(null);
                return Pair.create(values.get(0), values.get(values.size() - 1));
            }
        }

        private double estimateColToConstSelectRatio(ColumnRefOperator column, ColumnStatistic columnStatistic,
                                                     BinaryPredicateOperator predicate,
                                                     ConstantOperator constValue) {
            Statistics binaryStats =
                    BinaryPredicateStatisticCalculator.estimateColumnToConstantComparison(Optional.of(column),
                            columnStatistic, predicate, Optional.of(constValue), statistics);
            return Math.min(NON_SELECTIVITY, binaryStats.getOutputRowCount() / statistics.getOutputRowCount());
        }

        private double estimateColumnToExprSelectRatio(ColumnStatistic columnStatistic,
                                                       BinaryPredicateOperator predicate) {
            double selectRatio;
            switch (predicate.getBinaryType()) {
                case EQ:
                case EQ_FOR_NULL:
                    double distinctValues = columnStatistic.getDistinctValuesCount();
                    if (Double.isNaN(distinctValues)) {
                        selectRatio = NON_SELECTIVITY;
                    } else {
                        selectRatio = 1 / adjustNDV(distinctValues);
                    }
                    break;
                default:
                    selectRatio = NON_SELECTIVITY;
            }
            return selectRatio;
        }

        private double adjustNDV(double ndv) {
            return Math.min(ndv, statistics.getOutputRowCount());
        }
    }

    public static class ColumnFilter implements Comparable<ColumnFilter> {

        private Double selectRatio;

        // TODO add index info

        private Optional<ColumnRefOperator> column;

        private ScalarOperator filter;

        public ColumnFilter(double selectRatio, ScalarOperator filter) {
            this.selectRatio = selectRatio;
            this.column = Optional.empty();
            this.filter = filter;
        }

        public ColumnFilter(double selectRatio, ColumnRefOperator column, ScalarOperator filter) {
            this.selectRatio = selectRatio;
            this.column = Optional.of(column);
            this.filter = filter;
        }

        public Double getSelectRatio() {
            return selectRatio;
        }

        public ScalarOperator getFilter() {
            return filter;
        }

        public boolean isUnknownSelectRatio() {
            return selectRatio > 1 && selectRatio < NON_SELECTIVITY;
        }

        @Override
        public String toString() {
            return "ColumnFilter{" +
                    "selectRatio=" + selectRatio +
                    ", column=" + column +
                    ", filter=" + filter +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ColumnFilter that = (ColumnFilter) o;
            return Objects.equals(selectRatio, that.selectRatio) && Objects.equals(column, that.column) &&
                    Objects.equals(filter, that.filter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(selectRatio, column, filter);
        }

        @Override
        public int compareTo(@NotNull ColumnFilter o) {
            if (selectRatio < o.selectRatio) {
                return -1;
            } else if (selectRatio > o.selectRatio) {
                return 1;
            } else {
                return 0;
            }
        }
    }
}
