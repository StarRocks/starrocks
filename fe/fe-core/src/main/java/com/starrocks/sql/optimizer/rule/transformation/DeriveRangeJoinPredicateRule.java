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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DeriveRangeJoinPredicateRule extends TransformationRule {

    private static final Logger LOG = LogManager.getLogger(DeriveRangeJoinPredicateRule.class);

    public DeriveRangeJoinPredicateRule() {
        super(RuleType.TF_DERIVE_RANGE_JOIN_PREDICATE,
                Pattern.create(OperatorType.LOGICAL_JOIN, OperatorType.PATTERN_LEAF, OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        return context.getSessionVariable().enableCboDeriveRangeJoinPredicate();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator join = input.getOp().cast();
        List<ScalarOperator> onPredicate = Utils.extractConjuncts(join.getOnPredicate());

        Map<ColumnRefSet, List<BinaryPredicateOperator>> columnToRange = Maps.newHashMap();

        ColumnRefSet leftChildColumns = input.inputAt(0).getOutputColumns();
        ColumnRefSet rightChildColumns = input.inputAt(1).getOutputColumns();

        for (ScalarOperator p : onPredicate) {
            if (!(p instanceof BinaryPredicateOperator)) {
                continue;
            }
            BinaryPredicateOperator binaryPredicate = (BinaryPredicateOperator) p;
            if (!binaryPredicate.getBinaryType().isRange()) {
                continue;
            }

            ColumnRefSet left = binaryPredicate.getChild(0).getUsedColumns();
            ColumnRefSet right = binaryPredicate.getChild(1).getUsedColumns();

            if (left.isEmpty() || right.isEmpty()) {
                continue;
            }

            // normalized to left OP right
            if (leftChildColumns.containsAll(left) && rightChildColumns.containsAll(right)) {
                columnToRange.computeIfAbsent(left, k -> Lists.newArrayList()).add(binaryPredicate);
                columnToRange.computeIfAbsent(right, k -> Lists.newArrayList()).add(binaryPredicate);
            } else if (rightChildColumns.containsAll(left) && leftChildColumns.containsAll(right)) {
                columnToRange.computeIfAbsent(left, k -> Lists.newArrayList()).add(binaryPredicate.commutative());
                columnToRange.computeIfAbsent(right, k -> Lists.newArrayList()).add(binaryPredicate.commutative());
            }
        }

        // filter range predicate: [lower < refs < upper] and refs must one column
        columnToRange.entrySet().removeIf(entry -> {
            List<BinaryPredicateOperator> predicates = entry.getValue();
            ColumnRefSet key = entry.getKey();
            if (predicates.stream().map(BinaryPredicateOperator::getBinaryType)
                    .map(t -> BinaryType.GE.equals(t) || BinaryType.GT.equals(t)).distinct().count() < 2) {
                return true;
            }

            // must one column
            return key.cardinality() != 1;
        });

        if (columnToRange.isEmpty()) {
            return Lists.newArrayList(input);
        }

        // generate range predicate
        LogicalOperator leftOp = input.inputAt(0).getOp().cast();
        LogicalOperator rightOp = input.inputAt(1).getOp().cast();
        Statistics leftStatics = getStatistics(input.inputAt(0), context);
        Statistics rightStatics = getStatistics(input.inputAt(1), context);
        if (leftStatics == null || rightStatics == null) {
            return Lists.newArrayList();
        }

        List<ScalarOperator> leftPredicates = Lists.newArrayList();
        List<ScalarOperator> rightPredicates = Lists.newArrayList();
        for (ColumnRefSet refs : columnToRange.keySet()) {
            Optional<ColumnRefOperator> optional =
                    refs.getStream().map(context.getColumnRefFactory()::getColumnRef).findFirst();
            if (!optional.isPresent()) {
                continue;
            }

            ColumnStatistic columnStatistic;
            ColumnRefOperator anchor = optional.get();
            if (leftStatics.getColumnStatistic(anchor) != null) {
                columnStatistic = leftStatics.getColumnStatistic(anchor);
            } else if (rightStatics.getColumnStatistic(anchor) != null) {
                columnStatistic = rightStatics.getColumnStatistic(anchor);
            } else {
                continue;
            }
            if (StringUtils.isEmpty(columnStatistic.getMinString()) ||
                    StringUtils.isEmpty(columnStatistic.getMaxString())) {
                LOG.debug("column minString value: {}, maxString value: {}",
                        columnStatistic.getMinString(),
                        columnStatistic.getMaxValue());
                continue;
            }

            ScalarOperator lower = new ConstantOperator(columnStatistic.getMinString(), Type.STRING);
            ScalarOperator upper = new ConstantOperator(columnStatistic.getMaxString(), Type.STRING);

            if (!anchor.getType().isStringType()) {
                lower = new CastOperator(anchor.getType(), lower);
                upper = new CastOperator(anchor.getType(), upper);
            }

            List<BinaryPredicateOperator> predicates = columnToRange.get(refs);
            // must be range predicate
            for (BinaryPredicateOperator binary : predicates) {
                boolean isLeft = binary.getChild(0).getUsedColumns().containsAll(refs);
                BinaryType type = binary.getBinaryType();
                if (BinaryType.GE.equals(type) || BinaryType.GT.equals(type)) {
                    // A > l > B
                    if (isLeft) {
                        // l > B -> l-lower > B
                        rightPredicates.add(new BinaryPredicateOperator(BinaryType.LE, binary.getChild(1), upper));
                    } else {
                        // A > l -> A > l-upper
                        leftPredicates.add(new BinaryPredicateOperator(BinaryType.GE, binary.getChild(0), lower));
                    }
                } else if (BinaryType.LE.equals(type) || BinaryType.LT.equals(type)) {
                    // A < l < B
                    if (isLeft) {
                        // l < B -> l-lower < B
                        rightPredicates.add(new BinaryPredicateOperator(BinaryType.GE, binary.getChild(1), lower));
                    } else {
                        // A < l -> A < l-upper
                        leftPredicates.add(new BinaryPredicateOperator(BinaryType.LE, binary.getChild(0), upper));
                    }
                }
            }
        }

        if (leftPredicates.isEmpty() && rightPredicates.isEmpty()) {
            return Lists.newArrayList(input);
        }

        leftPredicates.add(leftOp.getPredicate());
        rightPredicates.add(rightOp.getPredicate());

        if (join.getJoinType().isInnerJoin()) {
            Operator ls = OperatorBuilderFactory.build(leftOp).withOperator(leftOp)
                    .setPredicate(Utils.compoundAnd(leftPredicates)).build();
            Operator rs = OperatorBuilderFactory.build(rightOp).withOperator(rightOp)
                    .setPredicate(Utils.compoundAnd(rightPredicates)).build();

            return Lists.newArrayList(OptExpression.create(join,
                    OptExpression.create(ls, input.inputAt(0).getInputs()),
                    OptExpression.create(rs, input.inputAt(1).getInputs())));

        } else if (join.getJoinType().isLeftOuterJoin()) {
            Operator rs = OperatorBuilderFactory.build(rightOp).withOperator(rightOp)
                    .setPredicate(Utils.compoundAnd(rightPredicates)).build();

            return Lists.newArrayList(OptExpression.create(join,
                    input.inputAt(0),
                    OptExpression.create(rs, input.inputAt(1).getInputs())));

        } else if (join.getJoinType().isRightOuterJoin()) {
            Operator ls = OperatorBuilderFactory.build(leftOp).withOperator(leftOp)
                    .setPredicate(Utils.compoundAnd(leftPredicates)).build();

            return Lists.newArrayList(OptExpression.create(join,
                    OptExpression.create(ls, input.inputAt(0).getInputs()),
                    input.inputAt(1)));
        }

        return null;
    }

    private Statistics getStatistics(OptExpression input, OptimizerContext context) {
        deriveStatistics(input, context);
        return input.getStatistics();
    }


    private void deriveStatistics(OptExpression optExpression, OptimizerContext context) {
        if (optExpression.getStatistics() != null) {
            return;
        }

        for (OptExpression child : optExpression.getInputs()) {
            deriveStatistics(child, context);
        }
        ExpressionContext ec = new ExpressionContext(optExpression);
        StatisticsCalculator sc = new StatisticsCalculator(ec, context.getColumnRefFactory(),
                context.getTaskContext().getOptimizerContext());
        sc.estimatorStats();
        optExpression.setStatistics(ec.getStatistics());
    }
}
