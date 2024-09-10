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
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.property.DomainProperty;
import com.starrocks.sql.optimizer.property.DomainPropertyDeriver;
import com.starrocks.sql.optimizer.property.RangeExtractor;
import com.starrocks.sql.optimizer.property.ReplaceShuttle;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class OnPredicateMoveAroundRule extends TransformationRule {

    public static final OnPredicateMoveAroundRule INSTANCE = new OnPredicateMoveAroundRule(RuleType.TF_PREDICATE_PROPAGATE,
            Pattern.create(OperatorType.LOGICAL_JOIN).
                    addChildren(Pattern.create(OperatorType.PATTERN_LEAF), Pattern.create(OperatorType.PATTERN_LEAF)));

    private OnPredicateMoveAroundRule(RuleType type, Pattern pattern) {
        super(type, pattern);
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnablePredicateMoveAround()) {
            return false;
        }
        LogicalJoinOperator joinOperator = input.getOp().cast();
        if (joinOperator.getJoinType().isFullOuterJoin() || joinOperator.getJoinType().isCrossJoin()) {
            return false;
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOperator = input.getOp().cast();
        ScalarOperator onPredicate = joinOperator.getOnPredicate();

        OptExpression leftChild = input.inputAt(0);
        OptExpression rightChild = input.inputAt(1);

        List<BinaryPredicateOperator> binaryPredicates = extractBinaryPredicates(onPredicate,
                leftChild.getOutputColumns(), rightChild.getOutputColumns());
        if (binaryPredicates.isEmpty()) {
            return Lists.newArrayList();
        }

        DomainProperty leftDomainProperty = leftChild.getDomainProperty();
        DomainProperty rightDomainProperty = rightChild.getDomainProperty();

        OptExpression result = null;
        if (joinOperator.getJoinType().isInnerJoin() || joinOperator.getJoinType().isSemiJoin()) {
            List<ScalarOperator> toLeftPredicates = binaryPredicates.stream()
                    .map(e -> derivePredicate(e, rightDomainProperty, leftDomainProperty, true))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            ScalarOperator toLeftPredicate = Utils.compoundAnd(distinctPredicates(toLeftPredicates));

            List<ScalarOperator> toRightPredicates = binaryPredicates.stream()
                    .map(e -> derivePredicate(e, leftDomainProperty, rightDomainProperty, false))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            ScalarOperator toRightPredicate = Utils.compoundAnd(distinctPredicates(toRightPredicates));

            if (toLeftPredicate == null && toRightPredicate == null) {
                return Lists.newArrayList();
            } else if (toLeftPredicate == null) {
                LogicalFilterOperator filter = new LogicalFilterOperator(toRightPredicate);
                result = OptExpression.create(joinOperator,
                        Lists.newArrayList(input.inputAt(0), OptExpression.create(filter, input.inputAt(1)))
                );
            } else if (toRightPredicate == null) {
                LogicalFilterOperator filter = new LogicalFilterOperator(toLeftPredicate);
                result = OptExpression.create(joinOperator,
                        Lists.newArrayList(OptExpression.create(filter, input.inputAt(0)), input.inputAt(1))
                );
            } else {
                LogicalFilterOperator toLeftFilter = new LogicalFilterOperator(toLeftPredicate);
                LogicalFilterOperator toRightFilter = new LogicalFilterOperator(toRightPredicate);
                result = OptExpression.create(joinOperator,
                        Lists.newArrayList(OptExpression.create(toLeftFilter, input.inputAt(0)),
                                OptExpression.create(toRightFilter, input.inputAt(1)))
                );
            }
        } else if (joinOperator.getJoinType().isLeftOuterJoin()) {
            List<ScalarOperator> toRightPredicates = binaryPredicates.stream()
                    .map(e -> derivePredicate(e, leftDomainProperty, rightDomainProperty, false))
                    .collect(Collectors.toList());
            ScalarOperator toRightPredicate = Utils.compoundAnd(distinctPredicates(toRightPredicates));

            if (toRightPredicate != null) {
                LogicalFilterOperator filter = new LogicalFilterOperator(toRightPredicate);
                result = OptExpression.create(joinOperator,
                        Lists.newArrayList(input.inputAt(0), OptExpression.create(filter, input.inputAt(1)))
                );
            }
        } else if (joinOperator.getJoinType().isRightOuterJoin()) {
            List<ScalarOperator> toLeftPredicates = binaryPredicates.stream()
                    .map(e -> derivePredicate(e, rightDomainProperty, leftDomainProperty, true))
                    .collect(Collectors.toList());
            ScalarOperator toLeftPredicate = Utils.compoundAnd(distinctPredicates(toLeftPredicates));

            if (toLeftPredicate != null) {
                LogicalFilterOperator filter = new LogicalFilterOperator(toLeftPredicate);
                result = OptExpression.create(joinOperator,
                        Lists.newArrayList(OptExpression.create(filter, input.inputAt(0)), input.inputAt(1))
                );
            }
        }

        return result == null ? Lists.newArrayList() : Lists.newArrayList(result);
    }

    private List<BinaryPredicateOperator> extractBinaryPredicates(ScalarOperator predicate,
                                                                  ColumnRefSet leftCols, ColumnRefSet rightCols) {
        List<BinaryPredicateOperator> result = Lists.newArrayList();
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        for (ScalarOperator conjunct : conjuncts) {
            if (conjunct instanceof BinaryPredicateOperator) {
                BinaryPredicateOperator binaryPredicate = (BinaryPredicateOperator) conjunct;
                if (binaryPredicate.getBinaryType().isEqualOrRange()) {
                    ColumnRefSet leftUsedCols = binaryPredicate.getChild(0).getUsedColumns();
                    ColumnRefSet rightUsedCols = binaryPredicate.getChild(1).getUsedColumns();
                    if (leftUsedCols.isEmpty() || rightUsedCols.isEmpty()) {
                        // skip constant predicate
                        continue;
                    }
                    if (leftCols.containsAll(leftUsedCols) && rightCols.containsAll(rightUsedCols)) {
                        result.add(binaryPredicate);
                    } else if (leftCols.containsAll(rightUsedCols) && rightCols.containsAll(leftUsedCols)) {
                        result.add(binaryPredicate.commutative());
                    }
                }
            }
        }

        return result;
    }

    private ScalarOperator derivePredicate(BinaryPredicateOperator binaryPredicate, DomainProperty domainProperty,
                                           DomainProperty existDomainProperty, boolean toLeft) {
        int idx = toLeft ? 1 : 0;
        ScalarOperator seed = binaryPredicate.getChild(idx);
        ScalarOperator offspring = binaryPredicate.getChild(1 - idx);
        BinaryType binaryType = binaryPredicate.getBinaryType();
        ScalarOperator rewriteResult = null;
        if (binaryType.isEqual()) {
            if (domainProperty.contains(seed)) {
                ReplaceShuttle shuttle = new ReplaceShuttle(Map.of(seed, offspring));
                rewriteResult = shuttle.rewrite(domainProperty.getPredicateDesc(seed));
            }
        } else if (binaryType == BinaryType.LT || binaryType == BinaryType.LE) {
            if (domainProperty.contains(seed)) {
                RangeExtractor.RangeDescriptor desc = domainProperty.getValueWrapper(seed).getRangeDesc();
                rewriteResult = deriveLessPredicate(offspring, desc, toLeft);
            }
        } else if (binaryType == BinaryType.GT || binaryType == BinaryType.GE) {
            if (domainProperty.contains(seed)) {
                RangeExtractor.RangeDescriptor desc = domainProperty.getValueWrapper(seed).getRangeDesc();
                rewriteResult = deriveGreaterPredicate(offspring, desc, toLeft);
            }
        }
        if (rewriteResult == null) {
            return null;
        }

        rewriteResult.setIsPushdown(true);
        return removeRedundantPredicate(offspring, rewriteResult, existDomainProperty);
    }

    private List<ScalarOperator> distinctPredicates(List<ScalarOperator> predicates) {
        return new ArrayList<>(new LinkedHashSet<>(predicates));
    }

    private ScalarOperator removeRedundantPredicate(ScalarOperator offspring, ScalarOperator rewriteResult,
                                                    DomainProperty existDomainProperty) {
        if (!existDomainProperty.contains(offspring)) {
            return rewriteResult;
        }
        Set<ScalarOperator> set = Sets.newLinkedHashSet();
        set.addAll(Utils.extractConjuncts(existDomainProperty.getPredicateDesc(offspring)));
        rewriteResult = Utils.compoundAnd(Utils.extractConjuncts(rewriteResult).stream()
                .filter(e  -> !set.contains(e)).collect(Collectors.toList()));
        if (rewriteResult == null) {
            return null;
        }

        DomainPropertyDeriver deriver = new DomainPropertyDeriver();
        DomainProperty newDomainProperty = deriver.derive(rewriteResult);
        Range<ConstantOperator> existRange = existDomainProperty.getValueWrapper(offspring).getRangeDesc().getRange();
        Range<ConstantOperator> newRange = newDomainProperty.getValueWrapper(offspring).getRangeDesc().getRange();
        if (existRange == null) {
            return newRange == null ? null : rewriteResult;
        } else if (newRange == null || newRange.encloses(existRange)) {
            return null;
        } else {
            return rewriteResult;
        }
    }

    // join on predicate left_tbl_col < right_tbl_col
    // if we want to derive predicate to left, we need obtain the upper bound value of right_tbl_col
    // if we want to derive predicate to right, we need obtain the lower bound value of left_tbl_col
    private ScalarOperator deriveLessPredicate(ScalarOperator offspring,
                                               RangeExtractor.RangeDescriptor desc, boolean toLeft) {
        if (desc == null) {
            return null;
        }
        Range<ConstantOperator> range = desc.getRange();

        if (range == null) {
            return null;
        }

        if (toLeft && range.hasUpperBound()) {
            return new BinaryPredicateOperator(BinaryType.LE, offspring, range.upperEndpoint());
        } else if (!toLeft && range.hasLowerBound()) {
            return new BinaryPredicateOperator(BinaryType.GE, offspring, range.lowerEndpoint());
        }
        return null;
    }

    // join on predicate left_tbl_col > right_tbl_col
    // if we want to derive predicate to left, we need obtain the lower bound value of right_tbl_col
    // if we want to derive predicate to right, we need obtain the upper bound value of left_tbl_col
    private ScalarOperator deriveGreaterPredicate(ScalarOperator offspring,
                                                  RangeExtractor.RangeDescriptor desc, boolean toLeft) {
        if (desc == null) {
            return null;
        }
        Range<ConstantOperator> range = desc.getRange();

        if (range == null) {
            return null;
        }
        if (toLeft && range.hasLowerBound()) {
            return new BinaryPredicateOperator(BinaryType.GE, offspring, range.lowerEndpoint());
        } else if (!toLeft && range.hasUpperBound()) {
            return new BinaryPredicateOperator(BinaryType.LE, offspring, range.upperEndpoint());
        }
        return null;
    }

}
