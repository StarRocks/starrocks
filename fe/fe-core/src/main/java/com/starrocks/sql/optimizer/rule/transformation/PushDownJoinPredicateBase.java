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
import com.google.common.collect.Sets;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarEquivalenceExtractor;
import com.starrocks.sql.optimizer.rewrite.ScalarRangePredicateExtractor;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class PushDownJoinPredicateBase extends TransformationRule {
    protected PushDownJoinPredicateBase(RuleType type, Pattern pattern) {
        super(type, pattern);
    }

    public static OptExpression pushDownPredicate(OptExpression root, List<ScalarOperator> leftPushDown,
                                                  List<ScalarOperator> rightPushDown) {
        if (CollectionUtils.isNotEmpty(leftPushDown)) {
            Set<ScalarOperator> set = Sets.newLinkedHashSet(leftPushDown);
            OptExpression newLeft = new OptExpression(new LogicalFilterOperator(Utils.compoundAnd(set)));
            newLeft.getInputs().add(root.getInputs().get(0));
            root.getInputs().set(0, newLeft);
        }

        if (CollectionUtils.isNotEmpty(rightPushDown)) {
            Set<ScalarOperator> set = Sets.newLinkedHashSet(rightPushDown);
            OptExpression newRight = new OptExpression(new LogicalFilterOperator(Utils.compoundAnd(set)));
            newRight.getInputs().add(root.getInputs().get(1));
            root.getInputs().set(1, newRight);
        }

        return root;
    }

    public static OptExpression pushDownOnPredicate(OptExpression input, ScalarOperator conjunct) {
        LogicalJoinOperator join = (LogicalJoinOperator) input.getOp();

        List<ScalarOperator> conjunctList = Utils.extractConjuncts(conjunct);
        List<BinaryPredicateOperator> eqConjuncts = JoinHelper.getEqualsPredicate(
                input.getInputs().get(0).getOutputColumns(),
                input.getInputs().get(1).getOutputColumns(),
                conjunctList);
        conjunctList.removeAll(eqConjuncts);

        List<ScalarOperator> leftPushDown = Lists.newArrayList();
        List<ScalarOperator> rightPushDown = Lists.newArrayList();
        ColumnRefSet leftColumns = input.getInputs().get(0).getOutputColumns();
        ColumnRefSet rightColumns = input.getInputs().get(1).getOutputColumns();
        if (join.isInnerOrCrossJoin()) {
            for (ScalarOperator predicate : conjunctList) {
                ColumnRefSet usedColumns = predicate.getUsedColumns();
                if (usedColumns.isEmpty()) {
                    leftPushDown.add(predicate);
                    rightPushDown.add(predicate);
                } else if (leftColumns.containsAll(usedColumns)) {
                    leftPushDown.add(predicate);
                } else if (rightColumns.containsAll(usedColumns)) {
                    rightPushDown.add(predicate);
                }
            }
        } else if (join.getJoinType().isOuterJoin()) {
            for (ScalarOperator predicate : conjunctList) {
                ColumnRefSet usedColumns = predicate.getUsedColumns();
                if (leftColumns.containsAll(usedColumns) && join.getJoinType().isRightOuterJoin()) {
                    leftPushDown.add(predicate);
                } else if (rightColumns.containsAll(usedColumns) && join.getJoinType().isLeftOuterJoin()) {
                    rightPushDown.add(predicate);
                }
            }
        } else if (join.getJoinType().isSemiAntiJoin()) {
            for (ScalarOperator predicate : conjunctList) {
                ColumnRefSet usedColumns = predicate.getUsedColumns();
                if (leftColumns.containsAll(usedColumns)) {
                    if (join.getJoinType().isLeftAntiJoin()) {
                        continue;
                    }
                    leftPushDown.add(predicate);
                } else if (rightColumns.containsAll(usedColumns)) {
                    if (join.getJoinType().isRightAntiJoin()) {
                        continue;
                    }
                    rightPushDown.add(predicate);
                }
            }
        }


        // predicate can be removed from the original on condition should meet at least one rule below:
        // 1. join type is not semi/anti
        // 2. eqConjuncts is not empty
        if (!join.getJoinType().isSemiAntiJoin() || CollectionUtils.isNotEmpty(eqConjuncts)) {
            conjunctList.removeAll(leftPushDown);
            conjunctList.removeAll(rightPushDown);
        }

        ScalarOperator joinEqPredicate = Utils.compoundAnd(Lists.newArrayList(eqConjuncts));
        ScalarOperator postJoinPredicate = Utils.compoundAnd(conjunctList);
        ScalarOperator newJoinOnPredicate = Utils.compoundAnd(joinEqPredicate, postJoinPredicate);

        OptExpression root;
        JoinOperator newJoinType = deriveJoinType(join.getJoinType(), newJoinOnPredicate);
        LogicalJoinOperator newJoinOperator = new LogicalJoinOperator.Builder().withOperator(join)
                .setJoinType(newJoinType)
                .setOnPredicate(newJoinOnPredicate)
                .build();
        root = OptExpression.create(newJoinOperator, input.getInputs());
        if (!join.hasDeriveIsNotNullPredicate()) {
            deriveIsNotNullPredicate(eqConjuncts, root, leftPushDown, rightPushDown);
        }
        return pushDownPredicate(root, leftPushDown, rightPushDown);
    }

    public static ScalarOperator equivalenceDerive(ScalarOperator predicate, boolean returnInputPredicate) {
        ScalarEquivalenceExtractor scalarEquivalenceExtractor = new ScalarEquivalenceExtractor();

        Set<ColumnRefOperator> allColumnRefs = Sets.newLinkedHashSet();
        allColumnRefs.addAll(Utils.extractColumnRef(predicate));

        Set<ScalarOperator> allPredicate = Sets.newLinkedHashSet();
        List<ScalarOperator> inputPredicates = Utils.extractConjuncts(predicate);
        allPredicate.addAll(inputPredicates);

        scalarEquivalenceExtractor.union(inputPredicates);

        for (ColumnRefOperator ref : allColumnRefs) {
            for (ScalarOperator so : scalarEquivalenceExtractor.getEquivalentScalar(ref)) {
                // only use one column predicate
                if (Utils.countColumnRef(so) > 1) {
                    continue;
                }

                if (OperatorType.BINARY.equals(so.getOpType())) {
                    BinaryPredicateOperator bpo = (BinaryPredicateOperator) so;

                    // avoid repeat predicate, like a = b, b = a
                    if (!allPredicate.contains(bpo) && !allPredicate.contains(bpo.commutative())) {
                        allPredicate.add(bpo);
                    }
                    continue;
                }

                allPredicate.add(so);
            }
        }

        if (!returnInputPredicate) {
            inputPredicates.forEach(allPredicate::remove);
        }
        return Utils.compoundAnd(Lists.newArrayList(allPredicate));
    }

    public static ScalarOperator rangePredicateDerive(ScalarOperator predicate) {
        ScalarRangePredicateExtractor scalarRangePredicateExtractor = new ScalarRangePredicateExtractor();
        return scalarRangePredicateExtractor.rewriteAll(Utils.compoundAnd(
                Utils.extractConjuncts(predicate).stream().map(scalarRangePredicateExtractor::rewriteAll)
                        .collect(Collectors.toList())));
    }

    private static void deriveIsNotNullPredicate(List<BinaryPredicateOperator> onEQPredicates, OptExpression join,
                                                 List<ScalarOperator> leftPushDown,
                                                 List<ScalarOperator> rightPushDown) {
        List<ScalarOperator> leftEQ = Lists.newArrayList();
        List<ScalarOperator> rightEQ = Lists.newArrayList();

        for (BinaryPredicateOperator bpo : onEQPredicates) {
            if (!bpo.getBinaryType().isEqual()) {
                continue;
            }

            if (join.getChildOutputColumns(0).containsAll(bpo.getChild(0).getUsedColumns())) {
                leftEQ.add(bpo.getChild(0));
                rightEQ.add(bpo.getChild(1));
            } else if (join.getChildOutputColumns(0).containsAll(bpo.getChild(1).getUsedColumns())) {
                leftEQ.add(bpo.getChild(1));
                rightEQ.add(bpo.getChild(0));
            }
        }

        LogicalJoinOperator joinOp = ((LogicalJoinOperator) join.getOp());
        JoinOperator joinType = joinOp.getJoinType();
        if ((joinType.isInnerJoin() || joinType.isRightSemiJoin()) && leftPushDown.isEmpty()) {
            leftEQ.stream().map(c -> new IsNullPredicateOperator(true, c.clone())).forEach(leftPushDown::add);
        }
        if ((joinType.isInnerJoin() || joinType.isLeftSemiJoin()) && rightPushDown.isEmpty()) {
            rightEQ.stream().map(c -> new IsNullPredicateOperator(true, c.clone())).forEach(rightPushDown::add);
        }
        joinOp.setHasDeriveIsNotNullPredicate(true);
    }

    private static JoinOperator deriveJoinType(JoinOperator originalType, ScalarOperator newJoinOnPredicate) {
        JoinOperator result;
        switch (originalType) {
            case INNER_JOIN:
            case CROSS_JOIN:
                result = newJoinOnPredicate == null ? JoinOperator.CROSS_JOIN : JoinOperator.INNER_JOIN;
                break;
            default:
                result = originalType;
        }
        return result;
    }
}
