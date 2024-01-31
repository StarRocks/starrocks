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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class JoinPredicatePushdown {
    // is it on predicate or normal predicate
    private final boolean isOnPredicate;

    // store predicate in new LogicalFilterOperator or directly in child operator
    // if true, add the pushdown predicate directly into child operator,
    // or add the predicates into new LogicalFilterOperator
    private final boolean directToChild;

    private final ColumnRefFactory columnRefFactory;

    private OptExpression joinOptExpression;

    private List<ScalarOperator> leftPushDown;
    private List<ScalarOperator> rightPushDown;

    private final OptimizerContext optimizerContext;

    public JoinPredicatePushdown(
            OptExpression joinOptExpression, boolean isOnPredicate, boolean directToChild,
<<<<<<< HEAD
            ColumnRefFactory columnRefFactory) {
=======
            ColumnRefFactory columnRefFactory, boolean enableLeftRightOuterJoinEquivalenceDerive,
            OptimizerContext optimizerContext) {
>>>>>>> 37b8aa5a55 ([BugFix] fix left outer join to inner join bug and string not equal rewrite bug (#39331))
        this.joinOptExpression = joinOptExpression;
        this.isOnPredicate = isOnPredicate;
        this.directToChild = directToChild;
        this.columnRefFactory = columnRefFactory;
        this.leftPushDown = Lists.newArrayList();
        this.rightPushDown = Lists.newArrayList();
        this.optimizerContext = optimizerContext;
    }

    public OptExpression pushdown(ScalarOperator predicateToPush) {
        Preconditions.checkState(joinOptExpression.getOp() instanceof LogicalJoinOperator);
        ScalarOperator derivedPredicate = predicateDerive(predicateToPush);
        return pushDownPredicate(derivedPredicate);
    }

    private void getJoinPushdownOnPredicates(
            OptExpression joinOptExpression, List<ScalarOperator> conjunctList) {
        LogicalJoinOperator join = (LogicalJoinOperator) joinOptExpression.getOp();
        ColumnRefSet leftColumns = joinOptExpression.inputAt(0).getOutputColumns();
        ColumnRefSet rightColumns = joinOptExpression.inputAt(1).getOutputColumns();
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
    }

    private OptExpression pushDownPredicate(ScalarOperator predicateToPush) {
        if (isOnPredicate) {
            return pushdownOnPredicate(predicateToPush);
        } else {
            return pushdownFilterPredicate(predicateToPush);
        }
    }

    private OptExpression pushdownFilterPredicate(ScalarOperator predicateToPush) {
        LogicalJoinOperator join = joinOptExpression.getOp().cast();

        if (join.isInnerOrCrossJoin()) {
            return pushdownOnPredicate(predicateToPush);
        }

        // for outer or semi/anti join
        ColumnRefSet leftColumns = joinOptExpression.inputAt(0).getOutputColumns();
        ColumnRefSet rightColumns = joinOptExpression.inputAt(1).getOutputColumns();

        List<ScalarOperator> remainingFilter = new ArrayList<>();
        if (join.getJoinType().isLeftOuterJoin()) {
            for (ScalarOperator e : Utils.extractConjuncts(predicateToPush)) {
                ColumnRefSet usedColumns = e.getUsedColumns();
                if (leftColumns.containsAll(usedColumns)) {
                    leftPushDown.add(e);
                } else {
                    remainingFilter.add(e);
                }
            }
        } else if (join.getJoinType().isRightOuterJoin()) {
            for (ScalarOperator e : Utils.extractConjuncts(predicateToPush)) {
                ColumnRefSet usedColumns = e.getUsedColumns();
                if (rightColumns.containsAll(usedColumns)) {
                    rightPushDown.add(e);
                } else {
                    remainingFilter.add(e);
                }
            }
        } else if (join.getJoinType().isFullOuterJoin()) {
            for (ScalarOperator e : Utils.extractConjuncts(predicateToPush)) {
                ColumnRefSet usedColumns = e.getUsedColumns();
                if (usedColumns.isEmpty()) {
                    leftPushDown.add(e);
                    rightPushDown.add(e);
                } else {
                    remainingFilter.add(e);
                }
            }
        } else if (join.getJoinType().isLeftSemiAntiJoin()) {
            for (ScalarOperator e : Utils.extractConjuncts(predicateToPush)) {
                ColumnRefSet usedColumns = e.getUsedColumns();
                if (leftColumns.containsAll(usedColumns)) {
                    leftPushDown.add(e);
                } else {
                    remainingFilter.add(e);
                }
            }
        } else if (join.getJoinType().isRightSemiAntiJoin()) {
            for (ScalarOperator e : Utils.extractConjuncts(predicateToPush)) {
                ColumnRefSet usedColumns = e.getUsedColumns();
                if (rightColumns.containsAll(usedColumns)) {
                    rightPushDown.add(e);
                } else {
                    remainingFilter.add(e);
                }
            }
        }

        pushDownPredicate(joinOptExpression, leftPushDown, rightPushDown);

        LogicalJoinOperator newJoinOperator;
        if (!remainingFilter.isEmpty()) {
            if (join.getJoinType().isInnerJoin()) {
                newJoinOperator = new LogicalJoinOperator.Builder().withOperator(join)
                        .setOnPredicate(Utils.compoundAnd(join.getOnPredicate(), Utils.compoundAnd(remainingFilter)))
                        .build();
            } else {
                newJoinOperator = new LogicalJoinOperator.Builder().withOperator(join)
                        .setPredicate(Utils.compoundAnd(remainingFilter)).build();
            }
        } else {
            newJoinOperator = join;
        }

        return OptExpression.create(newJoinOperator, joinOptExpression.getInputs());
    }

    private OptExpression pushdownOnPredicate(ScalarOperator predicateToPush) {
        LogicalJoinOperator join = joinOptExpression.getOp().cast();

        List<ScalarOperator> conjunctList = Utils.extractConjuncts(predicateToPush);
        List<BinaryPredicateOperator> eqConjuncts = JoinHelper.getEqualsPredicate(
                joinOptExpression.inputAt(0).getOutputColumns(),
                joinOptExpression.inputAt(1).getOutputColumns(),
                conjunctList);
        conjunctList.removeAll(eqConjuncts);

        getJoinPushdownOnPredicates(joinOptExpression, conjunctList);

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
                .setOriginalOnPredicate(join.getOnPredicate())
                .build();
        root = OptExpression.create(newJoinOperator, joinOptExpression.getInputs());
        // for mv planner, do not derive the is not null predicate to make mv rewrite cover more cases
        if (!join.hasDeriveIsNotNullPredicate()) {
            deriveIsNotNullPredicate(eqConjuncts, root, leftPushDown, rightPushDown);
        }
        pushDownPredicate(root, leftPushDown, rightPushDown);
        return root;
    }

    private void pushDownPredicate(
            OptExpression root, List<ScalarOperator> leftPushDown, List<ScalarOperator> rightPushDown) {
        if (directToChild) {
            pushDownPredicateDirectly(root, leftPushDown, rightPushDown);
        } else {
            pushDownPredicateByFilter(root, leftPushDown, rightPushDown);
        }
    }

    public void pushDownPredicateDirectly(OptExpression root, List<ScalarOperator> leftPushDown,
                                                 List<ScalarOperator> rightPushDown) {
        if (CollectionUtils.isNotEmpty(leftPushDown)) {
            Set<ScalarOperator> set = Sets.newLinkedHashSet(leftPushDown);
            Operator leftOp = root.inputAt(0).getOp().cast();
            ScalarOperator newPredicate =
                    MvUtils.canonizePredicate(Utils.compoundAnd(Utils.compoundAnd(set), leftOp.getPredicate()));
            LogicalOperator.Builder builder = OperatorBuilderFactory.build(leftOp);
            builder.withOperator(leftOp);
            builder.setPredicate(newPredicate);
            Operator newOp = builder.build();
            OptExpression newLeft = OptExpression.create(newOp, root.inputAt(0).getInputs());
            root.getInputs().set(0, newLeft);
        }

        if (CollectionUtils.isNotEmpty(rightPushDown)) {
            Set<ScalarOperator> set = Sets.newLinkedHashSet(rightPushDown);
            Operator rightOp = root.inputAt(1).getOp();
            ScalarOperator newPredicate =
                    MvUtils.canonizePredicate(Utils.compoundAnd(Utils.compoundAnd(set), rightOp.getPredicate()));
            LogicalOperator.Builder builder = OperatorBuilderFactory.build(rightOp);
            builder.withOperator(rightOp);
            builder.setPredicate(newPredicate);
            Operator newOp = builder.build();
            OptExpression newRight = OptExpression.create(newOp, root.inputAt(1).getInputs());
            root.getInputs().set(1, newRight);
        }
    }

    private void pushDownPredicateByFilter(
            OptExpression root, List<ScalarOperator> leftPushDown, List<ScalarOperator> rightPushDown) {
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
    }

    private void deriveIsNotNullPredicate(
            List<BinaryPredicateOperator> onEQPredicates, OptExpression join,
            List<ScalarOperator> leftPushDown, List<ScalarOperator> rightPushDown) {
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
        boolean isLeftEmpty = leftPushDown.isEmpty();
        if (joinType.isInnerJoin() || joinType.isRightSemiJoin()) {
            leftEQ.stream().map(c -> new IsNullPredicateOperator(true, c.clone(), true)).forEach(notNull -> {
                optimizerContext.addPushdownNotNullPredicates(notNull);
                if (isLeftEmpty) {
                    leftPushDown.add(notNull);
                }
            });
        }
        boolean isRightEmpty = rightPushDown.isEmpty();
        if (joinType.isInnerJoin() || joinType.isLeftSemiJoin()) {
            rightEQ.stream().map(c -> new IsNullPredicateOperator(true, c.clone(), true)).forEach(notNull -> {
                optimizerContext.addPushdownNotNullPredicates(notNull);
                if (isRightEmpty) {
                    rightPushDown.add(notNull);
                }
            });
        }
        joinOp.setHasDeriveIsNotNullPredicate(true);
    }

    private JoinOperator deriveJoinType(JoinOperator originalType, ScalarOperator newJoinOnPredicate) {
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

    // derive the original predicates by range and equivalence optimization
    public ScalarOperator predicateDerive(ScalarOperator predicateToPush) {
        LogicalJoinOperator join = joinOptExpression.getOp().cast();
        if (isOnPredicate) {
            ScalarOperator newOnPredicate = rangePredicateDerive(predicateToPush);
            newOnPredicate = equivalenceDeriveOnPredicate(newOnPredicate, joinOptExpression, join);
            return newOnPredicate;
        } else {
            if (join.getJoinType().isOuterJoin()) {
                joinOptExpression = convertOuterToInner(joinOptExpression, predicateToPush);
                join = (LogicalJoinOperator) joinOptExpression.getOp();
            }
            if (join.isInnerOrCrossJoin()) {
                ScalarOperator predicate =
                        rangePredicateDerive(Utils.compoundAnd(join.getOnPredicate(), predicateToPush, join.getPredicate()));
                return equivalenceDerive(predicate, true);
            } else {
                ScalarOperator predicate = rangePredicateDerive(predicateToPush);
                getPushdownPredicatesFromEquivalenceDerive(
                        Utils.compoundAnd(join.getOnPredicate(), predicate), joinOptExpression, join);
                return predicate;
            }
        }
    }

    // get pushdown predicates from equivalence derivation
    private void getPushdownPredicatesFromEquivalenceDerive(
            ScalarOperator predicate, OptExpression joinOpt, LogicalJoinOperator join) {
        // For SQl: selectc * from t1 left join t2 on t1.id = t2.id where t1.id > 1
        // Infer t2.id > 1 and Push down it to right child
        if (!join.getJoinType().isSemiJoin() && !join.getJoinType().isOuterJoin()) {
            return;
        }

        ColumnRefSet leftOutputColumns = joinOpt.getInputs().get(0).getOutputColumns();
        ColumnRefSet rightOutputColumns = joinOpt.getInputs().get(1).getOutputColumns();

        Set<ColumnRefOperator> leftOutputColumnOps = columnRefFactory.getColumnRefs(leftOutputColumns);
        Set<ColumnRefOperator> rightOutputColumnOps = columnRefFactory.getColumnRefs(rightOutputColumns);

        ScalarOperator derivedPredicate = equivalenceDerive(predicate, false);
        List<ScalarOperator> derivedPredicates = Utils.extractConjuncts(derivedPredicate);

        if (join.getJoinType().isLeftSemiJoin()) {
            for (ScalarOperator p : derivedPredicates) {
                if (rightOutputColumns.containsAll(p.getUsedColumns())) {
                    p.setIsPushdown(true);
                    rightPushDown.add(p);
                }
            }
        } else if (join.getJoinType().isLeftOuterJoin()) {
            for (ScalarOperator p : derivedPredicates) {
                if (rightOutputColumns.containsAll(p.getUsedColumns()) &&
                        Utils.canEliminateNull(rightOutputColumnOps, p.clone())) {
                    p.setIsPushdown(true);
                    rightPushDown.add(p);
                }
            }
        } else if (join.getJoinType().isRightSemiJoin()) {
            for (ScalarOperator p : derivedPredicates) {
                if (leftOutputColumns.containsAll(p.getUsedColumns())) {
                    p.setIsPushdown(true);
                    leftPushDown.add(p);
                }
            }
        } else if (join.getJoinType().isRightOuterJoin()) {
            for (ScalarOperator p : derivedPredicates) {
                if (leftOutputColumns.containsAll(p.getUsedColumns()) &&
                        Utils.canEliminateNull(leftOutputColumnOps, p.clone())) {
                    p.setIsPushdown(true);
                    leftPushDown.add(p);
                }
            }
        }
    }

    public ScalarOperator rangePredicateDerive(ScalarOperator predicate) {
        ScalarRangePredicateExtractor scalarRangePredicateExtractor = new ScalarRangePredicateExtractor();
        return scalarRangePredicateExtractor.rewriteAll(Utils.compoundAnd(
                Utils.extractConjuncts(predicate).stream().map(scalarRangePredicateExtractor::rewriteAll)
                        .collect(Collectors.toList())));
    }

    ScalarOperator equivalenceDeriveOnPredicate(ScalarOperator on, OptExpression joinOpt, LogicalJoinOperator join) {
        // For SQl: select * from t1 left join t2 on t1.id = t2.id and t1.id > 1
        // Infer t2.id > 1 and Push down it to right child
        if (!join.getJoinType().isInnerJoin() && !join.getJoinType().isSemiJoin() &&
                !join.getJoinType().isOuterJoin()) {
            return on;
        }

        List<ScalarOperator> pushDown = Lists.newArrayList(on);
        ColumnRefSet leftOutputColumns = joinOpt.getInputs().get(0).getOutputColumns();
        ColumnRefSet rightOutputColumns = joinOpt.getInputs().get(1).getOutputColumns();

        ScalarOperator derivedPredicate = equivalenceDerive(on, false);
        List<ScalarOperator> derivedPredicates = Utils.extractConjuncts(derivedPredicate);

        if (join.getJoinType().isInnerJoin() || join.getJoinType().isSemiJoin()) {
            return Utils.compoundAnd(on, derivedPredicate);
        } else if (join.getJoinType().isLeftOuterJoin()) {
            for (ScalarOperator p : derivedPredicates) {
                if (rightOutputColumns.containsAll(p.getUsedColumns())) {
                    p.setIsPushdown(true);
                    pushDown.add(p);
                }
            }
        } else if (join.getJoinType().isRightOuterJoin()) {
            for (ScalarOperator p : derivedPredicates) {
                if (leftOutputColumns.containsAll(p.getUsedColumns())) {
                    p.setIsPushdown(true);
                    pushDown.add(p);
                }
            }
        }
        return Utils.compoundAnd(pushDown);
    }

    public ScalarOperator equivalenceDerive(ScalarOperator predicate, boolean returnInputPredicate) {
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

    private OptExpression convertOuterToInner(OptExpression joinOpt, ScalarOperator predicateToPush) {
        LogicalJoinOperator join = (LogicalJoinOperator) joinOpt.getOp();

        ColumnRefSet leftColumns = joinOpt.getInputs().get(0).getOutputColumns();
        ColumnRefSet rightColumns = joinOpt.getInputs().get(1).getOutputColumns();

        Set<ColumnRefOperator> leftOutputColumnOps = columnRefFactory.getColumnRefs(leftColumns);
        Set<ColumnRefOperator> rightOutputColumnOps = columnRefFactory.getColumnRefs(rightColumns);

        if (join.getJoinType().isLeftOuterJoin()) {
            if (Utils.canEliminateNull(rightOutputColumnOps, predicateToPush)
                    || hasPushdownNotNull(rightOutputColumnOps, optimizerContext.getPushdownNotNullPredicates())) {
                OptExpression newOpt = OptExpression.create(new LogicalJoinOperator.Builder().withOperator(join)
                                .setJoinType(JoinOperator.INNER_JOIN)
                                .build(),
                        joinOpt.getInputs());
                return newOpt;
            }
        } else if (join.getJoinType().isRightOuterJoin()) {
            if (Utils.canEliminateNull(leftOutputColumnOps, predicateToPush)
                    || hasPushdownNotNull(leftOutputColumnOps, optimizerContext.getPushdownNotNullPredicates())) {
                OptExpression newOpt = OptExpression.create(new LogicalJoinOperator.Builder().withOperator(join)
                                .setJoinType(JoinOperator.INNER_JOIN)
                                .build(),
                        joinOpt.getInputs());
                return newOpt;
            }
        } else if (join.getJoinType().isFullOuterJoin()) {
            boolean canConvertLeft = false;
            boolean canConvertRight = false;

            if (Utils.canEliminateNull(leftOutputColumnOps, predicateToPush)) {
                canConvertLeft = true;
            }
            if (Utils.canEliminateNull(rightOutputColumnOps, predicateToPush)) {
                canConvertRight = true;
            }

            if (canConvertLeft && canConvertRight) {
                return OptExpression.create(new LogicalJoinOperator.Builder().withOperator(join)
                                .setJoinType(JoinOperator.INNER_JOIN)
                                .build(),
                        joinOpt.getInputs());
            } else if (canConvertLeft) {
                return OptExpression.create(new LogicalJoinOperator.Builder().withOperator(join)
                                .setJoinType(JoinOperator.LEFT_OUTER_JOIN)
                                .build(),
                        joinOpt.getInputs());
            } else if (canConvertRight) {
                return OptExpression.create(new LogicalJoinOperator.Builder().withOperator(join)
                                .setJoinType(JoinOperator.RIGHT_OUTER_JOIN)
                                .build(),
                        joinOpt.getInputs());
            }
        }
        return joinOpt;
    }

    private boolean hasPushdownNotNull(Set<ColumnRefOperator> outputColumnOps, List<IsNullPredicateOperator> pushdownNotNulls) {
        return pushdownNotNulls.stream().anyMatch(p -> outputColumnOps.containsAll(p.getColumnRefs()));
    }
}
