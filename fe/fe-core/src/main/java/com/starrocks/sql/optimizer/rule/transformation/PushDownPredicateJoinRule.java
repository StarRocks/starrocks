// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.rule.transformation.JoinPredicateUtils.pushDownOnPredicate;
import static com.starrocks.sql.optimizer.rule.transformation.JoinPredicateUtils.pushDownPredicate;
import static java.util.function.Function.identity;

public class PushDownPredicateJoinRule extends TransformationRule {
    public PushDownPredicateJoinRule() {
        super(RuleType.TF_PUSH_DOWN_PREDICATE_JOIN, Pattern.create(OperatorType.LOGICAL_FILTER)
                .addChildren(Pattern.create(OperatorType.LOGICAL_JOIN)
                        .addChildren(Pattern.create(OperatorType.PATTERN_LEAF))
                        .addChildren(Pattern.create(OperatorType.PATTERN_LEAF))));
    }

    private OptExpression pushDownOuterOrSemiJoin(OptExpression input, ScalarOperator predicate,
                                                  List<ScalarOperator> leftPushDown,
                                                  List<ScalarOperator> rightPushDown) {
        OptExpression joinOpt = input.getInputs().get(0);
        LogicalJoinOperator join = (LogicalJoinOperator) input.getInputs().get(0).getOp();

        ColumnRefSet leftColumns = joinOpt.getInputs().get(0).getOutputColumns();
        ColumnRefSet rightColumns = joinOpt.getInputs().get(1).getOutputColumns();

        List<ScalarOperator> remainingFilter = new ArrayList<>();

        if (join.getJoinType().isLeftOuterJoin()) {
            for (ScalarOperator e : Utils.extractConjuncts(predicate)) {
                ColumnRefSet usedColumns = e.getUsedColumns();
                if (leftColumns.contains(usedColumns)) {
                    leftPushDown.add(e);
                } else {
                    remainingFilter.add(e);
                }
            }
        } else if (join.getJoinType().isRightOuterJoin()) {
            for (ScalarOperator e : Utils.extractConjuncts(predicate)) {
                ColumnRefSet usedColumns = e.getUsedColumns();
                if (rightColumns.contains(usedColumns)) {
                    rightPushDown.add(e);
                } else {
                    remainingFilter.add(e);
                }
            }
        } else if (join.getJoinType().isFullOuterJoin()) {
            for (ScalarOperator e : Utils.extractConjuncts(predicate)) {
                ColumnRefSet usedColumns = e.getUsedColumns();
                if (usedColumns.isEmpty()) {
                    leftPushDown.add(e);
                    rightPushDown.add(e);
                } else {
                    remainingFilter.add(e);
                }
            }
        } else if (join.getJoinType().isLeftSemiAntiJoin()) {
            for (ScalarOperator e : Utils.extractConjuncts(predicate)) {
                ColumnRefSet usedColumns = e.getUsedColumns();
                if (leftColumns.contains(usedColumns)) {
                    leftPushDown.add(e);
                } else {
                    remainingFilter.add(e);
                }
            }
        } else if (join.getJoinType().isRightSemiAntiJoin()) {
            for (ScalarOperator e : Utils.extractConjuncts(predicate)) {
                ColumnRefSet usedColumns = e.getUsedColumns();
                if (rightColumns.contains(usedColumns)) {
                    rightPushDown.add(e);
                } else {
                    remainingFilter.add(e);
                }
            }
        }

        joinOpt = pushDownPredicate(joinOpt, Utils.compoundAnd(leftPushDown), Utils.compoundAnd(rightPushDown));

        if (!remainingFilter.isEmpty()) {
            if (join.getJoinType().isInnerJoin()) {
                join.setOnPredicate(
                        Utils.compoundAnd(join.getOnPredicate(), Utils.compoundAnd(remainingFilter)));
            } else {
                join.setPredicate(Utils.compoundAnd(remainingFilter));
            }
        }
        return joinOpt;
    }

    private void convertOuterToInner(OptExpression input) {
        LogicalFilterOperator filter = (LogicalFilterOperator) input.getOp();
        OptExpression joinOpt = input.getInputs().get(0);
        LogicalJoinOperator join = (LogicalJoinOperator) input.getInputs().get(0).getOp();

        ColumnRefSet leftColumns = joinOpt.getInputs().get(0).getOutputColumns();
        ColumnRefSet rightColumns = joinOpt.getInputs().get(1).getOutputColumns();

        if (join.getJoinType().isLeftOuterJoin()) {
            if (canEliminateNull(rightColumns, filter.getPredicate().clone())) {
                join.setJoinType(JoinOperator.INNER_JOIN);
            }
        } else if (join.getJoinType().isRightOuterJoin()) {
            if (canEliminateNull(leftColumns, filter.getPredicate().clone())) {
                join.setJoinType(JoinOperator.INNER_JOIN);
            }
        } else if (join.getJoinType().isFullOuterJoin()) {
            boolean canConvertLeft = false;
            boolean canConvertRight = false;

            if (canEliminateNull(leftColumns, filter.getPredicate().clone())) {
                canConvertLeft = true;
            }
            if (canEliminateNull(rightColumns, filter.getPredicate().clone())) {
                canConvertRight = true;
            }

            if (canConvertLeft && canConvertRight) {
                join.setJoinType(JoinOperator.INNER_JOIN);
            } else if (canConvertLeft) {
                join.setJoinType(JoinOperator.LEFT_OUTER_JOIN);
            } else if (canConvertRight) {
                join.setJoinType(JoinOperator.RIGHT_OUTER_JOIN);
            }
        }
    }

    /**
     * 1: replace the column of nullColumns in  expression to NULL literal
     * 2: Call the ScalarOperatorRewriter function to perform constant folding
     * 3: If the result of constant folding is NULL or false,
     * it proves that the expression can filter the NULL value in nullColumns
     * 4: Return true to prove that NULL values can be eliminated, and vice versa
     */
    public boolean canEliminateNull(ColumnRefSet nullColumns, ScalarOperator expression) {
        Map<ColumnRefOperator, ScalarOperator> m = Arrays.stream(nullColumns.getColumnIds()).boxed()
                .map(id -> new ColumnRefOperator(id, Type.INVALID, "", true))
                .collect(Collectors.toMap(identity(), col -> ConstantOperator.createNull(col.getType())));

        for (ScalarOperator e : Utils.extractConjuncts(expression)) {
            ScalarOperator nullEval = new ReplaceColumnRefRewriter(m).visit(e, null);

            ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
            //The calculation of the null value is in the constant fold
            nullEval = scalarRewriter.rewrite(nullEval, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
            if (nullEval.isConstantRef() && ((ConstantOperator) nullEval).isNull()) {
                return true;
            } else if (nullEval.equals(ConstantOperator.createBoolean(false))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator filter = (LogicalFilterOperator) input.getOp();
        OptExpression joinOpt = input.getInputs().get(0);
        LogicalJoinOperator join = (LogicalJoinOperator) joinOpt.getOp();

        if (join.getJoinType().isCrossJoin() || join.getJoinType().isInnerJoin()) {
            // The effect will be better, first do the range derive, and then do the equivalence derive
            ScalarOperator predicate = JoinPredicateUtils
                    .rangePredicateDerive(Utils.compoundAnd(join.getOnPredicate(), filter.getPredicate()));
            predicate = JoinPredicateUtils.equivalenceDerive(predicate, true);
            return Lists.newArrayList(pushDownOnPredicate(input.getInputs().get(0), predicate));
        } else {
            if (join.getJoinType().isOuterJoin()) {
                convertOuterToInner(input);
            }

            if (join.getJoinType().isCrossJoin() || join.getJoinType().isInnerJoin()) {
                ScalarOperator predicate = JoinPredicateUtils
                        .rangePredicateDerive(Utils.compoundAnd(join.getOnPredicate(), filter.getPredicate()));
                predicate = JoinPredicateUtils.equivalenceDerive(predicate, true);
                return Lists.newArrayList(pushDownOnPredicate(input.getInputs().get(0), predicate));
            } else {
                ScalarOperator predicate = JoinPredicateUtils.rangePredicateDerive(filter.getPredicate());
                List<ScalarOperator> leftPushDown = Lists.newArrayList();
                List<ScalarOperator> rightPushDown = Lists.newArrayList();
                equivalenceDeriveOnOuterOrSemi(Utils.compoundAnd(join.getOnPredicate(), predicate), joinOpt, join,
                        leftPushDown, rightPushDown);
                return Lists.newArrayList(pushDownOuterOrSemiJoin(input, predicate, leftPushDown, rightPushDown));
            }
        }
    }

    void equivalenceDeriveOnOuterOrSemi(ScalarOperator predicate, OptExpression joinOpt, LogicalJoinOperator join,
                                        List<ScalarOperator> leftPushDown, List<ScalarOperator> rightPushDown) {
        // For SQl: select * from t1 left join t2 on t1.id = t2.id where t1.id > 1
        // Infer t2.id > 1 and Push down it to right child
        if (!join.getJoinType().isSemiJoin() && !join.getJoinType().isOuterJoin()) {
            return;
        }

        ColumnRefSet leftOutputColumns = joinOpt.getInputs().get(0).getOutputColumns();
        ColumnRefSet rightOutputColumns = joinOpt.getInputs().get(1).getOutputColumns();

        ScalarOperator derivedPredicate = JoinPredicateUtils.equivalenceDerive(predicate, false);
        List<ScalarOperator> derivedPredicates = Utils.extractConjuncts(derivedPredicate);

        if (join.getJoinType().isLeftOuterJoin() || join.getJoinType().isLeftSemiJoin()) {
            for (ScalarOperator p : derivedPredicates) {
                if (rightOutputColumns.contains(derivedPredicate.getUsedColumns())) {
                    rightPushDown.add(p);
                }
            }
        } else if (join.getJoinType().isRightOuterJoin() || join.getJoinType().isRightSemiJoin()) {
            for (ScalarOperator p : derivedPredicates) {
                if (leftOutputColumns.contains(derivedPredicate.getUsedColumns())) {
                    leftPushDown.add(p);
                }
            }
        }
    }
}
