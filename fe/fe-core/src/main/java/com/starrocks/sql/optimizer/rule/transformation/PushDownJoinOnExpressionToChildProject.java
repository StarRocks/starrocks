// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Because the children of Join need to shuffle data
 * according to the equivalence conditions in onPredicate, if there is an expression on predicate,
 * we need to push the expression down to the project of the child
 */
public class PushDownJoinOnExpressionToChildProject extends TransformationRule {
    public PushDownJoinOnExpressionToChildProject() {
        super(RuleType.TF_PUSH_DOWN_JOIN_ON_EXPRESSION_TO_CHILD_PROJECT, Pattern.create(OperatorType.LOGICAL_JOIN).
                addChildren(Pattern.create(OperatorType.PATTERN_LEAF), Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) input.getOp();
        ScalarOperator onPredicate = joinOperator.getOnPredicate();

        ColumnRefSet leftOutputColumns = input.inputAt(0).getOutputColumns();
        ColumnRefSet rightOutputColumns = input.inputAt(1).getOutputColumns();

        List<BinaryPredicateOperator> equalConjs = JoinHelper.
                getEqualsPredicate(leftOutputColumns, rightOutputColumns, Utils.extractConjuncts(onPredicate));

        return !equalConjs.isEmpty();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) input.getOp();
        ScalarOperator onPredicate = joinOperator.getOnPredicate();

        ColumnRefSet leftOutputColumns = input.inputAt(0).getOutputColumns();
        ColumnRefSet rightOutputColumns = input.inputAt(1).getOutputColumns();

        List<BinaryPredicateOperator> equalsPredicate = JoinHelper.
                getEqualsPredicate(leftOutputColumns, rightOutputColumns, Utils.extractConjuncts(onPredicate));

        Map<ColumnRefOperator, ScalarOperator> leftProjectMaps = new HashMap<>();
        Map<ColumnRefOperator, ScalarOperator> rightProjectMaps = new HashMap<>();
        for (BinaryPredicateOperator binaryPredicateOperator : equalsPredicate) {
            ScalarOperator left = binaryPredicateOperator.getChild(0);
            ScalarOperator right = binaryPredicateOperator.getChild(1);
            if (leftOutputColumns.containsAll(left.getUsedColumns()) && !left.isColumnRef()) {
                leftProjectMaps.put(context.getColumnRefFactory().create(left, left.getType(), left.isNullable()),
                        left);
            }
            if (rightOutputColumns.containsAll(left.getUsedColumns()) && !left.isColumnRef()) {
                rightProjectMaps.put(context.getColumnRefFactory().create(left, left.getType(), left.isNullable()),
                        left);
            }
            if (rightOutputColumns.containsAll(right.getUsedColumns()) && !right.isColumnRef()) {
                rightProjectMaps.put(context.getColumnRefFactory().create(right, right.getType(), right.isNullable()),
                        right);
            }
            if (leftOutputColumns.containsAll(right.getUsedColumns()) && !right.isColumnRef()) {
                leftProjectMaps.put(context.getColumnRefFactory().create(right, right.getType(), right.isNullable()),
                        right);
            }
        }

        Rewriter leftRewriter = new Rewriter(leftProjectMaps);
        Rewriter rightRewriter = new Rewriter(rightProjectMaps);
        ScalarOperator newJoinOnPredicate = onPredicate.clone().accept(leftRewriter, null).accept(rightRewriter, null);

        OptExpression newJoinOpt = OptExpression.create(new LogicalJoinOperator.Builder().withOperator(joinOperator)
                .setOnPredicate(newJoinOnPredicate)
                .build(), input.getInputs());

        if (!leftProjectMaps.isEmpty()) {
            leftProjectMaps.putAll(leftOutputColumns.getStream()
                    .map(columnRefId -> context.getColumnRefFactory().getColumnRef(columnRefId))
                    .collect(Collectors.toMap(Function.identity(), Function.identity())));

            LogicalProjectOperator leftProject = new LogicalProjectOperator(leftProjectMaps);
            OptExpression leftProjectOpt = OptExpression.create(leftProject, input.inputAt(0));
            newJoinOpt.setChild(0, leftProjectOpt);
        }

        if (!rightProjectMaps.isEmpty()) {
            rightProjectMaps.putAll(rightOutputColumns.getStream()
                    .map(columnRefId -> context.getColumnRefFactory().getColumnRef(columnRefId))
                    .collect(Collectors.toMap(Function.identity(), Function.identity())));

            LogicalProjectOperator rightProject = new LogicalProjectOperator(rightProjectMaps);
            OptExpression rightProjectOpt = OptExpression.create(rightProject, input.inputAt(1));
            newJoinOpt.setChild(1, rightProjectOpt);
        }

        if (leftProjectMaps.isEmpty() && rightProjectMaps.isEmpty()) {
            return Collections.emptyList();
        } else {
            return Lists.newArrayList(newJoinOpt);
        }
    }

    static class Rewriter extends ScalarOperatorVisitor<ScalarOperator, Void> {
        private final Map<ScalarOperator, ColumnRefOperator> operatorMap = new HashMap<>();

        public Rewriter(Map<ColumnRefOperator, ScalarOperator> operatorMap) {
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : operatorMap.entrySet()) {
                this.operatorMap.put(entry.getValue(), entry.getKey());
            }
        }

        @Override
        public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
            if (operatorMap.containsKey(scalarOperator)) {
                return operatorMap.get(scalarOperator);
            }

            for (int i = 0; i < scalarOperator.getChildren().size(); ++i) {
                scalarOperator.setChild(i, scalarOperator.getChild(i).accept(this, null));
            }

            return scalarOperator;
        }
    }
}
