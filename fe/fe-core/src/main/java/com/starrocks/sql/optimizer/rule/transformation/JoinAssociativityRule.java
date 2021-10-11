// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class JoinAssociativityRule extends TransformationRule {
    private JoinAssociativityRule() {
        super(RuleType.TF_JOIN_ASSOCIATIVITY, Pattern.create(OperatorType.LOGICAL_JOIN)
                .addChildren(
                        Pattern.create(OperatorType.LOGICAL_JOIN).addChildren(
                                Pattern.create(OperatorType.PATTERN_LEAF),
                                Pattern.create(OperatorType.PATTERN_LEAF)),
                        Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    private static final JoinAssociativityRule instance = new JoinAssociativityRule();

    public static JoinAssociativityRule getInstance() {
        return instance;
    }

    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) input.getOp();
        if (!joinOperator.getJoinHint().isEmpty() ||
                !((LogicalJoinOperator) input.inputAt(0).getOp()).getJoinHint().isEmpty()) {
            return false;
        }

        return joinOperator.getJoinType().isInnerJoin();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        OptExpression leftChild = input.inputAt(0);
        OptExpression rightChild = input.inputAt(1);

        LogicalJoinOperator parentJoin = (LogicalJoinOperator) input.getOp();
        LogicalJoinOperator leftChildJoin = (LogicalJoinOperator) leftChild.getOp();
        // We do this check here not in check method, because check here is very simple
        if (!leftChildJoin.getJoinType().isInnerJoin() && !leftChildJoin.getJoinType().isCrossJoin()) {
            return Collections.emptyList();
        }

        if (leftChildJoin.getProjection() != null) {
            Projection projection = leftChildJoin.getProjection();
            if (projection.getColumnRefMap().values().stream().anyMatch(s -> !s.isColumnRef())) {
                Preconditions.checkState(false);
            }
        }

        List<ScalarOperator> parentConjuncts = Utils.extractConjuncts(parentJoin.getOnPredicate());
        List<ScalarOperator> childConjuncts = Utils.extractConjuncts(leftChildJoin.getOnPredicate());

        List<ScalarOperator> allConjuncts = Lists.newArrayList();
        allConjuncts.addAll(parentConjuncts);
        allConjuncts.addAll(childConjuncts);

        OptExpression leftChild1 = leftChild.inputAt(0);
        OptExpression leftChild2 = leftChild.inputAt(1);

        ColumnRefSet newRightChildColumns = new ColumnRefSet();
        newRightChildColumns.union(rightChild.getOutputColumns());
        newRightChildColumns.union(leftChild2.getOutputColumns());

        List<ScalarOperator> newChildConjuncts = Lists.newArrayList();
        List<ScalarOperator> newParentConjuncts = Lists.newArrayList();
        for (ScalarOperator conjunct : allConjuncts) {
            if (newRightChildColumns.contains(conjunct.getUsedColumns())) {
                newChildConjuncts.add(conjunct);
            } else {
                newParentConjuncts.add(conjunct);
            }
        }

        // Eliminate cross join
        if (newChildConjuncts.isEmpty() || newParentConjuncts.isEmpty()) {
            return Collections.emptyList();
        }

        LogicalJoinOperator.Builder topJoinBuilder = new LogicalJoinOperator.Builder();
        LogicalJoinOperator topJoinOperator = topJoinBuilder.withOperator(parentJoin)
                .setJoinType(JoinOperator.INNER_JOIN)
                .setOnPredicate(Utils.compoundAnd(newParentConjuncts))
                .build();

        // compute right child join output columns
        // right child join output columns not only contains the parent join output, but also contains the parent conjuncts used columns
        /*
        ColumnRefSet parentJoinOutputColumns = parentJoin.getOutputColumns(new ExpressionContext(input));
        newParentConjuncts.forEach(conjunct -> parentJoinOutputColumns.union(conjunct.getUsedColumns()));
        List<ColumnRefOperator> newRightOutputColumns =
                newRightChildColumns.getStream().filter(outputColumns::contains).
                        mapToObj(id -> context.getColumnRefFactory().getColumnRef(id)).collect(Collectors.toList());

         */

        ColumnRefSet parentJoinRequiredColumns = parentJoin.getOutputColumns(new ExpressionContext(input));
        parentJoinRequiredColumns.union(topJoinOperator.getRequiredChildInputColumns());
        List<ColumnRefOperator> newRightOutputColumns = newRightChildColumns.getStream()
                .filter(parentJoinRequiredColumns::contains)
                .mapToObj(id -> context.getColumnRefFactory().getColumnRef(id)).collect(Collectors.toList());

        LogicalJoinOperator.Builder rightChildJoinOperatorBuilder = new LogicalJoinOperator.Builder();
        LogicalJoinOperator rightChildJoinOperator = (LogicalJoinOperator) rightChildJoinOperatorBuilder
                .setJoinType(JoinOperator.INNER_JOIN)
                .setOnPredicate(Utils.compoundAnd(newChildConjuncts))
                .setProjection(new Projection(newRightOutputColumns.stream()
                        .collect(Collectors.toMap(Function.identity(), Function.identity())), new HashMap<>()))
                .build();

        OptExpression newRightChildJoin = OptExpression.create(rightChildJoinOperator, leftChild2, rightChild);

        OptExpression topJoin = OptExpression.create(
                topJoinOperator,
                leftChild1,
                newRightChildJoin);

        return Lists.newArrayList(topJoin);
    }
}
