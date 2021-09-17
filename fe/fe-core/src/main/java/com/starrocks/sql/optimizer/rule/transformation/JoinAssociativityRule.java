// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
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
        OptExpression leftChildLeftChild = leftChild.inputAt(0);
        OptExpression leftChildRightChild = leftChild.inputAt(1);

        LogicalJoinOperator parentJoin = (LogicalJoinOperator) input.getOp();
        LogicalJoinOperator leftChildJoin = (LogicalJoinOperator) leftChild.getOp();
        // We do this check here not in check method, because check here is very simple
        if (!leftChildJoin.getJoinType().isInnerJoin() && !leftChildJoin.getJoinType().isCrossJoin()) {
            return Collections.emptyList();
        }

        List<ScalarOperator> allConjuncts = Streams.concat(
                        Utils.extractConjuncts(parentJoin.getOnPredicate()).stream(),
                        Utils.extractConjuncts(leftChildJoin.getOnPredicate()).stream())
                .collect(Collectors.toList());

        ColumnRefSet newRightChildOutputColumns = new ColumnRefSet();
        newRightChildOutputColumns.union(rightChild.getOutputColumns());
        newRightChildOutputColumns.union(leftChildRightChild.getOutputColumns());

        List<ScalarOperator> newChildConjuncts = Lists.newArrayList();
        List<ScalarOperator> newParentConjuncts = Lists.newArrayList();
        for (ScalarOperator conjunct : allConjuncts) {
            if (newRightChildOutputColumns.contains(conjunct.getUsedColumns())) {
                newChildConjuncts.add(conjunct);
            } else {
                newParentConjuncts.add(conjunct);
            }
        }

        // Eliminate cross join
        if (newChildConjuncts.isEmpty() || newParentConjuncts.isEmpty()) {
            return Collections.emptyList();
        }

        // compute right child join output columns
        // right child join output columns not only contains the parent join output, but also contains the parent conjuncts used columns
        ColumnRefSet outputColumns = parentJoin.getOutputColumns(new ExpressionContext(input));
        outputColumns.union(parentJoin.getUsedColumns());
        newParentConjuncts.forEach(conjunct -> outputColumns.union(conjunct.getUsedColumns()));

        List<ColumnRefOperator> newRightOutputColumns =
                newRightChildOutputColumns.getStream().filter(outputColumns::contains).
                        mapToObj(id -> context.getColumnRefFactory().getColumnRef(id)).collect(Collectors.toList());

        //newRightOutputColumns =
        //        newRightChildOutputColumns.getStream().
        //                mapToObj(id -> context.getColumnRefFactory().getColumnRef(id)).collect(Collectors.toList());

        LogicalJoinOperator rightChildJoinOperator =
                new LogicalJoinOperator(JoinOperator.INNER_JOIN, Utils.compoundAnd(newChildConjuncts));
        rightChildJoinOperator.setProjection(
                new Projection(newRightOutputColumns.stream().collect(Collectors.toMap(Function.identity(),
                        Function.identity())), null));
        OptExpression newRightChildJoin = OptExpression.create(rightChildJoinOperator, leftChildRightChild, rightChild);

        LogicalJoinOperator topJoinOperator =
                new LogicalJoinOperator(JoinOperator.INNER_JOIN, Utils.compoundAnd(newParentConjuncts),
                        parentJoin.getLimit(), "");
        topJoinOperator.setProjection(parentJoin.getProjection());

        OptExpression topJoin = OptExpression.create(
                topJoinOperator,
                leftChildLeftChild,
                newRightChildJoin);

        return Lists.newArrayList(topJoin);
    }
}
