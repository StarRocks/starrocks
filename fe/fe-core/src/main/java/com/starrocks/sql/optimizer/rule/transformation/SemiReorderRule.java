// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * JoinReorder Rule for Semi/Anti and other JoinNode
 * <p>
 * Semi(Join(X, Y), Z) => Join(Semi(X, Z), Y)
 */
public class SemiReorderRule extends TransformationRule {
    public SemiReorderRule() {
        super(RuleType.TF_JOIN_SEMI_REORDER, Pattern.create(OperatorType.LOGICAL_JOIN).
                addChildren(
                        Pattern.create(OperatorType.LOGICAL_JOIN).addChildren(Pattern.create(OperatorType.PATTERN_LEAF),
                                Pattern.create(OperatorType.PATTERN_LEAF)
                                        .addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF))),
                        Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalJoinOperator topJoin = (LogicalJoinOperator) input.getOp();
        if (!topJoin.getJoinType().isLeftSemiJoin() && !topJoin.getJoinType().equals(JoinOperator.LEFT_ANTI_JOIN)) {
            return false;
        }

        // Because the X and Z nodes will be used to build a new semi join,
        // all existing predicates must be included in these two nodes
        ColumnRefSet usedInRewriteSemiJoin = new ColumnRefSet();
        usedInRewriteSemiJoin.union(input.inputAt(0).inputAt(0).getOutputColumns());
        usedInRewriteSemiJoin.union(input.inputAt(1).getOutputColumns());

        if (!usedInRewriteSemiJoin.containsAll(topJoin.getOnPredicate().getUsedColumns())) {
            return false;
        }

        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator topJoin = (LogicalJoinOperator) input.getOp();
        LogicalJoinOperator leftChildJoin = (LogicalJoinOperator) input.inputAt(0).getOp();

        Preconditions.checkState(topJoin.getPredicate() == null);

        LogicalJoinOperator newTopJoin = new LogicalJoinOperator.Builder().withOperator(leftChildJoin)
                .setProjection(topJoin.getProjection())
                .build();

        ColumnRefSet leftChildInputColumns = new ColumnRefSet();
        leftChildInputColumns.union(input.inputAt(0).inputAt(0).getOutputColumns());
        leftChildInputColumns.union(input.inputAt(1).getOutputColumns());

        ColumnRefSet newSemiOutputColumns = new ColumnRefSet();
        for (int id : leftChildInputColumns.getColumnIds()) {
            if (newTopJoin.getProjection().getOutputColumns().stream().anyMatch(c -> c.getId() == id)) {
                newSemiOutputColumns.union(id);
            }

            if (newTopJoin.getRequiredChildInputColumns().contains(id)) {
                newSemiOutputColumns.union(id);
            }
        }

        // recompute new semi join and right child projection
        OptExpression leftChildJoinRightChild = input.inputAt(0).inputAt(1);
        ColumnRefSet leftChildJoinRightChildOutputColumns = leftChildJoinRightChild.getOutputColumns();

        Projection leftChildJoinProjection = leftChildJoin.getProjection();
        HashMap<ColumnRefOperator, ScalarOperator> rightExpression = new HashMap<>();
        HashMap<ColumnRefOperator, ScalarOperator> semiExpression = new HashMap<>();
        if (leftChildJoinProjection != null) {
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : leftChildJoinProjection.getColumnRefMap()
                    .entrySet()) {
                boolean isProjectToColumnRef = entry.getValue().isColumnRef();
                if (!isProjectToColumnRef &&
                        leftChildJoinRightChildOutputColumns.containsAll(entry.getValue().getUsedColumns())) {
                    rightExpression.put(entry.getKey(), entry.getValue());
                } else if (!isProjectToColumnRef &&
                        newSemiOutputColumns.containsAll(entry.getValue().getUsedColumns())) {
                    semiExpression.put(entry.getKey(), entry.getValue());
                } else if (!isProjectToColumnRef && leftChildInputColumns.containsAll(entry.getValue().getUsedColumns())) {
                    // left child projection produce
                    semiExpression.put(entry.getKey(), entry.getValue());
                }
            }
        }

        Map<ColumnRefOperator, ScalarOperator> projectMap = new HashMap<>();
        if (newSemiOutputColumns.isEmpty()) {
            ColumnRefOperator smallestColumnRef = Utils.findSmallestColumnRef(
                    leftChildInputColumns.getStream().mapToObj(context.getColumnRefFactory()::getColumnRef)
                            .collect(Collectors.toList())
            );
            projectMap.put(smallestColumnRef, smallestColumnRef);
        } else {
            projectMap = newSemiOutputColumns.getStream()
                    .filter(c -> newTopJoin.getRequiredChildInputColumns().contains(c))
                    .mapToObj(context.getColumnRefFactory()::getColumnRef)
                    .collect(Collectors.toMap(Function.identity(), Function.identity()));
        }

        LogicalJoinOperator newSemiJoin;
        // build new semi join projection
        if (semiExpression.isEmpty()) {
            newSemiJoin = new LogicalJoinOperator.Builder().withOperator(topJoin)
                    .setProjection(new Projection(projectMap)).build();
        } else {
            semiExpression.putAll(projectMap);
            newSemiJoin = new LogicalJoinOperator.Builder().withOperator(topJoin)
                    .setProjection(new Projection(semiExpression)).build();
        }

        // build new right child projection
        OptExpression newRightChild = leftChildJoinRightChild;
        if (!rightExpression.isEmpty()) {
            Map<ColumnRefOperator, ScalarOperator> expressionProject;
            if (leftChildJoinRightChild.getOp().getProjection() == null) {
                expressionProject = leftChildJoinRightChild.getOutputColumns().getStream()
                        .mapToObj(id -> context.getColumnRefFactory().getColumnRef(id))
                        .collect(Collectors.toMap(Function.identity(), Function.identity()));
            } else {
                expressionProject = Maps.newHashMap(leftChildJoinRightChild.getOp().getProjection().getColumnRefMap());
            }

            // Use leftChildJoinRightChild projection to rewrite the rightExpression, it's like two project node merge.
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(expressionProject);
            Map<ColumnRefOperator, ScalarOperator> rewriteMap = Maps.newHashMap(expressionProject);
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : rightExpression.entrySet()) {
                rewriteMap.put(entry.getKey(), entry.getValue().accept(rewriter, null));
            }

            Operator.Builder builder = OperatorBuilderFactory.build(leftChildJoinRightChild.getOp());
            Operator newRightChildOperator =
                    builder.withOperator(leftChildJoinRightChild.getOp()).setProjection(new Projection(rewriteMap))
                            .build();
            newRightChild = OptExpression.create(newRightChildOperator, leftChildJoinRightChild.getInputs());
        }

        return Lists.newArrayList(OptExpression.create(newTopJoin,
                Lists.newArrayList(
                        OptExpression.create(newSemiJoin,
                                Lists.newArrayList(input.inputAt(0).inputAt(0), input.inputAt(1))),
                        newRightChild)));
    }
}