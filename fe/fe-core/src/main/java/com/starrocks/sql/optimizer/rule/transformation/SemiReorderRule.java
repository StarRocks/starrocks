// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
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
        super(RuleType.TF_JOIN_SEMI_REORDER, Pattern.create(OperatorType.LOGICAL_JOIN).addChildren(
                Pattern.create(OperatorType.LOGICAL_PROJECT).addChildren(
                        Pattern.create(OperatorType.LOGICAL_JOIN).addChildren(
                                Pattern.create(OperatorType.PATTERN_MULTI_LEAF))),
                Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalJoinOperator topJoin = (LogicalJoinOperator) input.getOp();
        if (!topJoin.getJoinType().isLeftSemiJoin() && !topJoin.getJoinType().equals(JoinOperator.LEFT_ANTI_JOIN)) {
            return false;
        }

        OptExpression topJoinOpt = input;
        OptExpression bottomJoinOpt = input.inputAt(0).inputAt(0);
        OptExpression optX = bottomJoinOpt.inputAt(0);
        OptExpression optZ = topJoinOpt.inputAt(1);

        ColumnRefSet outputColumnsX = bottomJoinOpt.inputAt(0).getOutputColumns();
        // Because the X and Z nodes will be used to build a new semi join,
        // all existing predicates must be included in these two nodes
        ColumnRefSet usedInRewriteSemiJoin = new ColumnRefSet();
        usedInRewriteSemiJoin.union(optX.getOutputColumns());
        usedInRewriteSemiJoin.union(optZ.getOutputColumns());

        LogicalProjectOperator projectOperator = (LogicalProjectOperator) topJoinOpt.inputAt(0).getOp();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projectOperator.getColumnRefMap().entrySet()) {
            if (outputColumnsX.containsAll(entry.getValue().getUsedColumns())) {
                usedInRewriteSemiJoin.union(entry.getKey());
            }
        }

        if (!usedInRewriteSemiJoin.containsAll(topJoin.getOnPredicate().getUsedColumns())) {
            return false;
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        OptExpression topJoinOpt = input;
        OptExpression bottomJoinOpt = input.inputAt(0).inputAt(0);
        OptExpression optX = bottomJoinOpt.inputAt(0);
        OptExpression optY = bottomJoinOpt.inputAt(1);
        OptExpression optZ = topJoinOpt.inputAt(1);

        LogicalJoinOperator topSemiJoin = (LogicalJoinOperator) topJoinOpt.getOp();
        LogicalJoinOperator bottomJoin = (LogicalJoinOperator) bottomJoinOpt.getOp();
        LogicalProjectOperator projectAtBottomJoin = (LogicalProjectOperator) topJoinOpt.inputAt(0).getOp();

        Preconditions.checkState(topSemiJoin.getPredicate() == null);

        LogicalJoinOperator newTopJoin = new LogicalJoinOperator.Builder()
                .withOperator(bottomJoin)
                .setOutputColumns(topJoinOpt.getOutputColumns())
                .build();

        //re-colocate project
        HashMap<ColumnRefOperator, ScalarOperator> leftExpression = new HashMap<>();
        HashMap<ColumnRefOperator, ScalarOperator> rightExpression = new HashMap<>();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projectAtBottomJoin.getColumnRefMap().entrySet()) {
            if (!entry.getValue().isColumnRef()) {
                if (optX.getOutputColumns().containsAll(entry.getValue().getUsedColumns())) {
                    leftExpression.put(entry.getKey(), entry.getValue());
                }

                if (optY.getOutputColumns().containsAll(entry.getValue().getUsedColumns())) {
                    rightExpression.put(entry.getKey(), entry.getValue());
                }
            }
        }

        ColumnRefSet newBottomSemiJoinInputColumns = new ColumnRefSet();
        newBottomSemiJoinInputColumns.union(optX.getOutputColumns());
        newBottomSemiJoinInputColumns.union(new ColumnRefSet(new ArrayList<>(leftExpression.keySet())));
        newBottomSemiJoinInputColumns.union(optZ.getOutputColumns());

        ColumnRefSet newSemiOutputColumns = new ColumnRefSet();
        for (int id : newBottomSemiJoinInputColumns.getColumnIds()) {
            if (topJoinOpt.getOutputColumns().contains(id)) {
                newSemiOutputColumns.union(id);
            }

            if (newTopJoin.getRequiredChildInputColumns().contains(id)) {
                newSemiOutputColumns.union(id);
            }
        }

        Map<ColumnRefOperator, ScalarOperator> projectMap = new HashMap<>();
        if (newSemiOutputColumns.isEmpty()) {
            ColumnRefOperator smallestColumnRef = Utils.findSmallestColumnRef(
                    newBottomSemiJoinInputColumns.getStream().mapToObj(context.getColumnRefFactory()::getColumnRef)
                            .collect(Collectors.toList())
            );
            projectMap.put(smallestColumnRef, smallestColumnRef);
            newSemiOutputColumns.union(smallestColumnRef);
        } else {
            projectMap = newSemiOutputColumns.getStream().mapToObj(context.getColumnRefFactory()::getColumnRef)
                    .collect(Collectors.toMap(Function.identity(), Function.identity()));
        }

        LogicalJoinOperator newSemiJoin = new LogicalJoinOperator.Builder()
                .withOperator(topSemiJoin)
                .setOutputColumns(newSemiOutputColumns)
                .build();

        return Lists.newArrayList(OptExpression.create(newTopJoin, Lists.newArrayList(
                OptExpression.create(new LogicalProjectOperator(projectMap), Lists.newArrayList(
                        OptExpression.create(newSemiJoin, Lists.newArrayList(
                                buildProject(context, leftExpression, optX),
                                optZ)))),
                buildProject(context, rightExpression, optY))));
    }

    private OptExpression buildProject(OptimizerContext context, HashMap<ColumnRefOperator, ScalarOperator> project,
                                       OptExpression input) {
        if (project.isEmpty()) {
            return input;
        }

        project.putAll(input.getOutputColumns().getStream().mapToObj(id ->
                        context.getColumnRefFactory().getColumnRef(id))
                .collect(Collectors.toMap(Function.identity(), Function.identity())));

        return OptExpression.create(new LogicalProjectOperator(project), input);
    }
}