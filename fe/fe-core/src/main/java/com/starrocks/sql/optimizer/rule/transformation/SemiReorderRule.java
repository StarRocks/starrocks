// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
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

        // Because the X and Z nodes will be used to build a new semi join,
        // all existing predicates must be included in these two nodes
        ColumnRefSet usedInRewriteSemiJoin = new ColumnRefSet();
        usedInRewriteSemiJoin.union(input.inputAt(0).inputAt(0).inputAt(0).getOutputColumns());
        usedInRewriteSemiJoin.union(input.inputAt(1).getOutputColumns());

        if (!usedInRewriteSemiJoin.containsAll(topJoin.getOnPredicate().getUsedColumns())) {
            return false;
        }

        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        OptExpression topJoinOpt = input;
        OptExpression bottomJoinOpt = input.inputAt(0).inputAt(0);

        LogicalJoinOperator topJoin = (LogicalJoinOperator) topJoinOpt.getOp();
        LogicalJoinOperator leftChildJoin = (LogicalJoinOperator) bottomJoinOpt.getOp();

        Preconditions.checkState(topJoin.getPredicate() == null);

        LogicalJoinOperator newTopJoin = new LogicalJoinOperator.Builder()
                .withOperator(leftChildJoin)
                .setOutputColumns(topJoinOpt.getOutputColumns())
                .build();

        ColumnRefSet newBottomJoinInputColumns = new ColumnRefSet();
        newBottomJoinInputColumns.union(bottomJoinOpt.inputAt(0).getOutputColumns());
        newBottomJoinInputColumns.union(topJoinOpt.inputAt(1).getOutputColumns());

        ColumnRefSet newSemiOutputColumns = new ColumnRefSet();
        for (int id : newBottomJoinInputColumns.getColumnIds()) {
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
                    newBottomJoinInputColumns.getStream().mapToObj(context.getColumnRefFactory()::getColumnRef)
                            .collect(Collectors.toList())
            );
            projectMap.put(smallestColumnRef, smallestColumnRef);
        } else {
            projectMap = newSemiOutputColumns.getStream().mapToObj(context.getColumnRefFactory()::getColumnRef)
                    .collect(Collectors.toMap(Function.identity(), Function.identity()));
        }

        LogicalProjectOperator newProjectOperator = new LogicalProjectOperator(projectMap);
        LogicalJoinOperator newSemiJoin = new LogicalJoinOperator.Builder().withOperator(topJoin)
                .setOutputColumns(new ColumnRefSet(new ArrayList<>(projectMap.keySet())))
                .build();

        return Lists.newArrayList(OptExpression.create(newTopJoin, Lists.newArrayList(
                OptExpression.create(newProjectOperator, Lists.newArrayList(
                        OptExpression.create(newSemiJoin, Lists.newArrayList(
                                bottomJoinOpt.inputAt(0),
                                topJoinOpt.inputAt(1))))),
                bottomJoinOpt.inputAt(1)
        )));
    }
}