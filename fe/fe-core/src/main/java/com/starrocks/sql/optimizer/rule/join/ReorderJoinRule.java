// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.join;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ReorderJoinRule extends Rule {
    public ReorderJoinRule() {
        super(RuleType.TF_MULTI_JOIN_ORDER, Pattern.create(OperatorType.PATTERN));
    }

    private void extractRootInnerJoin(OptExpression root,
                                      List<OptExpression> results,
                                      boolean findNewRoot) {
        Operator operator = root.getOp();
        if (operator instanceof LogicalJoinOperator) {
            // If the user specifies joinHint, then no reorder
            if (!((LogicalJoinOperator) operator).getJoinHint().isEmpty()) {
                return;
            }
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) operator;
            if (joinOperator.isInnerOrCrossJoin()) {
                // For A inner join (B inner join C), we only think A is root tree
                if (!findNewRoot) {
                    findNewRoot = true;
                    results.add(root);
                }
            } else {
                findNewRoot = false;
            }
        } else {
            findNewRoot = false;
        }

        for (OptExpression child : root.getInputs()) {
            extractRootInnerJoin(child, results, findNewRoot);
        }
    }

    void enumerate(JoinOrder reorderAlgorithm, OptimizerContext context, OptExpression innerJoinRoot,
                   MultiJoinNode multiJoinNode) {
        reorderAlgorithm.reorder(Lists.newArrayList(multiJoinNode.getAtoms()),
                multiJoinNode.getPredicates());

        List<OptExpression> reorderTopKResult = reorderAlgorithm.getResult();
        LogicalJoinOperator oldRoot = (LogicalJoinOperator) innerJoinRoot.getOp();

        // Set limit to top join if needed
        if (oldRoot.hasLimit()) {
            for (OptExpression joinExpr : reorderTopKResult) {
                ((LogicalOperator) joinExpr.getOp()).setLimit(oldRoot.getLimit());
            }
        }

        OutputColumnsPrune prune = new OutputColumnsPrune(context);

        for (OptExpression joinExpr : reorderTopKResult) {
            joinExpr.getOp().setProjection(oldRoot.getProjection());
            ColumnRefSet requireInputColumns = ((LogicalJoinOperator) joinExpr.getOp()).getRequiredChildInputColumns();

            if (joinExpr.getOp().getProjection() == null) {
                ColumnRefSet outputColumns = new ColumnRefSet();
                innerJoinRoot.getInputs().forEach(opt -> outputColumns.union(opt.getOutputColumns()));
                requireInputColumns.union(outputColumns);

                Projection topProjection = new Projection(outputColumns.getStream()
                        .mapToObj(context.getColumnRefFactory()::getColumnRef)
                        .collect(Collectors.toMap(Function.identity(), Function.identity())), new HashMap<>());
                joinExpr.getOp().setProjection(topProjection);
            }

            for (int i = 0; i < joinExpr.arity(); ++i) {
                OptExpression optExpression = prune.rewrite(joinExpr.inputAt(i), requireInputColumns);
                joinExpr.setChild(i, optExpression);
            }

            joinExpr = new RemoveDuplicateProject(context).rewrite(joinExpr);

            context.getMemo().copyIn(innerJoinRoot.getGroupExpression().getGroup(), joinExpr);
        }
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        List<OptExpression> innerJoinTrees = Lists.newArrayList();
        extractRootInnerJoin(input, innerJoinTrees, false);
        if (!innerJoinTrees.isEmpty()) {
            // In order to reorder the bottom join tree firstly
            Collections.reverse(innerJoinTrees);
            for (OptExpression innerJoinRoot : innerJoinTrees) {
                MultiJoinNode multiJoinNode = MultiJoinNode.toMultiJoinNode(innerJoinRoot);

                enumerate(new JoinReorderLeftDeep(context), context, innerJoinRoot, multiJoinNode);

                if (multiJoinNode.getAtoms().size() <= context.getSessionVariable().getCboMaxReorderNodeUseDP()
                        && context.getSessionVariable().isCboEnableDPJoinReorder()) {
                    //10 table join reorder takes more than 100ms,
                    //so the join reorder using dp is currently controlled below 10.
                    enumerate(new JoinReorderDP(context), context, innerJoinRoot, multiJoinNode);
                }

                if (context.getSessionVariable().isCboEnableGreedyJoinReorder()) {
                    enumerate(new JoinReorderGreedy(context), context, innerJoinRoot, multiJoinNode);
                }
            }
        }
        return Collections.emptyList();
    }

    /**
     * Because the order of Join has changed,
     * the outputColumns of Join will also change accordingly.
     * Here we need to perform column cropping again on Join.
     */
    public static class OutputColumnsPrune extends OptExpressionVisitor<OptExpression, ColumnRefSet> {
        private final OptimizerContext optimizerContext;

        public OutputColumnsPrune(OptimizerContext optimizerContext) {
            this.optimizerContext = optimizerContext;
        }

        public OptExpression rewrite(OptExpression optExpression, ColumnRefSet pruneOutputColumns) {
            Operator operator = optExpression.getOp();
            if (operator.getProjection() != null) {
                Projection projection = operator.getProjection();

                for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projection.getColumnRefMap().entrySet()) {
                    if (!entry.getValue().isColumnRef()) {
                        return optExpression;
                    }

                    if (!entry.getKey().equals(entry.getValue())) {
                        return optExpression;
                    }
                }
            }

            return optExpression.getOp().accept(this, optExpression, pruneOutputColumns);
        }

        @Override
        public OptExpression visit(OptExpression optExpression, ColumnRefSet pruneOutputColumns) {
            return optExpression;
        }

        @Override
        public OptExpression visitLogicalJoin(OptExpression optExpression, ColumnRefSet requireColumns) {
            ColumnRefSet outputColumns = optExpression.getOutputColumns();
            ColumnRefSet newOutputColumns = new ColumnRefSet();
            for (int id : outputColumns.getColumnIds()) {
                if (requireColumns.contains(id)) {
                    newOutputColumns.union(id);
                }
            }
            requireColumns = ((LogicalJoinOperator) optExpression.getOp()).getRequiredChildInputColumns();

            ColumnRefSet childInputColumns = new ColumnRefSet();
            optExpression.getInputs().forEach(opt -> childInputColumns.union(
                    ((LogicalOperator) opt.getOp()).getOutputColumns(new ExpressionContext(opt))));

            LogicalJoinOperator joinOperator = new LogicalJoinOperator.Builder()
                    .withOperator((LogicalJoinOperator) optExpression.getOp())
                    .setProjection(new Projection(newOutputColumns.getStream()
                            .mapToObj(optimizerContext.getColumnRefFactory()::getColumnRef)
                            .collect(Collectors.toMap(Function.identity(), Function.identity())),
                            new HashMap<>()))
                    .build();

            requireColumns.union(newOutputColumns);
            OptExpression left = rewrite(optExpression.inputAt(0), (ColumnRefSet) requireColumns.clone());
            OptExpression right = rewrite(optExpression.inputAt(1), (ColumnRefSet) requireColumns.clone());

            OptExpression joinOpt = OptExpression.create(joinOperator, Lists.newArrayList(left, right));
            joinOpt.deriveLogicalPropertyItself();

            ExpressionContext expressionContext = new ExpressionContext(joinOpt);
            StatisticsCalculator statisticsCalculator = new StatisticsCalculator(
                    expressionContext, optimizerContext.getColumnRefFactory(), optimizerContext.getDumpInfo());
            statisticsCalculator.estimatorStats();
            joinOpt.setStatistics(expressionContext.getStatistics());
            return joinOpt;
        }
    }

    public static class RemoveDuplicateProject extends OptExpressionVisitor<OptExpression, Void> {
        private final OptimizerContext optimizerContext;

        public RemoveDuplicateProject(OptimizerContext optimizerContext) {
            this.optimizerContext = optimizerContext;
        }

        public OptExpression rewrite(OptExpression optExpression) {
            Operator operator = optExpression.getOp();
            if (operator.getProjection() != null) {
                Projection projection = operator.getProjection();

                for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projection.getColumnRefMap().entrySet()) {
                    if (!entry.getValue().isColumnRef()) {
                        return optExpression;
                    }

                    if (!entry.getKey().equals(entry.getValue())) {
                        return optExpression;
                    }
                }
            }

            return optExpression.getOp().accept(this, optExpression, null);
        }

        @Override
        public OptExpression visit(OptExpression optExpression, Void context) {
            return optExpression;
        }

        @Override
        public OptExpression visitLogicalJoin(OptExpression optExpression, Void context) {
            ColumnRefSet outputColumns = new ColumnRefSet(optExpression.getOp().getProjection().getOutputColumns());
            ColumnRefSet childInputColumns = new ColumnRefSet();
            optExpression.getInputs().forEach(opt -> childInputColumns.union(
                    ((LogicalOperator) opt.getOp()).getOutputColumns(new ExpressionContext(opt))));

            OptExpression left = rewrite(optExpression.inputAt(0));
            OptExpression right = rewrite(optExpression.inputAt(1));

            if (childInputColumns.equals(outputColumns)) {
                LogicalJoinOperator joinOperator = new LogicalJoinOperator.Builder().withOperator(
                                (LogicalJoinOperator) optExpression.getOp())
                        .setProjection(null).build();
                OptExpression joinOpt = OptExpression.create(joinOperator, Lists.newArrayList(left, right));
                joinOpt.deriveLogicalPropertyItself();

                ExpressionContext expressionContext = new ExpressionContext(joinOpt);
                StatisticsCalculator statisticsCalculator = new StatisticsCalculator(
                        expressionContext, optimizerContext.getColumnRefFactory(), optimizerContext.getDumpInfo());
                statisticsCalculator.estimatorStats();
                joinOpt.setStatistics(expressionContext.getStatistics());
                return joinOpt;
            } else {
                optExpression.setChild(0, left);
                optExpression.setChild(1, right);
                return optExpression;
            }
        }
    }
}
