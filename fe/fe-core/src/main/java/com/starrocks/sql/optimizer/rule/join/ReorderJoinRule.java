// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.join;

import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReorderJoinRule extends Rule {
    public ReorderJoinRule() {
        super(RuleType.TF_MULTI_JOIN_ORDER, Pattern.create(OperatorType.PATTERN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        OptExpression rewriteJoinTree = new MergeProjectWithJoin().rewrite(input);

        List<OptExpression> innerJoinTrees = Lists.newArrayList();
        extractRootInnerJoin(rewriteJoinTree, innerJoinTrees, false);
        if (!innerJoinTrees.isEmpty()) {
            // In order to reorder the bottom join tree firstly
            Collections.reverse(innerJoinTrees);
            for (OptExpression innerJoinRoot : innerJoinTrees) {
                MultiJoinNode multiJoinNode = MultiJoinNode.toMultiJoinNode(innerJoinRoot);

                enumerate(new JoinReorderLeftDeep(context), context, (MultiJoinOperator) innerJoinRoot.getOp(),
                        multiJoinNode);
                //If there is no statistical information, the DP and greedy reorder algorithm are disabled,
                //and the query plan degenerates to the left deep tree
                if (Utils.hasUnknownColumnsStats(input) && !FeConstants.runningUnitTest) {
                    continue;
                }

                if (multiJoinNode.getAtoms().size() <= context.getSessionVariable().getCboMaxReorderNodeUseDP()
                        && context.getSessionVariable().isCboEnableDPJoinReorder()) {
                    //10 table join reorder takes more than 100ms,
                    //so the join reorder using dp is currently controlled below 10.
                    enumerate(new JoinReorderDP(context), context, (MultiJoinOperator) innerJoinRoot.getOp(),
                            multiJoinNode);
                }

                if (context.getSessionVariable().isCboEnableGreedyJoinReorder()) {
                    enumerate(new JoinReorderGreedy(context), context, (MultiJoinOperator) innerJoinRoot.getOp(),
                            multiJoinNode);
                }
            }
        }
        return Collections.emptyList();
    }

    private void extractRootInnerJoin(OptExpression root,
                                      List<OptExpression> results,
                                      boolean findNewRoot) {
        Operator operator = root.getOp();
        if (operator instanceof MultiJoinOperator) {
            // If the user specifies joinHint, then no reorder
            if (!((MultiJoinOperator) operator).getJoinHint().isEmpty()) {
                return;
            }
            MultiJoinOperator joinOperator = (MultiJoinOperator) operator;
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

    void enumerate(JoinOrder reorderAlgorithm, OptimizerContext context,
                   MultiJoinOperator root,
                   MultiJoinNode multiJoinNode) {
        reorderAlgorithm.reorder(Lists.newArrayList(multiJoinNode.getAtoms()),
                multiJoinNode.getPredicates(), multiJoinNode.getExpressionMap());

        List<OptExpression> reorderTopKResult = reorderAlgorithm.getResult();

        // Set limit to top join if needed
        //FIXME
        /*
        if (oldRoot.hasLimit()) {
            for (OptExpression joinExpr : reorderTopKResult) {
                joinExpr.getOp().setLimit(oldRoot.getLimit());
            }
        }

         */

        for (OptExpression joinExpr : reorderTopKResult) {
            ColumnRefSet outputColumns = new ColumnRefSet();

            outputColumns.union(root.getOutputColumns());
            Map<ColumnRefOperator, ScalarOperator> projectMap =
                    new HashMap<>(root.getProjectOperator().getColumnRefMap());

            ColumnRefSet newRootInputColumns = new ColumnRefSet();
            joinExpr.getInputs().forEach(opt -> newRootInputColumns.union(opt.getOutputColumns()));
            ColumnRefSet expressionKeys = new ColumnRefSet(new ArrayList<>(multiJoinNode.getExpressionMap().keySet()));
            for (int id : outputColumns.getColumnIds()) {
                // If the ColumnRef contained in the output before reorder does not exist after reorder.
                // Explain that this is an expression that is not referenced by onPredicate,
                // but the upstream node does depend on this input,
                // so we need to restore this expression at this position
                if (!newRootInputColumns.contains(id) && expressionKeys.contains(id)) {
                    projectMap.put(
                            context.getColumnRefFactory().getColumnRef(id),
                            multiJoinNode.getExpressionMap().get(context.getColumnRefFactory().getColumnRef(id)));
                }
            }
            ((MultiJoinOperator) joinExpr.getOp()).setProjectOperator(new LogicalProjectOperator(projectMap));

            ColumnRefSet requireInputColumns = new ColumnRefSet(new ArrayList<>(projectMap.keySet()));
            joinExpr = new OutputColumnsPrune(context).rewrite(joinExpr, requireInputColumns);
            //joinExpr = new RemoveDuplicateProject(context).rewrite(joinExpr);
            joinExpr = new DecoupleMultiJoin(context).rewrite(joinExpr);
            joinExpr.deriveLogicalPropertyItself();
            context.getMemo().copyIn(root.getGroup(), joinExpr);
        }
    }

    public static class MergeProjectWithJoin extends OptExpressionVisitor<OptExpression, Void> {
        public OptExpression rewrite(OptExpression optExpression) {
            return optExpression.getOp().accept(this, optExpression, null);
        }

        @Override
        public OptExpression visit(OptExpression optExpr, Void context) {
            for (int idx = 0; idx < optExpr.arity(); ++idx) {
                optExpr.setChild(idx, rewrite(optExpr.inputAt(idx)));
            }
            return optExpr;
        }

        @Override
        public OptExpression visitLogicalProject(OptExpression optExpression, Void context) {
            visit(optExpression, context);
            if (optExpression.inputAt(0).getOp() instanceof LogicalJoinOperator) {
                LogicalJoinOperator joinOperator = (LogicalJoinOperator) optExpression.inputAt(0).getOp();
                if (!joinOperator.isInnerOrCrossJoin()) {
                    return optExpression;
                }

                MultiJoinOperator multiJoinOperator = new MultiJoinOperator(
                        (LogicalProjectOperator) optExpression.getOp(),
                        (LogicalJoinOperator) optExpression.inputAt(0).getOp());
                multiJoinOperator.setGroup(optExpression.getGroupExpression().getGroup());
                OptExpression multiOpt = OptExpression.create(multiJoinOperator, optExpression.inputAt(0).getInputs());
                multiOpt.deriveLogicalPropertyItself();

                return multiOpt;
            } else {
                return optExpression;
            }
        }
    }

    public static class DecoupleMultiJoin extends OptExpressionVisitor<OptExpression, Void> {
        private final OptimizerContext optimizerContext;

        public DecoupleMultiJoin(OptimizerContext optimizerContext) {
            this.optimizerContext = optimizerContext;
        }

        public OptExpression rewrite(OptExpression optExpression) {
            return optExpression.getOp().accept(this, optExpression, null);
        }

        @Override
        public OptExpression visit(OptExpression optExpr, Void context) {
            for (int idx = 0; idx < optExpr.arity(); ++idx) {
                optExpr.setChild(idx, rewrite(optExpr.inputAt(idx)));
            }
            return optExpr;
        }

        @Override
        public OptExpression visitMultiJoin(OptExpression optExpression, Void context) {
            visit(optExpression, context);

            MultiJoinOperator multiJoinOperator = (MultiJoinOperator) optExpression.getOp();

            OptExpression projectOpt =
                    OptExpression.create(multiJoinOperator.getJoinOperator(), optExpression.getInputs());
            projectOpt.deriveLogicalPropertyItself();

            ExpressionContext expressionContext = new ExpressionContext(projectOpt);
            StatisticsCalculator statisticsCalculator = new StatisticsCalculator(
                    expressionContext, optimizerContext.getColumnRefFactory(), optimizerContext);
            statisticsCalculator.estimatorStats();
            projectOpt.setStatistics(expressionContext.getStatistics());

            OptExpression joinOpt =
                    OptExpression.create(multiJoinOperator.getProjectOperator(), Lists.newArrayList(projectOpt));
            joinOpt.deriveLogicalPropertyItself();

            expressionContext = new ExpressionContext(joinOpt);
            statisticsCalculator = new StatisticsCalculator(
                    expressionContext, optimizerContext.getColumnRefFactory(), optimizerContext);
            statisticsCalculator.estimatorStats();
            joinOpt.setStatistics(expressionContext.getStatistics());

            return joinOpt;
        }
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
            return optExpression.getOp().accept(this, optExpression, pruneOutputColumns);
        }

        @Override
        public OptExpression visit(OptExpression optExpression, ColumnRefSet pruneOutputColumns) {
            return optExpression;
        }

        @Override
        public OptExpression visitMultiJoin(OptExpression optExpression, ColumnRefSet requireColumns) {
            MultiJoinOperator multiJoinOperator = (MultiJoinOperator) optExpression.getOp();
            ColumnRefSet outputColumns = multiJoinOperator.getOutputColumns();

            ColumnRefSet newOutputColumns = new ColumnRefSet();
            for (int id : outputColumns.getColumnIds()) {
                if (requireColumns.contains(id)) {
                    newOutputColumns.union(id);
                }
            }

            LogicalProjectOperator projectOperator = multiJoinOperator.getProjectOperator();
            Map<ColumnRefOperator, ScalarOperator> newProject = new HashMap<>();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projectOperator.getColumnRefMap().entrySet()) {
                if (newOutputColumns.contains(entry.getKey().getId())) {
                    newProject.put(entry.getKey(), entry.getValue());
                }
            }

            multiJoinOperator.setProjectOperator(new LogicalProjectOperator(newProject));
            requireColumns = multiJoinOperator.getRequiredChildInputColumns();
            OptExpression left = rewrite(optExpression.inputAt(0), (ColumnRefSet) requireColumns.clone());
            OptExpression right = rewrite(optExpression.inputAt(1), (ColumnRefSet) requireColumns.clone());

            OptExpression joinOpt = OptExpression.create(multiJoinOperator, Lists.newArrayList(left, right));
            joinOpt.deriveLogicalPropertyItself();

            ExpressionContext expressionContext = new ExpressionContext(joinOpt);
            StatisticsCalculator statisticsCalculator = new StatisticsCalculator(
                    expressionContext, optimizerContext.getColumnRefFactory(), optimizerContext);
            statisticsCalculator.estimatorStats();
            joinOpt.setStatistics(expressionContext.getStatistics());
            return joinOpt;
        }
    }
}
