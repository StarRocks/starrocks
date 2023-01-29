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

package com.starrocks.sql.optimizer.rule.join;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.PlannerProfile;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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
        try (PlannerProfile.ScopedTimer ignore = PlannerProfile.getScopedTimer(
                reorderAlgorithm.getClass().getSimpleName())) {
            reorderAlgorithm.reorder(Lists.newArrayList(multiJoinNode.getAtoms()),
                    multiJoinNode.getPredicates(), multiJoinNode.getExpressionMap());
        }

        List<OptExpression> reorderTopKResult = reorderAlgorithm.getResult();
        LogicalJoinOperator oldRoot = (LogicalJoinOperator) innerJoinRoot.getOp();

        // Set limit to top join if needed
        if (oldRoot.hasLimit()) {
            for (OptExpression joinExpr : reorderTopKResult) {
                joinExpr.getOp().setLimit(oldRoot.getLimit());
            }
        }

        OutputColumnsPrune prune = new OutputColumnsPrune(context);
        for (OptExpression joinExpr : reorderTopKResult) {
            ColumnRefSet outputColumns = new ColumnRefSet();
            Map<ColumnRefOperator, ScalarOperator> projectMap = new HashMap<>();
            if (oldRoot.getProjection() == null) {
                innerJoinRoot.getInputs().forEach(opt -> outputColumns.union(opt.getOutputColumns()));

                projectMap.putAll(outputColumns.getStream()
                        .map(context.getColumnRefFactory()::getColumnRef)
                        .collect(Collectors.toMap(Function.identity(), Function.identity())));
            } else {
                outputColumns.union(oldRoot.getProjection().getOutputColumns());
                projectMap.putAll(oldRoot.getProjection().getColumnRefMap());
            }

            ColumnRefSet newRootInputColumns = new ColumnRefSet();
            joinExpr.getInputs().forEach(opt -> newRootInputColumns.union(opt.getOutputColumns()));
            ColumnRefSet expressionKeys = new ColumnRefSet(new ArrayList<>(multiJoinNode.getExpressionMap().keySet()));
            for (int id : outputColumns.getColumnIds()) {
                // If the ColumnRef contained in the output before reorder does not exist after reorder.
                // Explain that this is an expression that is not referenced by onPredicate,
                // but the upstream node does depend on this input,
                // so we need to restore this expression at this position
                if (!newRootInputColumns.contains(id) && expressionKeys.contains(id)) {
                    ScalarOperator scalarOperator =
                            multiJoinNode.getExpressionMap().get(context.getColumnRefFactory().getColumnRef(id));
                    // The expression map in multiJoinNode could have map like this :
                    //  21 -> cast(20 as varchar)
                    //  20 -> constant operator
                    // The expression map key 21 should use replaceColumnRewriter to rewrite the value instead of use
                    // its value cast operator directly.
                    ReplaceColumnRefRewriter replaceColumnRefRewriter =
                            new ReplaceColumnRefRewriter(multiJoinNode.getExpressionMap(), true);
                    projectMap.put(context.getColumnRefFactory().getColumnRef(id),
                            replaceColumnRefRewriter.rewrite(scalarOperator));
                }
            }
            joinExpr.getOp().setProjection(new Projection(projectMap));
            ColumnRefSet requireInputColumns = ((LogicalJoinOperator) joinExpr.getOp()).getRequiredChildInputColumns();
            requireInputColumns.union(outputColumns);

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
                if (!multiJoinNode.checkDependsPredicate()) {
                    continue;
                }
                enumerate(new JoinReorderLeftDeep(context), context, innerJoinRoot, multiJoinNode);
                // If there is no statistical information, the DP and greedy reorder algorithm are disabled,
                // and the query plan degenerates to the left deep tree
                if (Utils.hasUnknownColumnsStats(input) && !FeConstants.runningUnitTest) {
                    continue;
                }

                if (multiJoinNode.getAtoms().size() <= context.getSessionVariable().getCboMaxReorderNodeUseDP()
                        && context.getSessionVariable().isCboEnableDPJoinReorder()) {
                    // 10 table join reorder takes more than 100ms,
                    // so the join reorder using dp is currently controlled below 10.
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

        public OptExpression rewrite(OptExpression optExpression, ColumnRefSet requiredColumns) {
            Operator operator = optExpression.getOp();
            if (operator.getProjection() != null) {
                Projection projection = operator.getProjection();

                List<ColumnRefOperator> outputColumns = Lists.newArrayList();
                for (ColumnRefOperator key : projection.getColumnRefMap().keySet()) {
                    if (requiredColumns.contains(key)) {
                        outputColumns.add(key);
                    }
                }

                if (outputColumns.size() == 0) {
                    outputColumns.add(Utils.findSmallestColumnRef(projection.getOutputColumns()));
                }

                if (outputColumns.size() != projection.getColumnRefMap().size()) {
                    Map<ColumnRefOperator, ScalarOperator> newOutputProjections = Maps.newHashMap();
                    for (ColumnRefOperator ref : outputColumns) {
                        newOutputProjections.put(ref, projection.getColumnRefMap().get(ref));
                    }
                    optExpression = deriveNewOptExpression(optExpression, newOutputProjections);
                }

                for (ScalarOperator value : optExpression.getOp().getProjection().getColumnRefMap().values()) {
                    requiredColumns.union(value.getUsedColumns());
                }
            }

            return optExpression.getOp().accept(this, optExpression, requiredColumns);
        }

        @Override
        public OptExpression visit(OptExpression optExpression, ColumnRefSet pruneOutputColumns) {
            return optExpression;
        }

        @Override
        public OptExpression visitLogicalJoin(OptExpression optExpression, ColumnRefSet requireColumns) {
            // use children output columns as join output columns.
            ColumnRefSet outputColumns = optExpression.inputAt(0).getOutputColumns().clone();
            outputColumns.union(optExpression.inputAt(1).getOutputColumns());
            ColumnRefSet newOutputColumns = new ColumnRefSet();
            for (int id : outputColumns.getColumnIds()) {
                if (requireColumns.contains(id)) {
                    newOutputColumns.union(id);
                }
            }

            LogicalJoinOperator joinOperator = (LogicalJoinOperator) optExpression.getOp();
            if (joinOperator.getProjection() == null && !newOutputColumns.isEmpty()) {
                joinOperator = new LogicalJoinOperator.Builder()
                        .withOperator((LogicalJoinOperator) optExpression.getOp())
                        .setProjection(new Projection(newOutputColumns.getStream()
                                .map(optimizerContext.getColumnRefFactory()::getColumnRef)
                                .collect(Collectors.toMap(Function.identity(), Function.identity())),
                                new HashMap<>()))
                        .build();
            }

            requireColumns = ((LogicalJoinOperator) optExpression.getOp()).getRequiredChildInputColumns();
            requireColumns.union(newOutputColumns);
            OptExpression left = rewrite(optExpression.inputAt(0), (ColumnRefSet) requireColumns.clone());
            OptExpression right = rewrite(optExpression.inputAt(1), (ColumnRefSet) requireColumns.clone());

            OptExpression joinOpt = OptExpression.create(joinOperator, Lists.newArrayList(left, right));
            joinOpt.deriveLogicalPropertyItself();

            ExpressionContext expressionContext = new ExpressionContext(joinOpt);
            StatisticsCalculator statisticsCalculator = new StatisticsCalculator(
                    expressionContext, optimizerContext.getColumnRefFactory(), optimizerContext);
            statisticsCalculator.estimatorStats();
            joinOpt.setStatistics(expressionContext.getStatistics());
            return joinOpt;
        }

        private OptExpression deriveNewOptExpression(OptExpression optExpression,
                                                     Map<ColumnRefOperator, ScalarOperator> newOutputProjections) {
            Operator operator = optExpression.getOp();
            ColumnRefSet newCols = new ColumnRefSet(newOutputProjections.keySet());
            LogicalProperty newProperty = new LogicalProperty(optExpression.getLogicalProperty());
            newProperty.setOutputColumns(newCols);

            Statistics newStats = Statistics.buildFrom(optExpression.getStatistics()).build();
            Iterator<Map.Entry<ColumnRefOperator, ColumnStatistic>>
                    iterator = newStats.getColumnStatistics().entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<ColumnRefOperator, ColumnStatistic> columnStatistic = iterator.next();
                if (!newCols.contains(columnStatistic.getKey())) {
                    iterator.remove();
                }
            }

            Operator.Builder builder = OperatorBuilderFactory.build(operator);
            Operator newOp = builder.withOperator(operator)
                    .setProjection(new Projection(newOutputProjections))
                    .build();

            OptExpression newOpt = OptExpression.create(newOp, optExpression.getInputs());
            newOpt.setLogicalProperty(newProperty);
            newOpt.setStatistics(newStats);
            return newOpt;
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
            ColumnRefSet childInputColumns = new ColumnRefSet();
            optExpression.getInputs().forEach(opt -> childInputColumns.union(
                    ((LogicalOperator) opt.getOp()).getOutputColumns(new ExpressionContext(opt))));

            OptExpression left = rewrite(optExpression.inputAt(0));
            OptExpression right = rewrite(optExpression.inputAt(1));

            ColumnRefSet outputColumns = new ColumnRefSet();
            if (optExpression.getOp().getProjection() != null) {
                outputColumns = new ColumnRefSet(optExpression.getOp().getProjection().getOutputColumns());
            }

            if (childInputColumns.equals(outputColumns)) {
                LogicalJoinOperator joinOperator = new LogicalJoinOperator.Builder().withOperator(
                                (LogicalJoinOperator) optExpression.getOp())
                        .setProjection(null).build();
                OptExpression joinOpt = OptExpression.create(joinOperator, Lists.newArrayList(left, right));
                joinOpt.deriveLogicalPropertyItself();

                ExpressionContext expressionContext = new ExpressionContext(joinOpt);
                StatisticsCalculator statisticsCalculator = new StatisticsCalculator(
                        expressionContext, optimizerContext.getColumnRefFactory(), optimizerContext);
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
