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

package com.starrocks.sql.optimizer.rule.tree;

import com.google.common.collect.Lists;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.expression.JoinOperator;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.tree.pdagg.PushDownAggregateCollector;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.ExpressionStatisticCalculator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;
import com.starrocks.sql.optimizer.task.TaskContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SemiJoinDeduplicateRule implements TreeRewriteRule {
    public static final int DISABLE_PUSH_DOWN_DISTINCT = -1;
    private static final Logger LOG = LogManager.getLogger(SemiJoinDeduplicateRule.class);
    private static final int PUSH_DOWN_DISTINCT_AUTO = 0;
    private static final int PUSH_DOWN_ALL_DISTINCT_FORCE = 1;

    private boolean hasRewrite;

    public SemiJoinDeduplicateRule() {
        hasRewrite = false;
    }

    public boolean hasRewrite() {
        return hasRewrite;
    }

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        if (taskContext.getOptimizerContext().getSessionVariable().getSemiJoinDeduplicateMode() ==
                DISABLE_PUSH_DOWN_DISTINCT) {
            return root;
        }
        DeduplicateVisitor visitor = new DeduplicateVisitor(taskContext);
        root = root.getOp().accept(visitor, root, DeduplicateContext.EMPTY);
        hasRewrite = visitor.hasRewrite();
        return root;
    }

    private static class DeduplicateContext {
        public static final DeduplicateContext EMPTY = new DeduplicateContext();

        boolean canDeduplicate;
        boolean parentIsSemiAntiJoin;
        // only used when canDeduplicate is true
        boolean isJoinLeftChild;

        public DeduplicateContext(boolean parentIsSemiJoin, boolean canDeduplicate, boolean isJoinLeftChild) {
            this.parentIsSemiAntiJoin = parentIsSemiJoin;
            this.canDeduplicate = canDeduplicate;
            this.isJoinLeftChild = isJoinLeftChild;
        }

        public DeduplicateContext() {
            this.canDeduplicate = false;
            this.parentIsSemiAntiJoin = false;
            this.isJoinLeftChild = false;
        }
    }

    private static class DeduplicateVisitor extends OptExpressionVisitor<OptExpression, DeduplicateContext> {
        private final OptimizerContext optimizerContext;
        private final ColumnRefFactory factory;
        private final SessionVariable sessionVariable;
        private boolean hasRewrite;
        private final boolean forcePushDown;

        public DeduplicateVisitor(TaskContext taskContext) {
            this.optimizerContext = taskContext.getOptimizerContext();
            this.factory = taskContext.getOptimizerContext().getColumnRefFactory();
            this.sessionVariable = taskContext.getOptimizerContext().getSessionVariable();
            this.hasRewrite = false;
            this.forcePushDown = sessionVariable.getSemiJoinDeduplicateMode() == PUSH_DOWN_ALL_DISTINCT_FORCE;
        }

        public boolean hasRewrite() {
            return hasRewrite;
        }

        private OptExpression visitChildren(OptExpression optExpression, DeduplicateContext context) {
            for (int i = 0; i < optExpression.getInputs().size(); i++) {
                optExpression.getInputs().set(i, process(optExpression.inputAt(i), DeduplicateContext.EMPTY));
            }
            return optExpression;
        }

        private OptExpression process(OptExpression optExpression, DeduplicateContext context) {
            return optExpression.getOp().accept(this, optExpression, context);
        }

        @Override
        public OptExpression visit(OptExpression opt, DeduplicateContext context) {
            // default behavior: forbidden push down
            return visitChildren(opt, context);
        }

        public OptExpression passThrough(OptExpression opt, DeduplicateContext context) {
            if (opt.getOp().hasLimit()) {
                return visitChildren(opt, context);
            }

            // pass through the context from parent to child
            opt.setChild(0, process(opt.inputAt(0), context));
            return opt;
        }

        @Override
        public OptExpression visitLogicalProject(OptExpression opt, DeduplicateContext context) {
            return passThrough(opt, context);
        }

        @Override
        public OptExpression visitLogicalFilter(OptExpression opt, DeduplicateContext context) {
            return passThrough(opt, context);
        }

        @Override
        public OptExpression visitLogicalAggregate(OptExpression opt, DeduplicateContext context) {
            if (opt.getOp().hasLimit()) {
                return visitChildren(opt, context);
            }

            LogicalAggregationOperator aggOperator = (LogicalAggregationOperator) opt.getOp();
            // support distinct or count(distinct)
            if (!aggOperator.getAggregations().isEmpty() &&
                    !aggOperator.getAggregations().values().stream().allMatch(agg -> agg.isDistinct())) {
                return visitChildren(opt, context);
            }
            OptExpression child = opt.inputAt(0);
            // when meets distinct agg, we unset 'parentIsSemiJoin' and set 'canDeduplicate'
            // this is because we only want to do deduplicate to children like:
            // distinct->semi join->children, instead of semi join->distinct->children
            DeduplicateContext newContext = new DeduplicateContext(false, true, false);
            opt.setChild(0, process(child, newContext));
            return opt;
        }

        @Override
        public OptExpression visitLogicalJoin(OptExpression opt, DeduplicateContext context) {
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) opt.getOp();
            // 1. don't support limit
            if (joinOperator.hasLimit()) {
                return visit(opt, context);
            }

            // 2. only support semi/anti
            JoinOperator joinType = joinOperator.getJoinType();
            if (!joinType.isSemiAntiJoin()) {
                return visit(opt, context);
            }

            ColumnRefSet leftOutputColumns = opt.inputAt(0).getOutputColumns();
            ColumnRefSet rightOutputColumns = opt.inputAt(1).getOutputColumns();

            List<BinaryPredicateOperator> equalConjs =
                    JoinHelper.getEqualsPredicate(leftOutputColumns, rightOutputColumns,
                            Utils.extractConjuncts(joinOperator.getOnPredicate()));
            // 3. only support with one equal condition in onPredicate when forcePushDown is false
            if (!forcePushDown && (equalConjs.size() != 1 || joinOperator.getPredicate() != null)) {
                return visit(opt, context);
            }

            if (joinType.isLeftSemiAntiJoin()) {
                // left semi/anti join's left side can be deduplicated when parent doesn't care about input's repetition rate
                if (context.canDeduplicate) {
                    DeduplicateContext newContextForLeftChild = new DeduplicateContext(true, true, true);
                    opt.setChild(0, process(opt.inputAt(0), newContextForLeftChild));
                } else {
                    opt.setChild(0, process(opt.inputAt(0), DeduplicateContext.EMPTY));
                }

                // left semi/anti join's right side always can be deduplicated
                DeduplicateContext newContextForRightChild = new DeduplicateContext(true, true, false);
                opt.setChild(1, process(opt.inputAt(1), newContextForRightChild));
            } else {
                // right semi/anti join's left side always can be deduplicated
                DeduplicateContext newContextForLeftChild = new DeduplicateContext(true, true, true);
                opt.setChild(0, process(opt.inputAt(0), newContextForLeftChild));

                if (context.canDeduplicate) {
                    // right semi/anti join's right side can be deduplicated when parent doesn't care about input's repetition rate
                    DeduplicateContext newContextForRightChild = new DeduplicateContext(true, true, false);
                    opt.setChild(1, process(opt.inputAt(1), newContextForRightChild));
                } else {
                    opt.setChild(1, process(opt.inputAt(1), DeduplicateContext.EMPTY));
                }
            }

            return opt;
        }

        @Override
        public OptExpression visitLogicalTableScan(OptExpression opt, DeduplicateContext context) {
            if (opt.getOp().hasLimit()) {
                return visitChildren(opt, context);
            }

            if (!context.canDeduplicate || !context.parentIsSemiAntiJoin) {
                return opt;
            }

            ExpressionContext expressionContext = new ExpressionContext(opt);
            StatisticsCalculator statisticsCalculator =
                    new StatisticsCalculator(expressionContext, factory, optimizerContext);
            statisticsCalculator.estimatorStats();

            ColumnRefSet outputColumns = opt.getOutputColumns();
            List<ColumnRefOperator> groupBys =
                    factory.getColumnRefs(outputColumns).stream().collect(Collectors.toList());
            if (checkStatistics(context, outputColumns, expressionContext.getStatistics())) {
                hasRewrite = true;

                LogicalAggregationOperator distinct = getLogicalAggregationOperator(groupBys);
                return OptExpression.create(distinct, opt);
            } else {
                return opt;
            }
        }

        @NotNull
        private LogicalAggregationOperator getLogicalAggregationOperator(List<ColumnRefOperator> groupBys) {
            AggType aggType;
            if (sessionVariable.getCboPushDownDistinct().equalsIgnoreCase("local")) {
                aggType = AggType.LOCAL;
            } else {
                aggType = AggType.GLOBAL;
            }

            LogicalAggregationOperator distinct = new LogicalAggregationOperator(aggType, groupBys, new HashMap<>());
            if (aggType.isLocal()) {
                distinct.setOnlyLocalAggregate();
            }
            return distinct;
        }

        private boolean checkStatistics(DeduplicateContext context, ColumnRefSet groupBys, Statistics statistics) {
            if (forcePushDown) {
                return true;
            }

            // add a distinct node in left child can be a bad case when join is brodcast join or bucket shuffle join
            // so we have forbidden it for safety
            if (context.isJoinLeftChild) {
                return false;
            }

            if (groupBys.size() > 2) {
                return false;
            }

            Set<ColumnStatistic> columnStatistics = groupBys.getStream().map(factory::getColumnRef)
                    .map(s -> ExpressionStatisticCalculator.calculate(s, statistics)).collect(Collectors.toSet());

            List<ColumnStatistic> lower = Lists.newArrayList();
            List<ColumnStatistic> medium = Lists.newArrayList();
            List<ColumnStatistic> high = Lists.newArrayList();

            List<ColumnStatistic>[] cards = new List[] {lower, medium, high};

            columnStatistics.forEach(
                    s -> cards[PushDownAggregateCollector.groupByCardinality(s, statistics.getOutputRowCount())].add(
                            s));

            LOG.debug("lower: {}, medium: {}, high: {}, isJoinLeftChild: {}",
                    lower.size(), medium.size(), high.size(), context.isJoinLeftChild);

            if (!high.isEmpty()) {
                return false;
            }

            if (groupBys.size() == 2 && !medium.isEmpty()) {
                return false;
            }

            lower.addAll(medium);
            double cartesian = lower.stream().map(ColumnStatistic::getDistinctValuesCount).reduce((a, b) -> a * b)
                    .orElse(Double.MAX_VALUE);

            // only 2 low cardinality column or 1 low cardinality column or 1 medium cardinality column can get there
            return !(cartesian * 5 > statistics.getOutputRowCount());
        }
    }
}
