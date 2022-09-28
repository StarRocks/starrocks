// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.tree.pdagg;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.ExpressionStatisticCalculator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;
import com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient;
import com.starrocks.sql.optimizer.task.TaskContext;
import org.apache.commons.collections4.CollectionUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * Collect all can be push down aggregate context, to get which aggregation can be
 * pushed down and the push down path.
 *
 * Can't rewrite directly, because we don't know which aggregation needs to be
 * push down before arrive at scan node.
 *
 * And in this phase, the key of AggregateContext's map is origin aggregate column, for
 * mark push down which aggregation, the value will multi-rewrite by path, for check
 * which aggregation needs push down
 */
class PushDownAggregateCollector extends OptExpressionVisitor<Void, AggregateContext> {
    private static final List<String> WHITE_FNS = ImmutableList.of(FunctionSet.MAX, FunctionSet.MIN,
            FunctionSet.SUM, FunctionSet.AVG, FunctionSet.HLL_UNION, FunctionSet.BITMAP_UNION,
            FunctionSet.PERCENTILE_UNION);

    private final TaskContext taskContext;
    private final OptimizerContext optimizerContext;
    private final ColumnRefFactory factory;
    private final SessionVariable sessionVariable;

    private final Map<LogicalAggregationOperator, List<AggregateContext>> allRewriteContext = Maps.newHashMap();
    // private final Map<Integer, List<AggregateContext>> cteRewriteContexts = Maps.newHashMap();

    public PushDownAggregateCollector(TaskContext taskContext) {
        this.taskContext = taskContext;
        optimizerContext = taskContext.getOptimizerContext();
        factory = taskContext.getOptimizerContext().getColumnRefFactory();
        sessionVariable = taskContext.getOptimizerContext().getSessionVariable();
    }

    Map<LogicalAggregationOperator, List<AggregateContext>> getAllRewriteContext() {
        return allRewriteContext;
    }

    public void collect(OptExpression root) {
        process(root, new AggregateContext());
    }

    @Override
    public Void visit(OptExpression optExpression, AggregateContext context) {
        // forbidden push down
        for (OptExpression input : optExpression.getInputs()) {
            process(input, AggregateContext.EMPTY);
        }
        return null;
    }

    private Void processChild(OptExpression optExpression, AggregateContext context) {
        for (OptExpression input : optExpression.getInputs()) {
            process(input, context);
        }
        return null;
    }

    private void process(OptExpression opt, AggregateContext context) {
        opt.getOp().accept(this, opt, context);
    }

    private boolean isInvalid(OptExpression optExpression, AggregateContext context) {
        return context.isEmpty() || optExpression.getOp().hasLimit();
    }

    @Override
    public Void visitLogicalFilter(OptExpression optExpression, AggregateContext context) {
        if (isInvalid(optExpression, context)) {
            return visit(optExpression, context);
        }

        // add filter columns in groupBys
        LogicalFilterOperator filter = (LogicalFilterOperator) optExpression.getOp();
        filter.getRequiredChildInputColumns().getStream().mapToObj(factory::getColumnRef)
                .forEach(v -> context.groupBys.put(v, v));
        return processChild(optExpression, context);
    }

    @Override
    public Void visitLogicalProject(OptExpression optExpression, AggregateContext context) {
        if (isInvalid(optExpression, context)) {
            return visit(optExpression, context);
        }

        LogicalProjectOperator project = (LogicalProjectOperator) optExpression.getOp();

        if (project.getColumnRefMap().values().stream().allMatch(ScalarOperator::isColumnRef)) {
            return processChild(optExpression, context);
        }

        // rewrite
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(project.getColumnRefMap());
        context.aggregations.replaceAll((k, v) -> (CallOperator) rewriter.rewrite(v));
        context.groupBys.replaceAll((k, v) -> rewriter.rewrite(v));

        // handle specials functions case-when/if
        // split to groupBys and mock new aggregations by values, don't need to save
        // origin predicate, we just do check in collect phase
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : context.aggregations.entrySet()) {
            CallOperator aggFn = entry.getValue();
            ScalarOperator aggInput = aggFn.getChild(0);

            if (!(aggInput instanceof CallOperator)) {
                continue;
            }

            CallOperator callInput = (CallOperator) aggInput;
            if (aggInput instanceof CaseWhenOperator) {
                CaseWhenOperator caseWhen = (CaseWhenOperator) aggInput;
                for (ScalarOperator condition : caseWhen.getAllConditionClause()) {
                    condition.getUsedColumns().getStream().mapToObj(factory::getColumnRef)
                            .forEach(v -> context.groupBys.put(v, v));
                }

                List<ScalarOperator> newWhenThen = Lists.newArrayList();
                for (int i = 0; i < caseWhen.getWhenClauseSize(); i++) {
                    if (caseWhen.getThenClause(i).isConstant() && !caseWhen.getThenClause(i).isConstantNull()) {
                        // forbidden push down
                        return visit(optExpression, context);
                    }
                    newWhenThen.add(ConstantOperator.createBoolean(false));
                    newWhenThen.add(caseWhen.getThenClause(i));
                }

                // mock just value case when
                CaseWhenOperator newCaseWhen = new CaseWhenOperator(caseWhen.getType(), null,
                        caseWhen.hasElse() ? caseWhen.getElseClause() : null, newWhenThen);

                // replace origin
                aggFn.setChild(0, newCaseWhen);
            } else if (callInput.getFunction() != null &&
                    FunctionSet.IF.equals(callInput.getFunction().getFunctionName().getFunction())) {
                if (aggInput.getChildren().stream().skip(1).anyMatch(c -> c.isConstant() && !c.isConstantNull())) {
                    // forbidden push down
                    return visit(optExpression, context);
                }

                aggInput.getChild(0).getUsedColumns().getStream().mapToObj(factory::getColumnRef)
                        .forEach(v -> context.groupBys.put(v, v));
                aggInput.setChild(0, ConstantOperator.createBoolean(false));
            }
        }

        // check has constant aggregate, forbidden
        if (context.aggregations.values().stream().anyMatch(ScalarOperator::isConstant)) {
            return visit(optExpression, context);
        }

        return processChild(optExpression, context);
    }

    @Override
    public Void visitLogicalAggregate(OptExpression optExpression, AggregateContext context) {
        LogicalAggregationOperator aggregate = (LogicalAggregationOperator) optExpression.getOp();
        // distinct/count*/constant aggregate can't push down
        if (aggregate.getAggregations().values().stream()
                .anyMatch(c -> c.isDistinct() || c.isCountStar() || c.isConstant())) {
            return visit(optExpression, context);
        }

        context = new AggregateContext();
        context.setAggregator(aggregate);
        return processChild(optExpression, context);
    }

    @Override
    public Void visitLogicalJoin(OptExpression optExpression, AggregateContext context) {
        if (isInvalid(optExpression, context)) {
            return visit(optExpression, context);
        }
        LogicalJoinOperator join = (LogicalJoinOperator) optExpression.getOp();
        // constant aggregate can't push down
        if (context.aggregations.values().stream().anyMatch(ScalarOperator::isConstant)) {
            return visit(optExpression, context);
        }

        if (join.getOnPredicate() != null) {
            join.getOnPredicate().getUsedColumns().getStream().mapToObj(factory::getColumnRef)
                    .forEach(c -> context.groupBys.put(c, c));
        }

        if (join.getPredicate() != null) {
            join.getPredicate().getUsedColumns().getStream().mapToObj(factory::getColumnRef)
                    .forEach(v -> context.groupBys.put(v, v));
        }

        // split aggregate to left/right child
        AggregateContext leftContext = splitJoinAggregate(optExpression, context, 0);
        AggregateContext rightContext = splitJoinAggregate(optExpression, context, 1);
        process(optExpression.inputAt(0), leftContext);
        process(optExpression.inputAt(1), rightContext);
        return null;
    }

    private AggregateContext splitJoinAggregate(OptExpression optExpression, AggregateContext context, int child) {
        ColumnRefSet childOutput = optExpression.getChildOutputColumns(child);
        AggregateContext childContext = new AggregateContext();

        // split group by
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : context.groupBys.entrySet()) {
            ColumnRefSet groupByUseColumns = entry.getValue().getUsedColumns();
            if (childOutput.containsAll(groupByUseColumns)) {
                childContext.groupBys.put(entry.getKey(), entry.getValue());
            } else if (childOutput.isIntersect(groupByUseColumns)) {
                // e.g. group by abs(a + b), we can derive group by a
                Map<ColumnRefOperator, ScalarOperator> rewriteMap = groupByUseColumns.getStream()
                        .filter(c -> !childOutput.contains(c)).mapToObj(factory::getColumnRef)
                        .collect(Collectors.toMap(k -> k, k -> ConstantOperator.createNull(k.getType())));
                ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(rewriteMap);
                childContext.groupBys.put(entry.getKey(), rewriter.rewrite(entry.getValue()));
            }
        }

        // split aggregations
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : context.aggregations.entrySet()) {
            ColumnRefSet aggUsedColumns = entry.getValue().getUsedColumns();
            if (childOutput.containsAll(aggUsedColumns)) {
                childContext.aggregations.put(entry.getKey(), entry.getValue());
            } else if (childOutput.isIntersect(aggUsedColumns)) {
                // e.g. MAX(A + B) can't push down
                return AggregateContext.EMPTY;
            }
        }

        childContext.origAggregator = context.origAggregator;
        childContext.pushPaths.addAll(context.pushPaths);
        childContext.pushPaths.add(child);
        return childContext;
    }

    @Override
    public Void visitLogicalUnion(OptExpression optExpression, AggregateContext context) {
        if (isInvalid(optExpression, context)) {
            return visit(optExpression, context);
        }

        List<PushDownAggregateCollector> collectors = Lists.newArrayList();
        LogicalUnionOperator union = (LogicalUnionOperator) optExpression.getOp();
        for (int i = 0; i < optExpression.getInputs().size(); i++) {
            List<ColumnRefOperator> childOutput = union.getChildOutputColumns().get(i);
            Map<ColumnRefOperator, ScalarOperator> rewriteMap = Maps.newHashMap();
            Preconditions.checkState(childOutput.size() == union.getOutputColumnRefOp().size());
            for (int k = 0; k < union.getOutputColumnRefOp().size(); k++) {
                rewriteMap.put(union.getOutputColumnRefOp().get(k), childOutput.get(k));
            }

            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(rewriteMap);
            AggregateContext childContext = new AggregateContext();
            childContext.origAggregator = context.origAggregator;
            childContext.aggregations.putAll(context.aggregations);
            childContext.aggregations.replaceAll((k, v) -> (CallOperator) rewriter.rewrite(v));

            childContext.groupBys.putAll(context.groupBys);
            childContext.groupBys.replaceAll((k, v) -> rewriter.rewrite(v));
            childContext.pushPaths.addAll(context.pushPaths);
            childContext.pushPaths.add(i);

            PushDownAggregateCollector collector = new PushDownAggregateCollector(this.taskContext);
            collectors.add(collector);
            collector.process(optExpression.inputAt(i), childContext);
        }

        // collect push down aggregate context
        List<List<AggregateContext>> allChildRewriteContext = Lists.newArrayList();
        for (PushDownAggregateCollector childCollector : collectors) {
            if (childCollector.allRewriteContext.containsKey(context.origAggregator)) {
                allChildRewriteContext.add(childCollector.allRewriteContext.get(context.origAggregator));
                childCollector.allRewriteContext.remove(context.origAggregator);
            }
        }

        // merge other rewrite context
        for (PushDownAggregateCollector collector : collectors) {
            Preconditions.checkState(
                    !CollectionUtils.containsAny(allRewriteContext.keySet(), collector.allRewriteContext.keySet()));
            allRewriteContext.putAll(collector.allRewriteContext);
        }

        // none aggregate can push down to union children
        if (allChildRewriteContext.isEmpty() || allChildRewriteContext.size() != collectors.size()) {
            return null;
        }

        Set<ColumnRefOperator> checkGroupBys = new HashSet<>(context.groupBys.keySet());
        Set<ColumnRefOperator> checkAggregations = new HashSet<>(context.aggregations.keySet());

        for (List<AggregateContext> childContexts : allChildRewriteContext) {
            Set<ColumnRefOperator> cg = new HashSet<>();
            Set<ColumnRefOperator> ca = new HashSet<>();

            childContexts.forEach(c -> {
                cg.addAll(c.groupBys.keySet());
                ca.addAll(c.aggregations.keySet());
            });

            // Must all same, like Agg1, Agg2 split by Union, and Scan1 support Agg1/Agg2, and
            // Scan2 support Agg1, Scan3 support Agg2, we must promise to either push down or
            // not push down
            // like:
            //         UNION
            //        /      \
            //     Scan1    Join
            //             /    \
            //         Scan2    Scan3
            if (!cg.containsAll(checkGroupBys) || !ca.containsAll(checkAggregations)) {
                return null;
            }
        }

        List<AggregateContext> list = allRewriteContext.getOrDefault(context.origAggregator, Lists.newArrayList());
        allChildRewriteContext.forEach(list::addAll);
        allRewriteContext.put(context.origAggregator, list);
        return null;
    }

    @Override
    public Void visitLogicalCTEAnchor(OptExpression optExpression, AggregateContext context) {
        process(optExpression.inputAt(1), context);
        process(optExpression.inputAt(0), AggregateContext.EMPTY);
        return null;
    }

    /* @Todo: CTE push down need right column prune
    @Override
    public Void visitLogicalCTEProduce(OptExpression optExpression, AggregateContext context) {
        LogicalCTEProduceOperator produce = (LogicalCTEProduceOperator) optExpression.getOp();

        List<AggregateContext> contexts = cteRewriteContexts.getOrDefault(produce.getCteId(), Collections.emptyList());
        int consumeNum = optimizerContext.getCteContext().getCTEConsumeNum(produce.getCteId());

        if (contexts.size() != consumeNum || contexts.stream().anyMatch(AggregateContext::isEmpty)) {
            return visit(optExpression, context);
        }

        for (AggregateContext childContext : contexts) {
            visitChild(optExpression, childContext);
        }

        // must all push down
        if (!contexts.stream().allMatch(c -> allRewriteContext.containsKey(c.origAggregator))) {
            contexts.forEach(c -> allRewriteContext.remove(c.origAggregator));
        }
        return null;
    }

    @Override
    public Void visitLogicalCTEConsume(OptExpression optExpression, AggregateContext context) {
        if (isInvalid(optExpression, context)) {
            return visit(optExpression, context);
        }

        // derive children's aggregation
        visit(optExpression, AggregateContext.EMPTY);

        // add cte produce aggregate context
        LogicalCTEConsumeOperator consume = (LogicalCTEConsumeOperator) optExpression.getOp();
        consume.getPredicate().getUsedColumns().getStream().mapToObj(factory::getColumnRef)
                .forEach(v -> context.groupBys.put(v, v));

        ReplaceColumnRefRewriter rewriter =
                new ReplaceColumnRefRewriter(Maps.newHashMap(consume.getCteOutputColumnRefMap()));

        context.aggregations.replaceAll((k, v) -> (CallOperator) rewriter.rewrite(v));
        context.groupBys.replaceAll((k, v) -> rewriter.rewrite(v));
        context.pushPaths.add(0);

        List<AggregateContext> list = cteRewriteContexts.getOrDefault(consume.getCteId(), Lists.newArrayList());
        list.add(context);
        cteRewriteContexts.put(consume.getCteId(), list);
        return null;
    }
    */

    @Override
    public Void visitLogicalTableScan(OptExpression optExpression, AggregateContext context) {
        // least cross join/union/cte
        if (context.isEmpty() || context.pushPaths.isEmpty()) {
            return null;
        }

        if (context.aggregations.isEmpty() && context.groupBys.isEmpty()) {
            return null;
        }

        // distinct function, not support function can't push down
        if (context.aggregations.values().stream()
                .anyMatch(v -> v.isDistinct() || !WHITE_FNS.contains(v.getFnName()))) {
            return null;
        }

        LogicalScanOperator scan = (LogicalScanOperator) optExpression.getOp();
        ColumnRefSet scanOutput = new ColumnRefSet(scan.getOutputColumns());

        ColumnRefSet allGroupByColumns = new ColumnRefSet();
        context.groupBys.values().forEach(c -> allGroupByColumns.union(c.getUsedColumns()));

        ColumnRefSet allAggregateColumns = new ColumnRefSet();
        context.aggregations.values().forEach(c -> allAggregateColumns.union(c.getUsedColumns()));

        Preconditions.checkState(scanOutput.containsAll(allGroupByColumns));
        Preconditions.checkState(scanOutput.containsAll(allAggregateColumns));

        ExpressionContext expressionContext = new ExpressionContext(optExpression);
        StatisticsCalculator statisticsCalculator =
                new StatisticsCalculator(expressionContext, factory, optimizerContext);
        statisticsCalculator.estimatorStats();

        if (!checkStatistics(allGroupByColumns, expressionContext.getStatistics())) {
            return null;
        }

        List<AggregateContext> list = allRewriteContext.getOrDefault(context.origAggregator, Lists.newArrayList());
        list.add(context);
        allRewriteContext.put(context.origAggregator, list);
        return null;
    }

    private boolean checkStatistics(ColumnRefSet groupBys, Statistics statistics) {
        // check force push down flag
        // flag 0: auto. 1: force push down. -1: don't push down. 2: push down medium. 3: push down high
        if (sessionVariable.getCboPushDownAggregateMode() == 1) {
            return true;
        }

        if (sessionVariable.getCboPushDownAggregateMode() == -1) {
            return false;
        }

        List<ColumnStatistic> lower = Lists.newArrayList();
        List<ColumnStatistic> medium = Lists.newArrayList();
        List<ColumnStatistic> high = Lists.newArrayList();

        List<ColumnStatistic>[] cards = new List[] {lower, medium, high};

        groupBys.getStream().mapToObj(factory::getColumnRef)
                .map(s -> ExpressionStatisticCalculator.calculate(s, statistics))
                .forEach(s -> cards[groupByCardinality(s, statistics.getOutputRowCount())].add(s));

        double lowerCartesian = lower.stream().map(ColumnStatistic::getDistinctValuesCount).reduce((a, b) -> a * b)
                .orElse(Double.MAX_VALUE);

        // pow(row_count/20, a half of lower column size)
        double lowerUpper = Math.max(statistics.getOutputRowCount() / 20, 1);
        lowerUpper = Math.pow(lowerUpper, Math.max(lower.size() / 2, 1));

        // 1. white push down rules
        // 1.1 only one lower/medium cardinality columns
        if (high.isEmpty() && (lower.size() + medium.size()) == 1) {
            return true;
        }

        // 1.2 the cartesian of all lower/count <= 1
        // 1.3 the lower cardinality <= 3 and lowerCartesian < lowerUpper
        // 1.4 follow medium cardinality flag
        if (high.isEmpty() && medium.isEmpty()) {
            if (lowerCartesian <= statistics.getOutputRowCount() || lower.size() <= 2) {
                return true;
            } else if (lower.size() <= 4 && lowerCartesian < lowerUpper) {
                return true;
            } else {
                return sessionVariable.getCboPushDownAggregateMode() >= 2;
            }
        }

        // 2. forbidden rules
        // 2.1 high cardinality >= 2
        // 2.2 medium cardinality > 2
        // 2.3 high cardinality = 1 and medium cardinality > 0
        if (high.size() >= 2 || medium.size() > 2 || (high.size() == 1 && !medium.isEmpty())) {
            return false;
        }

        // 3. high cardinality < 2 and lower cardinality <= 3
        if (high.size() == 1 && lower.size() <= 3) {
            return sessionVariable.getCboPushDownAggregateMode() >= 3;
        }

        // 4. medium cardinality <= 2
        if (lower.size() <= 3) {
            if (sessionVariable.getCboPushDownAggregateMode() >= 2) {
                return true;
            }
            return statistics.getOutputRowCount() >=
                    StatisticsEstimateCoefficient.DEFAULT_PUSH_DOWN_AGGREGATE_ROWS_LIMIT;
        }

        return false;
    }

    // high(2): cardinality/count > 1/5
    // medium(1): cardinality/count < 1/5 and > 1/20
    // lower(0): cardinality/count < 1/20
    private int groupByCardinality(ColumnStatistic statistic, double rowCount) {
        if (statistic.isUnknown()) {
            return 2;
        }

        double distinct = statistic.getDistinctValuesCount();

        if (rowCount == 0 || distinct * 5 >= rowCount) {
            return 2;
        } else if (distinct * 5 < rowCount && distinct * 20 >= rowCount) {
            return 1;
        } else if (distinct * 20 <= rowCount) {
            return 0;
        }

        return 2;
    }
}
