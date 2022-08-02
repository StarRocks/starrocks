// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CTEUtils {
    /*
     * The score refers to the complexity of the operator and the probability of CTE reuse.
     * The higher the score, the greater the probability of CTE reuse
     */
    private static final Map<OperatorType, Integer> OPERATOR_SCORES = ImmutableMap.<OperatorType, Integer>builder()
            .put(OperatorType.LOGICAL_JOIN, 4)
            .put(OperatorType.LOGICAL_AGGR, 6)
            .put(OperatorType.LOGICAL_OLAP_SCAN, 4)
            .put(OperatorType.LOGICAL_UNION, 4)
            .put(OperatorType.LOGICAL_CTE_PRODUCE, 0)
            .build();

    /*
     * Collect CTE operators info, why don't use TransformRule to search?
     *
     * like SQL:
     *  with cte1 as (select * from A), cte2 as (select * from cte1) select * from cte2 union all select * from cte2;
     * Now plan tree which contains CTE:
     *                      Anchor1
     *                    /         \
     *              Produce1        Anchor2
     *                /           /         \
     *             ScanA     Produce2        UNION
     *                         /            /      \
     *                      Consume1  Consume2    Consume2
     *                       /           |           |
     *                     ScanA      Consume1    Consume1
     *                                   |           |
     *                                 ScanA        ScanA
     *
     * Based on the plan structure, there are such advantages:
     * 1. Can push down consume predicate/limit for CTEs with forward dependencies, else need deduction
     *    of predicate and limit in a loop
     * 2. Inline CTE easy, don't need consider column-ref rewrite, call logical rule rewrite after inline
     *
     * But there are also questions:
     * 1. Collect CTE operator is inaccurate, like the demo, the Consume1 only use once in `cte2`,
     *    but it's appear there in the plan tree, it's not friendly to InlineRule check
     * 2. CTE consume cost compute is inaccurate.
     *
     * So we need collect CTE operators info by plan tree, avoid collect repeat consume operator
     *
     * @Todo: move CTE inline into memo optimize phase
     * */
    public static void collectCteOperators(Memo memo, OptimizerContext context) {
        context.getCteContext().reset();
        collectCteProduce(memo.getRootGroup(), context, true);
        collectCteConsume(memo.getRootGroup(), context, true);
        collectCteProduceEstimate(context);
    }

    public static void collectCteOperatorsWithoutCosts(Memo memo, OptimizerContext context) {
        context.getCteContext().reset();
        collectCteProduce(memo.getRootGroup(), context, false);
        collectCteConsume(memo.getRootGroup(), context, false);
    }

    private static void collectCteProduceEstimate(OptimizerContext context) {
        CTEContext cteContext = context.getCteContext();
        for (Map.Entry<Integer, OptExpression> entry : cteContext.getAllCTEProduce().entrySet()) {
            AtomicInteger scores = new AtomicInteger(0);
            cteProduceEstimate(entry.getValue().getGroupExpression().getGroup(), scores);

            // score 15 meanings (1 join + 2 scan + 3 project), (1 agg + 1 scan + 2 project)
            // A lower score will make a higher penalty factor
            cteContext.addCTEProduceComplexityScores(entry.getKey(), 15.0 / scores.intValue());
        }
    }

    /*
     * Estimate the complexity of the produce plan
     * */
    private static void cteProduceEstimate(Group root, AtomicInteger scores) {
        GroupExpression expression = root.getFirstLogicalExpression();
        scores.addAndGet(OPERATOR_SCORES.getOrDefault(expression.getOp().getOpType(), 1));

        if (OperatorType.LOGICAL_CTE_CONSUME.equals(expression.getOp().getOpType())) {
            // don't ask consume children
            return;
        }

        for (Group group : expression.getInputs()) {
            cteProduceEstimate(group, scores);
        }
    }

    private static void collectCteProduce(Group root, OptimizerContext context, boolean collectCosts) {
        GroupExpression expression = root.getFirstLogicalExpression();

        for (Group group : expression.getInputs()) {
            collectCteProduce(group, context, collectCosts);
        }

        if (OperatorType.LOGICAL_CTE_PRODUCE.equals(expression.getOp().getOpType())) {
            // produce
            LogicalCTEProduceOperator produce = (LogicalCTEProduceOperator) expression.getOp();
            OptExpression opt = OptExpression.create(produce);
            opt.attachGroupExpression(expression);
            context.getCteContext().addCTEProduce(produce.getCteId(), opt);

            // costs
            if (collectCosts) {
                Statistics statistics = calculateStatistics(root, context);
                opt.setStatistics(statistics);
                context.getCteContext().addCTEProduceCost(produce.getCteId(), statistics.getComputeSize());
            }
        }
    }

    private static void collectCteConsume(Group root, OptimizerContext context, boolean collectCosts) {
        for (Integer cteId : context.getCteContext().getAllCTEProduce().keySet()) {
            Group anchor = findCteAnchor(root, cteId);
            Preconditions.checkNotNull(anchor);
            collectCteConsumeImpl(anchor, cteId, context, collectCosts);
        }
    }

    private static Group findCteAnchor(Group root, Integer cteId) {
        LinkedList<Group> queue = Lists.newLinkedList();
        queue.addLast(root);

        while (!queue.isEmpty()) {
            Group group = queue.pollFirst();
            GroupExpression expression = group.getFirstLogicalExpression();
            if (OperatorType.LOGICAL_CTE_ANCHOR.equals(expression.getOp().getOpType()) &&
                    ((LogicalCTEAnchorOperator) expression.getOp()).getCteId() == cteId) {
                return group.getFirstLogicalExpression().inputAt(1);
            }

            expression.getInputs().forEach(queue::addLast);
        }
        return null;
    }

    private static void collectCteConsumeImpl(Group root, Integer cteId, OptimizerContext context,
                                              boolean collectCosts) {
        GroupExpression expression = root.getFirstLogicalExpression();

        if (OperatorType.LOGICAL_CTE_CONSUME.equals(expression.getOp().getOpType())) {
            if (((LogicalCTEConsumeOperator) expression.getOp()).getCteId() != cteId) {
                // not ask children
                return;
            }
            // consumer
            LogicalCTEConsumeOperator consume = (LogicalCTEConsumeOperator) expression.getOp();
            context.getCteContext().addCTEConsume(consume.getCteId());

            // required columns
            ColumnRefSet requiredColumnRef =
                    context.getCteContext().getRequiredColumns().getOrDefault(consume.getCteId(), new ColumnRefSet());
            consume.getCteOutputColumnRefMap().values().forEach(requiredColumnRef::union);
            context.getCteContext().getRequiredColumns().put(consume.getCteId(), requiredColumnRef);

            // inline costs
            if (collectCosts && !expression.getInputs().isEmpty()) {
                context.getCteContext().addCTEConsumeInlineCost(consume.getCteId(),
                        calculateStatistics(expression.getInputs().get(0), context).getComputeSize());
            }

            // not ask children
            return;
        }

        for (Group group : expression.getInputs()) {
            collectCteConsumeImpl(group, cteId, context, collectCosts);
        }
    }

    private static Statistics calculateStatistics(Group root, OptimizerContext context) {
        OptExpression opt = root.extractLogicalTree();

        /*
         * Because logical operator can't compute costs in rewrite phase now, so
         * there temporary use rows size
         *
         * */
        calculateStatistics(opt, context);
        return opt.getStatistics();
    }

    private static void calculateStatistics(OptExpression expr, OptimizerContext context) {
        // don't ask cte consume children
        if (expr.getOp().getOpType() != OperatorType.LOGICAL_CTE_CONSUME) {
            for (OptExpression child : expr.getInputs()) {
                calculateStatistics(child, context);
            }
        }

        ExpressionContext expressionContext = new ExpressionContext(expr);
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(
                expressionContext, context.getColumnRefFactory(), context);
        statisticsCalculator.estimatorStats();

        if (OperatorType.LOGICAL_OLAP_SCAN.equals(expr.getOp().getOpType()) &&
                expressionContext.getStatistics().getColumnStatistics().values().stream()
                        .anyMatch(ColumnStatistic::isUnknown)) {
            // Can't evaluated the effect of CTE if don't know statistic,
            // Mark output rows is zero, will choose inline when CTEContext check output rows
            expr.setStatistics(Statistics.buildFrom(expressionContext.getStatistics()).setOutputRowCount(0).build());
        } else {
            expr.setStatistics(expressionContext.getStatistics());
        }
    }
}
