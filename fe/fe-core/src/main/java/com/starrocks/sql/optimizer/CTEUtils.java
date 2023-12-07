// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;

import java.util.LinkedList;

public class CTEUtils {

    public static void collectCteOperators(OptExpression anchor, OptimizerContext context) {
        context.getCteContext().reset();
        collectCteProduce(anchor, context);
        collectCteConsume(anchor, context);
    }

    /*
     * Estimate the complexity of the produce plan
     * */
    private static void collectCteProduce(OptExpression root, OptimizerContext context) {
        for (OptExpression child : root.getInputs()) {
            collectCteProduce(child, context);
        }

        if (OperatorType.LOGICAL_CTE_PRODUCE.equals(root.getOp().getOpType())) {
            // produce
            LogicalCTEProduceOperator produce = (LogicalCTEProduceOperator) root.getOp();
            context.getCteContext().addCTEProduce(produce.getCteId());
        }
    }

    private static void collectCteConsume(OptExpression root, OptimizerContext context) {
        for (Integer cteId : context.getCteContext().getAllCTEProduce()) {
            OptExpression anchor = findCteAnchor(root, cteId);
            Preconditions.checkNotNull(anchor);
            collectCteConsumeImpl(anchor, cteId, context);
        }
    }

    private static OptExpression findCteAnchor(OptExpression root, Integer cteId) {
        LinkedList<OptExpression> queue = Lists.newLinkedList();
        queue.addLast(root);

        while (!queue.isEmpty()) {
            OptExpression expression = queue.pollFirst();
            if (OperatorType.LOGICAL_CTE_ANCHOR.equals(expression.getOp().getOpType()) &&
                    ((LogicalCTEAnchorOperator) expression.getOp()).getCteId() == cteId) {
                return expression.getInputs().get(1);
            }

            expression.getInputs().forEach(queue::addLast);
        }
        return null;
    }

    private static void collectCteConsumeImpl(OptExpression root, Integer cteId, OptimizerContext context) {
        if (OperatorType.LOGICAL_CTE_CONSUME.equals(root.getOp().getOpType())) {
            if (((LogicalCTEConsumeOperator) root.getOp()).getCteId() != cteId) {
                // not ask children
                return;
            }
            // consumer
            LogicalCTEConsumeOperator consume = (LogicalCTEConsumeOperator) root.getOp();
            context.getCteContext().addCTEConsume(consume.getCteId());
            // not ask children
            return;
        }

        for (OptExpression child : root.getInputs()) {
            collectCteConsumeImpl(child, cteId, context);
        }
    }

    // Force CTEConsume has none children, but JoinReorder need CTEConsume statistics
    public static void collectForceCteStatistics(Memo memo, OptimizerContext context) {
        collectForceCteStatistics(memo.getRootGroup(), context);
    }

    private static void collectForceCteStatistics(Group root, OptimizerContext context) {
        GroupExpression expression = root.getFirstLogicalExpression();

        for (Group group : expression.getInputs()) {
            collectForceCteStatistics(group, context);
        }

        if (OperatorType.LOGICAL_CTE_PRODUCE.equals(expression.getOp().getOpType())) {
            // produce
            LogicalCTEProduceOperator produce = (LogicalCTEProduceOperator) expression.getOp();

            // costs
            OptExpression opt = root.extractLogicalTree();
            calculateStatistics(opt, context);
            context.getCteContext().addCTEStatistics(produce.getCteId(), opt.getStatistics());
        }
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
