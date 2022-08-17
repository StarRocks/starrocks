// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
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

public class CTEUtils {

    public static void collectCteOperators(Memo memo, OptimizerContext context) {
        context.getCteContext().reset();
        collectCteProduce(memo.getRootGroup(), context);
        collectCteConsume(memo.getRootGroup(), context);
    }

    /*
     * Estimate the complexity of the produce plan
     * */
    private static void collectCteProduce(Group root, OptimizerContext context) {
        GroupExpression expression = root.getFirstLogicalExpression();

        for (Group group : expression.getInputs()) {
            collectCteProduce(group, context);
        }

        if (OperatorType.LOGICAL_CTE_PRODUCE.equals(expression.getOp().getOpType())) {
            // produce
            LogicalCTEProduceOperator produce = (LogicalCTEProduceOperator) expression.getOp();
            context.getCteContext().addCTEProduce(produce.getCteId());
        }
    }

    private static void collectCteConsume(Group root, OptimizerContext context) {
        for (Integer cteId : context.getCteContext().getAllCTEProduce()) {
            Group anchor = findCteAnchor(root, cteId);
            Preconditions.checkNotNull(anchor);
            collectCteConsumeImpl(anchor, cteId, context);
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

    private static void collectCteConsumeImpl(Group root, Integer cteId, OptimizerContext context) {
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

            // not ask children
            return;
        }

        for (Group group : expression.getInputs()) {
            collectCteConsumeImpl(group, cteId, context);
        }
    }

    // Force CTEConsume has none children, but JoinReorder need CTEConsume statistics
    public static void collectForceCteStatistics(Memo memo, OptimizerContext context) {
        collectForceCteStatistics(memo.getRootGroup(), context);
    }

    public static void collectForceCteStatistics(Group root, OptimizerContext context) {
        GroupExpression expression = root.getFirstLogicalExpression();

        for (Group group : expression.getInputs()) {
            collectCteProduce(group, context);
        }

        if (OperatorType.LOGICAL_CTE_PRODUCE.equals(expression.getOp().getOpType())) {
            // produce
            LogicalCTEProduceOperator produce = (LogicalCTEProduceOperator) expression.getOp();

            // costs
            if (context.getCteContext().isForceCTE(produce.getCteId())) {
                OptExpression opt = root.extractLogicalTree();
                calculateStatistics(opt, context);
                context.getCteContext().addCTEStatistics(produce.getCteId(), opt.getStatistics());
            }
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
