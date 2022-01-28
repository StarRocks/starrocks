// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer;

import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;

public class CTEUtils {

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
    }

    public static void collectCteOperatorsWithoutCosts(Memo memo, OptimizerContext context) {
        context.getCteContext().reset();
        collectCteProduce(memo.getRootGroup(), context, false);
        collectCteConsume(memo.getRootGroup(), context, false);
    }

    private static void collectCteProduce(Group root, OptimizerContext context, boolean collectCosts) {
        GroupExpression expression = root.getFirstLogicalExpression();

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

        for (Group group : expression.getInputs()) {
            collectCteProduce(group, context, collectCosts);
        }
    }

    private static void collectCteConsume(Group root, OptimizerContext context, boolean collectCosts) {
        GroupExpression expression = root.getFirstLogicalExpression();

        if (OperatorType.LOGICAL_CTE_CONSUME.equals(expression.getOp().getOpType())) {
            // consumer
            LogicalCTEConsumeOperator consume = (LogicalCTEConsumeOperator) expression.getOp();
            context.getCteContext().addCTEConsume(consume.getCteId());

            // required columns
            ColumnRefSet requiredColumnRef =
                    context.getCteContext().getRequiredColumns().getOrDefault(consume.getCteId(), new ColumnRefSet());
            consume.getCteOutputColumnRefMap().values().forEach(requiredColumnRef::union);
            context.getCteContext().getRequiredColumns().put(consume.getCteId(), requiredColumnRef);

            // inline costs
            if (collectCosts) {
                context.getCteContext().addCTEConsumeInlineCost(consume.getCteId(),
                        calculateStatistics(expression.getInputs().get(0), context).getComputeSize());
            }

            // not ask children
            return;
        }

        for (Group group : expression.getInputs()) {
            collectCteConsume(group, context, collectCosts);
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
        expr.setStatistics(expressionContext.getStatistics());
    }
}
