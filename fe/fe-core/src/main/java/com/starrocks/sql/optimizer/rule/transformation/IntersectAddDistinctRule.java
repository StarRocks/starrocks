// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;

import java.util.List;

/*
 *                                    Intersect
 *       Intersect                  /     |     \
 *     /     |     \       -->  Distinct  |   Distinct
 *  Node1   Node2  Node3          /       |       \
 *                             Node1    Node2     Node3
 */
public class IntersectAddDistinctRule extends TransformationRule {
    public IntersectAddDistinctRule() {
        super(RuleType.TF_INTERSECT_DISTINCT, Pattern.create(OperatorType.LOGICAL_INTERSECT).
                addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression intersectOpt, OptimizerContext context) {
        List<OptExpression> newInputs = Lists.newArrayList();
        for (int i = 0; i < intersectOpt.getInputs().size(); i++) {
            newInputs.add(addDistinct(intersectOpt, i, context));
        }

        return Lists.newArrayList(OptExpression.create(intersectOpt.getOp(), newInputs));
    }

    private OptExpression addDistinct(OptExpression intersectOpt, int child,
                                      OptimizerContext context) {
        LogicalIntersectOperator intersect = (LogicalIntersectOperator) intersectOpt.getOp();
        OptExpression childOpt = intersectOpt.inputAt(child);

        LogicalAggregationOperator agg = new LogicalAggregationOperator(AggType.GLOBAL,
                Lists.newArrayList(intersect.getChildOutputColumns().get(child)), Maps.newHashMap());

        OptExpression aggOpt = OptExpression.create(agg, childOpt);
        calculateStatistics(aggOpt, context);
        double originRows = childOpt.getStatistics().getOutputRowCount();
        double aggRows = aggOpt.getStatistics().getOutputRowCount();

        if (aggRows <= 0 || originRows <= 0 || aggRows * 1000 > originRows) {
            return childOpt;
        }
        return aggOpt;
    }

    private void calculateStatistics(OptExpression expr, OptimizerContext context) {
        // Avoid repeated calculate
        if (expr.getStatistics() != null) {
            return;
        }

        for (OptExpression child : expr.getInputs()) {
            calculateStatistics(child, context);
        }

        ExpressionContext expressionContext = new ExpressionContext(expr);
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(
                expressionContext, context.getColumnRefFactory(), context);
        statisticsCalculator.estimatorStats();
        expr.setStatistics(expressionContext.getStatistics());
    }
}
