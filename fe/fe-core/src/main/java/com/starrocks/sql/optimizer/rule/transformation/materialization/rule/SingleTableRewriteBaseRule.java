// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization.rule;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.rule.BaseMaterializedViewRewriteRule;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;

import java.util.Collections;
import java.util.List;

/*
 *
 * Here is the rule for pattern Filter - Scan
 * Keep single table rewrite in rule-base phase to reduce the search space
 *
 */
public abstract class SingleTableRewriteBaseRule extends BaseMaterializedViewRewriteRule {
    public SingleTableRewriteBaseRule(RuleType type, Pattern pattern) {
        super(type, pattern);
    }

    // select OptExpression based on statistics
    @Override
    public List<OptExpression> transform(OptExpression queryExpression, OptimizerContext context) {
        List<OptExpression> expressions = super.transform(queryExpression, context);
        if (expressions == null || expressions.isEmpty()) {
            return Lists.newArrayList();
        } else {
            if (expressions.size() == 1) {
                return expressions;
            }
            // compute the statistics of OptExpression
            for (OptExpression expression : expressions) {
                calculateStatistics(expression, context);
            }
            // sort expressions based on statistics output row count
            Collections.sort(expressions, (expression1, expression2) -> {
                if (expression1.getStatistics() == null && expression1.getStatistics() == null) {
                    return 0;
                } else if (expression1.getStatistics() == null) {
                    return -1;
                } else if (expression2.getStatistics() == null) {
                    return 1;
                } else {
                    Statistics statistics1 = expression1.getStatistics();
                    Statistics statistics2 = expression2.getStatistics();
                    return Double.valueOf(statistics1.getOutputRowCount()).compareTo(statistics2.getOutputRowCount());
                }
            });
            return expressions.subList(0, 1);
        }
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
