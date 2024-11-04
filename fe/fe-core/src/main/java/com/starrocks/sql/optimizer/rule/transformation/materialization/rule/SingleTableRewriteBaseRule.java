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


package com.starrocks.sql.optimizer.rule.transformation.materialization.rule;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;

import java.util.Comparator;
import java.util.List;

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
            if (context.isInMemoPhase()) {
                return expressions;
            }
            if (expressions.size() == 1) {
                return expressions;
            }
            // compute the statistics of OptExpression
            for (OptExpression expression : expressions) {
                calculateStatistics(expression, context);
            }
            // sort expressions based on statistics output row count
            expressions.sort(Comparator.comparingDouble(expression -> expression.getStatistics().getOutputRowCount()));
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
