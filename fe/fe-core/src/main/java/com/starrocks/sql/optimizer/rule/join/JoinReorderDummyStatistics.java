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

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.statistics.DummyStatisticsCalculator;

// JoinReorderDummyStatistics is just used to eliminate cross join, it is
// used by AutoMV. The difference between this algorithm and others is it
// just uses DummyStatisticsCalculator to assign ColumnStatistic.unknown()
// to each column of the operator.
public class JoinReorderDummyStatistics extends JoinReorderGreedy {
    public JoinReorderDummyStatistics(OptimizerContext context) {
        super(context);
    }

    @Override
    protected void calculateStatistics(OptExpression expr) {
        // Avoid repeated calculate
        if (expr.getStatistics() != null) {
            return;
        }

        for (OptExpression child : expr.getInputs()) {
            calculateStatistics(child);
        }

        ExpressionContext expressionContext = new ExpressionContext(expr);
        DummyStatisticsCalculator statisticsCalculator = new DummyStatisticsCalculator(
                expressionContext, context.getColumnRefFactory(), context);
        statisticsCalculator.estimatorStats();
        expr.setStatistics(expressionContext.getStatistics());
    }
}
