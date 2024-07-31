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

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DummyStatisticsCalculator extends OptExpressionVisitor<Void, ExpressionContext> {
    private static final Logger LOG = LogManager.getLogger(StatisticsCalculator.class);

    private final ExpressionContext expressionContext;
    private final ColumnRefFactory columnRefFactory;
    private final OptimizerContext optimizerContext;

    public DummyStatisticsCalculator(ExpressionContext expressionContext, ColumnRefFactory columnRefFactory,
                                     OptimizerContext optimizerContext) {
        this.expressionContext = expressionContext;
        this.columnRefFactory = columnRefFactory;
        this.optimizerContext = optimizerContext;
    }

    @Override
    public Void visit(OptExpression optExpression, ExpressionContext context) {

        Statistics.Builder statisticsBuilder = new Statistics.Builder();
        statisticsBuilder.setOutputRowCount(0xdeadbeef);
        statisticsBuilder.setTableRowCountMayInaccurate(true);

        optExpression.getRowOutputInfo().getOutputColRefs().forEach(columnRef -> {
            statisticsBuilder.addColumnStatistic(columnRef, ColumnStatistic.unknown());
        });
        context.setStatistics(statisticsBuilder.build());
        return null;
    }

    public void estimatorStats() {
        expressionContext.getOp().accept(this, expressionContext.getExpression(), expressionContext);
    }
}
