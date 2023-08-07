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


package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;

import java.util.BitSet;

public class CTEUtils {
    public static void collectCteOperators(OptExpression anchor, OptimizerContext context) {
        context.getCteContext().reset();
        collectCteOperatorsImpl(anchor, context, new BitSet());
    }

    public static void collectCteOperatorsImpl(OptExpression root, OptimizerContext context, BitSet searchConsume) {
        if (OperatorType.LOGICAL_CTE_ANCHOR.equals(root.getOp().getOpType())) {
            // produce
            LogicalCTEAnchorOperator anchor = (LogicalCTEAnchorOperator) root.getOp();
            context.getCteContext().addCTEProduce(anchor.getCteId());

            Preconditions.checkState(root.getInputs().get(0).getOp() instanceof LogicalCTEProduceOperator);
            LogicalCTEProduceOperator produce = (LogicalCTEProduceOperator) root.getInputs().get(0).getOp();
            Preconditions.checkState(produce.getCteId() == anchor.getCteId());

            BitSet left = (BitSet) searchConsume.clone();
            collectCteOperatorsImpl(root.inputAt(0), context, left);

            BitSet right = (BitSet) searchConsume.clone();
            right.set(anchor.getCteId());
            collectCteOperatorsImpl(root.inputAt(1), context, right);
            return;
        }

        if (OperatorType.LOGICAL_CTE_CONSUME.equals(root.getOp().getOpType())) {
            // consumer
            LogicalCTEConsumeOperator consume = (LogicalCTEConsumeOperator) root.getOp();

            if (searchConsume.get(consume.getCteId())) {
                context.getCteContext().addCTEConsume(consume.getCteId());
                searchConsume = new BitSet();
            }

            if (root.arity() > 0) {
                collectCteOperatorsImpl(root.inputAt(0), context, searchConsume);
            }
            return;
        }

        for (OptExpression child : root.getInputs()) {
            collectCteOperatorsImpl(child, context, searchConsume);
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
