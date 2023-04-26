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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class ReorderIntersectRule extends TransformationRule {
    public ReorderIntersectRule() {
        super(RuleType.TF_INTERSECT_REORDER, Pattern.create(OperatorType.LOGICAL_INTERSECT).
                addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression intersectOpt, OptimizerContext context) {
        LogicalIntersectOperator intersectOperator = (LogicalIntersectOperator) intersectOpt.getOp();
        calculateStatistics(intersectOpt, context);
        Optional<OptExpression> optO = intersectOpt.getInputs().stream().min(
                Comparator.comparingDouble(c -> c.getStatistics().getOutputRowCount()));
        Preconditions.checkState(optO.isPresent());
        OptExpression o = optO.get();
        int index = intersectOpt.getInputs().indexOf(o);

        List<OptExpression> newChildList = new ArrayList<>();
        List<List<ColumnRefOperator>> childOutputColumns = new ArrayList<>();
        newChildList.add(intersectOpt.getInputs().get(index));
        childOutputColumns.add(intersectOperator.getChildOutputColumns().get(index));

        for (int idx = 0; idx < intersectOpt.arity(); ++idx) {
            OptExpression child = intersectOpt.inputAt(idx);
            if (!child.equals(o)) {
                newChildList.add(child);
                childOutputColumns.add(intersectOperator.getChildOutputColumns().get(idx));
            }
        }
        return Lists.newArrayList(OptExpression.create(
                new LogicalIntersectOperator.Builder().withOperator(intersectOperator)
                        .setChildOutputColumns(childOutputColumns).build(), newChildList));
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
