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

import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import jersey.repackaged.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PruneProjectRule extends TransformationRule {
    public PruneProjectRule() {
        super(RuleType.TF_PRUNE_PROJECT, Pattern.create(OperatorType.LOGICAL_PROJECT)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF, OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        Map<ColumnRefOperator, ScalarOperator> projections = ((LogicalProjectOperator) input.getOp()).getColumnRefMap();

        // avoid prune expression
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projections.entrySet()) {
            if (!entry.getKey().equals(entry.getValue())) {
                return false;
            }
        }

        // For count(*), the child output columns maybe empty, we needn't apply this rule
        LogicalOperator logicalOperator = (LogicalOperator) input.inputAt(0).getOp();
        ColumnRefSet outputColumn = logicalOperator.getOutputColumns(new ExpressionContext(input.inputAt(0)));
        return outputColumn.cardinality() > 0;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        if (((LogicalProjectOperator) input.getOp()).getColumnRefMap().isEmpty()) {
            Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
            LogicalOperator logicalOperator = (LogicalOperator) input.inputAt(0).getOp();

            ColumnRefOperator smallestColumn =
                    logicalOperator.getSmallestColumn(null, context.getColumnRefFactory(), input.inputAt(0));
            if (smallestColumn != null) {
                projectMap.put(smallestColumn, smallestColumn);
                LogicalProjectOperator newProject = new LogicalProjectOperator(projectMap, logicalOperator.getLimit());
                if (!newProject.equals(input.getOp())) {
                    return Lists.newArrayList(OptExpression.create(newProject, input.getInputs()));
                }
            }
        }

        return Collections.emptyList();
    }

}