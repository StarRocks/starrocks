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

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PruneAIProjectColumnsRule extends TransformationRule {
    public PruneAIProjectColumnsRule() {
        super(RuleType.TF_PRUNE_AI_PROJECT_COLUMNS,
                Pattern.create(OperatorType.LOGICAL_AI_PROJECT, OperatorType.PATTERN_LEAF));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAIProjectOperator project = input.getOp().cast();
        ColumnRefSet required = context.getTaskContext().getRequiredColumns();
        Map<ColumnRefOperator, ScalarOperator> slots = new LinkedHashMap<>();
        project.getColumnRefMap().forEach((column, expression) -> {
            if (required.contains(column)) {
                slots.put(column, expression);
            }
        });
        if (slots.isEmpty()) {
            project.getColumnRefMap().entrySet().stream()
                    .filter(entry -> entry.getKey().equals(entry.getValue()))
                    .findFirst().ifPresent(entry -> slots.put(entry.getKey(), entry.getValue()));
        }

        ColumnRefSet dependencies = new ColumnRefSet();
        slots.values().forEach(expression -> dependencies.union(expression.getUsedColumns()));
        Map<ColumnRefOperator, ScalarOperator> common = new LinkedHashMap<>();
        boolean changed;
        do {
            changed = false;
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry :
                    project.getCommonSubOperatorMap().entrySet()) {
                if (dependencies.contains(entry.getKey()) && !common.containsKey(entry.getKey())) {
                    common.put(entry.getKey(), entry.getValue());
                    dependencies.union(entry.getValue().getUsedColumns());
                    changed = true;
                }
            }
        } while (changed);

        required.union(new Projection(slots, common).getUsedColumns());
        if (slots.equals(project.getColumnRefMap()) && common.equals(project.getCommonSubOperatorMap())) {
            return List.of();
        }
        if (slots.values().stream().noneMatch(AiFunctionExtractor::isAICall)) {
            if (slots.isEmpty()) {
                return List.of(input.inputAt(0));
            }
            return List.of(OptExpression.create(new LogicalProjectOperator(slots), input.getInputs()));
        }
        return List.of(OptExpression.create(new LogicalAIProjectOperator(slots, common), input.getInputs()));
    }
}
