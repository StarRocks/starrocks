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

package com.starrocks.sql.optimizer.rule.ivm;

import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;

import java.util.List;
import java.util.Map;

public class IvmDeltaProjectRule extends TransformationRule {
    public IvmDeltaProjectRule() {
        super(RuleType.TF_IVM_DELTA_PROJECT,
                Pattern.create(OperatorType.LOGICAL_DELTA)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalDeltaOperator delta = (LogicalDeltaOperator) input.getOp();
        LogicalProjectOperator project = (LogicalProjectOperator) input.inputAt(0).getOp();

        Map<ColumnRefOperator, ScalarOperator> newProjectMap = project.getColumnRefMap();
        ColumnRefOperator actionColumn = delta.getActionColumn();
        if (actionColumn != null) {
            newProjectMap = Maps.newHashMap(newProjectMap);
            newProjectMap.put(actionColumn, actionColumn);
        }

        LogicalProjectOperator newProject = LogicalProjectOperator.builder()
                .withOperator(project)
                .setColumnRefMap(newProjectMap)
                .build();
        LogicalDeltaOperator newDelta = new LogicalDeltaOperator(delta.isRootDelta(), actionColumn);
        OptExpression child = input.inputAt(0).inputAt(0);

        OptExpression rewritten = OptExpression.create(newProject, OptExpression.create(newDelta, child));
        return List.of(rewritten);
    }
}
