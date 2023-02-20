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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.SubqueryUtils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/*
 *          Apply                 Project
 *         /     \                   |
 *     Project    subquery  ==>    Apply
 *       |                         /   \
 *       A                        A     subquery
 *
 */
public class PushDownApplyLeftProjectRule extends TransformationRule {
    public PushDownApplyLeftProjectRule() {
        super(RuleType.TF_PUSH_DOWN_APPLY, Pattern.create(OperatorType.LOGICAL_APPLY)
                .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.PATTERN_LEAF))
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();
        return SubqueryUtils.isUnCorrelationScalarSubquery(apply);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();
        ColumnRefSet outerColumns = apply.getUnCorrelationSubqueryPredicateColumns();

        OptExpression projectOpt = input.getInputs().get(0);

        // find subquery bind child
        if (!projectOpt.getInputs().get(0).getOutputColumns().containsAll(outerColumns)) {
            return Collections.emptyList();
        }

        LogicalProjectOperator project = (LogicalProjectOperator) projectOpt.getOp();

        Map<ColumnRefOperator, ScalarOperator> newMaps = Maps.newHashMap(project.getColumnRefMap());
        newMaps.put(apply.getOutput(), apply.getOutput());

        OptExpression newApply = OptExpression.create(apply, projectOpt.getInputs().get(0), input.getInputs().get(1));
        return Lists.newArrayList(OptExpression.create(new LogicalProjectOperator(newMaps), newApply));
    }
}
