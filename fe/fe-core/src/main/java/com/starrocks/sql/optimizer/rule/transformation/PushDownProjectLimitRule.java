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
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

/*
 * e.g.
 *    Project                    Limit(Global)
 *        |                           |
 *   Limit(Global)   =>            Project
 *        |                           |
 *       ...                         ...
 *
 * Execute Project first VS Execute Limit first:
 * 1. Execute Project first to prune columns to reduce network cost, but may add expressions compute cost.
 * 2. Execute Limit first may add network cost because it's will send more columns, but the number of rows
 *    where the expressions compute is small
 *
 */
public class PushDownProjectLimitRule extends TransformationRule {
    public PushDownProjectLimitRule() {
        super(RuleType.TF_PUSH_DOWN_PROJECT_LIMIT, Pattern.create(OperatorType.LOGICAL_PROJECT)
                .addChildren(Pattern.create(OperatorType.LOGICAL_LIMIT, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalLimitOperator limit = (LogicalLimitOperator) input.getInputs().get(0).getOp();
        return limit.isGlobal();
    }

    @Override
    public List<OptExpression> transform(OptExpression project, OptimizerContext context) {
        LogicalLimitOperator limit = (LogicalLimitOperator) project.getInputs().get(0).getOp();
        Preconditions.checkState(!limit.hasOffset());
        LogicalLimitOperator newLimit = LogicalLimitOperator.global(limit.getLimit());
        return Lists.newArrayList(OptExpression.create(newLimit,
                OptExpression.create(project.getOp(), project.getInputs().get(0).getInputs())));
    }
}
