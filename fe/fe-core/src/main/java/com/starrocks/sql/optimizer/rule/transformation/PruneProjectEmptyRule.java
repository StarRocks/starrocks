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
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;

public class PruneProjectEmptyRule extends TransformationRule {

    public PruneProjectEmptyRule() {
        super(RuleType.TF_PRUNE_PROJECT_EMPTY,
                Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.LOGICAL_VALUES));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        return ((LogicalValuesOperator) input.getInputs().get(0).getOp()).getRows().isEmpty();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        List<ColumnRefOperator> outputs =
                Lists.newArrayList(((LogicalProjectOperator) input.getOp()).getColumnRefMap().keySet());
        return Lists.newArrayList(OptExpression.create(new LogicalValuesOperator(outputs, Collections.emptyList())));
    }
}
