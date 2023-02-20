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
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class PushDownLimitDirectRule extends TransformationRule {
    public static final PushDownLimitDirectRule PROJECT = new PushDownLimitDirectRule(OperatorType.LOGICAL_PROJECT);
    public static final PushDownLimitDirectRule ASSERT_ONE_ROW =
            new PushDownLimitDirectRule(OperatorType.LOGICAL_ASSERT_ONE_ROW);
    public static final PushDownLimitDirectRule CTE_CONSUME =
            new PushDownLimitDirectRule(OperatorType.LOGICAL_CTE_CONSUME);

    public PushDownLimitDirectRule(OperatorType logicalOperatorType) {
        super(RuleType.TF_PUSH_DOWN_LIMIT, Pattern.create(OperatorType.LOGICAL_LIMIT)
                .addChildren(Pattern.create(logicalOperatorType, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalLimitOperator limit = (LogicalLimitOperator) input.getOp();
        return limit.isLocal();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalLimitOperator limit = (LogicalLimitOperator) input.getOp();
        Preconditions.checkState(!limit.hasOffset());
        OptExpression child = input.inputAt(0);
        LogicalOperator logicOperator = (LogicalOperator) child.getOp();

        // set limit
        logicOperator.setLimit(limit.getLimit());

        // push down
        OptExpression nl = new OptExpression(LogicalLimitOperator.local(limit.getLimit()));
        nl.getInputs().addAll(child.getInputs());
        child.getInputs().clear();
        child.getInputs().add(nl);

        return Lists.newArrayList(child);
    }
}
