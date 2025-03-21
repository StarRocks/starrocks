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
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.join.ReorderJoinRule;

import java.util.List;

public class InnerToSemiRule extends TransformationRule {

    public InnerToSemiRule() {
        super(RuleType.TF_INNER_TO_SEMI, Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_MULTIJOIN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator logicalAggregationOperator = (LogicalAggregationOperator) input.getOp();
        if (logicalAggregationOperator.getAggregations() != null &&
                !logicalAggregationOperator.getAggregations().isEmpty()) {
            return Lists.newArrayList(input);
        }

        ColumnRefSet columnRefSet = input.getChildOutputColumns(0);
        ColumnRefSet back = context.getTaskContext().getRequiredColumns();
        context.getTaskContext().setRequiredColumns(columnRefSet);

        OptExpression newChild = new ReorderJoinRule().rewrite_for_distinct_join(input.inputAt(0), context);
        input.setChild(0, newChild);
        context.getTaskContext().setRequiredColumns(back);
        return Lists.newArrayList(input);
    }
}
