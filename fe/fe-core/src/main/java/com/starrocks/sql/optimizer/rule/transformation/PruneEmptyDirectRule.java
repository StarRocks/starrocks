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
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class PruneEmptyDirectRule extends TransformationRule {
    public PruneEmptyDirectRule() {
        super(RuleType.TF_PRUNE_EMPTY_DIRECT,
                Pattern.create(OperatorType.PATTERN_LEAF).addChildren(Pattern.create(OperatorType.LOGICAL_VALUES)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (OperatorType.LOGICAL_CTE_PRODUCE.equals(input.getOp().getOpType()) ||
                OperatorType.LOGICAL_ASSERT_ONE_ROW.equals(input.getOp().getOpType())) {
            return false;
        }

        if (OperatorType.LOGICAL_AGGR.equals(input.getOp().getOpType())) {
            LogicalAggregationOperator agg = input.getOp().cast();

            if (agg.getGroupingKeys().isEmpty()) {
                return false;
            }
        }

        LogicalValuesOperator v = input.inputAt(0).getOp().cast();
        return v.getRows().isEmpty();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        List<ColumnRefOperator> refs = input.getOutputColumns().getColumnRefOperators(context.getColumnRefFactory());
        return Lists.newArrayList(OptExpression.create(new LogicalValuesOperator(refs)));
    }
}
