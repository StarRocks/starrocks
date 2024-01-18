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
import com.starrocks.common.structure.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;

public class PruneTableFunctionColumnRule extends TransformationRule {
    public PruneTableFunctionColumnRule() {
        super(RuleType.TF_PRUNE_TABLE_FUNCTION_COLUMNS,
                Pattern.create(OperatorType.LOGICAL_TABLE_FUNCTION)
                        .addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTableFunctionOperator logicalTableFunctionOperator = (LogicalTableFunctionOperator) input.getOp();
        ColumnRefSet requiredOutputColumns = context.getTaskContext().getRequiredColumns();

        List<ColumnRefOperator> newOuterCols = Lists.newArrayList();
        for (ColumnRefOperator col : logicalTableFunctionOperator.getOuterColRefs()) {
            if (requiredOutputColumns.contains(col.getId())) {
                newOuterCols.add(col);
            }
        }

        for (Pair<ColumnRefOperator, ScalarOperator> pair : logicalTableFunctionOperator.getFnParamColumnProject()) {
            requiredOutputColumns.union(pair.first);
        }

        LogicalTableFunctionOperator newOperator = (new LogicalTableFunctionOperator.Builder())
                .withOperator(logicalTableFunctionOperator)
                .setOuterColRefs(newOuterCols).build();

        if (logicalTableFunctionOperator.equals(newOperator)) {
            return Collections.emptyList();
        }

        return Lists.newArrayList(OptExpression.create(newOperator, input.getInputs()));
    }
}