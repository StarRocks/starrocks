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
import com.starrocks.sql.optimizer.operator.logical.LogicalSetOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;

public class PruneExceptColumnsRule extends TransformationRule {
    public PruneExceptColumnsRule() {
        super(RuleType.TF_PRUNE_EXCEPT_COLUMNS,
                Pattern.create(OperatorType.LOGICAL_EXCEPT)
                        .addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        ColumnRefSet requiredOutputColumns = context.getTaskContext().getRequiredColumns();

        LogicalSetOperator lso = (LogicalSetOperator) input.getOp();
        List<ColumnRefOperator> outputs = lso.getOutputColumnRefOp();

        for (int idx = 0; idx < outputs.size(); ++idx) {
            for (int childIdx = 0; childIdx < input.arity(); ++childIdx) {
                requiredOutputColumns.union(lso.getChildOutputColumns().get(childIdx).get(idx));
            }
        }

        return Collections.emptyList();
    }
}
