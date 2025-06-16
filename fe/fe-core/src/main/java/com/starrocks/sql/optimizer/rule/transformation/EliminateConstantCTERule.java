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
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

// If all leaf nodes of cte are constant, we can directly inline this cte.
// After inline, we can perform more optimizations, such as
// with cte as (select 111) select * from cte join (select * from (select * from t1 join cte) s1) s2;
// we can eliminate join node by EliminateJoinWithConstantRule.
public class EliminateConstantCTERule extends TransformationRule {
    public EliminateConstantCTERule() {
        super(RuleType.TF_ELIMINATE_CONSTANT_CTE_CONSUME, Pattern.create(OperatorType.LOGICAL_CTE_CONSUME)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        return areAllLeafNodesConstants(input);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        return input.getInputs();
    }

    private boolean areAllLeafNodesConstants(OptExpression root) {
        for (OptExpression optExpression : root.getInputs()) {
            if (!optExpression.getInputs().isEmpty()) {
                if (!areAllLeafNodesConstants(optExpression)) {
                    return false;
                }
            } else {
                if (optExpression.getOp().getOpType() != OperatorType.LOGICAL_VALUES) {
                    return false;
                }
            }
        }

        return true;
    }

}
