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
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

/*
 * Merge multiple limit, can be support:
 *
 *      Limit (less rows limit)
 *        |                       ===>  Limit(less rows limit)
 *      Limit (more rows limit)
 *
 * can't merge like:
 *
 *      Limit (more rows limit)
 *        |
 *      Limit (less rows limit)
 *
 * */
public class MergeLimitWithLimitRule extends TransformationRule {
    public MergeLimitWithLimitRule() {
        super(RuleType.TF_MERGE_LIMIT_WITH_LIMIT, Pattern.create(OperatorType.LOGICAL_LIMIT)
                .addChildren(Pattern.create(OperatorType.LOGICAL_LIMIT, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        // Any limit operator can be merged with global/init limit, no scene where the child is local limit
        LogicalLimitOperator childLimit = (LogicalLimitOperator) input.getInputs().get(0).getOp();
        return childLimit.isGlobal() || childLimit.isInit();
    }

    // eg.1, child limit is smaller than parent, child must gather
    // before:
    //   Limit 5 (hit line range: [0, 5), output line range: [0, 2))
    //      |
    // Global-Limit 2 (hit line range: [0, 2), output line range: [0, 2))
    //
    // after:
    // Init-Limit 0, 2 (hit line range: [0, 2), output line range: [0, 2))
    //
    // eg.2, child limit is larger than parent, child don't gather
    // before:
    //   Limit 2 (hit line range: [0, 2), output line range: [0, 2))
    //      |
    // Global-Limit 5 (hit line range: [0, 5), output line range: [0, 5))
    //
    // after:
    // Local-Limit 2 (hit line range: [0, 2), output line range: [0, 2))
    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalLimitOperator l1 = (LogicalLimitOperator) input.getOp();
        LogicalLimitOperator l2 = (LogicalLimitOperator) input.getInputs().get(0).getOp();

        Preconditions.checkState(!l1.hasOffset());

        // l2 range
        long l2Max = l2.getLimit();

        // l1 range
        long l1Max = l1.getLimit();

        long limit = Math.min(l2Max, l1Max);

        if (limit <= 0) {
            limit = 0;
        }

        Operator result;
        if (l1.getLimit() <= l2.getLimit()) {
            result = LogicalLimitOperator.local(limit, l2.getOffset());
        } else {
            result = LogicalLimitOperator.init(limit, l2.getOffset());
        }

        return Lists.newArrayList(OptExpression.create(result, input.getInputs().get(0).getInputs()));
    }
}
