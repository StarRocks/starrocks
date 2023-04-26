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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.SubqueryUtils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;

/*
 *          Apply                 join
 *         /     \               /    \
 *      join    subquery  ==>  A     Apply
 *     /   \                        /   \
 *    A     B                      B     subquery
 *
 */
public class PushDownApplyLeftRule extends TransformationRule {

    private static final List<OperatorType> WHITE_LIST =
            ImmutableList.of(OperatorType.LOGICAL_JOIN,
                    OperatorType.LOGICAL_FILTER,
                    OperatorType.LOGICAL_APPLY);

    public PushDownApplyLeftRule() {
        super(RuleType.TF_PUSH_DOWN_APPLY, Pattern.create(OperatorType.LOGICAL_APPLY)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF, OperatorType.PATTERN_MULTI_LEAF))
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();

        OperatorType childType = input.getInputs().get(0).getOp().getOpType();

        if (!WHITE_LIST.contains(childType)) {
            return false;
        }

        if (childType == OperatorType.LOGICAL_APPLY) {
            LogicalApplyOperator child = (LogicalApplyOperator) input.getInputs().get(0).getOp();
            if (SubqueryUtils.isUnCorrelationScalarSubquery(child)) {
                return false;
            }
        }

        return SubqueryUtils.isUnCorrelationScalarSubquery(apply);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();
        ColumnRefSet outerColumns = apply.getUnCorrelationSubqueryPredicateColumns();

        OptExpression left = input.getInputs().get(0);

        // find subquery bind child
        int index = 0;
        for (; index < left.getInputs().size(); index++) {
            if (left.getInputs().get(index).getOutputColumns().containsAll(outerColumns)) {
                break;
            }
        }

        if (index >= left.getInputs().size()) {
            return Collections.emptyList();
        }

        OptExpression newApply = OptExpression.create(apply, left.getInputs().get(index), input.getInputs().get(1));
        List<OptExpression> newChildren = Lists.newArrayList(left.getInputs());
        newChildren.set(index, newApply);
        return Lists.newArrayList(OptExpression.create(left.getOp(), newChildren));
    }
}
