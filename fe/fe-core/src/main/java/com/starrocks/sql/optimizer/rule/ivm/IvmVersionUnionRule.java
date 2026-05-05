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

package com.starrocks.sql.optimizer.rule.ivm;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;

import java.util.List;

/**
 * Pushes {@link LogicalVersionOperator} below {@link LogicalUnionOperator} by wrapping each
 * union child with the same version operator.
 *
 * <p>Pattern: {@code LogicalVersionOperator -> LogicalUnionOperator(MULTI_LEAF)}
 *
 * <p>Output: {@code LogicalUnionOperator(Version -> child_0, Version -> child_1, ...)}
 */
public class IvmVersionUnionRule extends TransformationRule {
    public IvmVersionUnionRule() {
        super(RuleType.TF_IVM_VERSION_UNION,
                Pattern.create(OperatorType.LOGICAL_VERSION)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_UNION, OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalVersionOperator version = (LogicalVersionOperator) input.getOp();
        OptExpression unionExpr = input.inputAt(0);
        LogicalUnionOperator union = (LogicalUnionOperator) unionExpr.getOp();

        List<OptExpression> newChildren = Lists.newArrayListWithCapacity(unionExpr.arity());
        for (OptExpression child : unionExpr.getInputs()) {
            newChildren.add(OptExpression.create(
                    new LogicalVersionOperator(version.getVersionRefType()), child));
        }

        return List.of(OptExpression.create(
                LogicalUnionOperator.builder().withOperator(union).build(), newChildren));
    }
}
