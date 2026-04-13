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

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;

import java.util.List;

/**
 * Pushes {@link LogicalVersionOperator} below {@link LogicalJoinOperator} by wrapping
 * both join children with the same version operator.
 *
 * <p>Pattern: {@code LogicalVersionOperator -> LogicalJoinOperator(LEAF, LEAF)}
 *
 * <p>Output: {@code LogicalJoinOperator(Version -> left, Version -> right)}
 *
 * <p>This rule is needed for multi-way joins (3+ tables). When the outer join is expanded
 * by {@link IvmDeltaJoinRule}, one branch produces {@code Version -> inner_join}, which
 * needs this rule to push Version down to each scan.
 */
public class IvmVersionJoinRule extends TransformationRule {
    public IvmVersionJoinRule() {
        super(RuleType.TF_IVM_VERSION_JOIN,
                Pattern.create(OperatorType.LOGICAL_VERSION)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_JOIN,
                                OperatorType.PATTERN_LEAF, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalVersionOperator version = (LogicalVersionOperator) input.getOp();
        LogicalJoinOperator join = (LogicalJoinOperator) input.inputAt(0).getOp();
        OptExpression left = input.inputAt(0).inputAt(0);
        OptExpression right = input.inputAt(0).inputAt(1);

        OptExpression versionLeft = OptExpression.create(
                new LogicalVersionOperator(version.getVersionRefType()), left);
        OptExpression versionRight = OptExpression.create(
                new LogicalVersionOperator(version.getVersionRefType()), right);

        return List.of(OptExpression.create(
                LogicalJoinOperator.builder().withOperator(join).build(),
                versionLeft, versionRight));
    }
}
