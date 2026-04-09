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
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;

import java.util.List;

/**
 * Pushes {@link LogicalVersionOperator} below {@link LogicalAggregationOperator}.
 *
 * <p>Pattern: {@code LogicalVersionOperator -> LogicalAggregationOperator -> child}
 *
 * <p>Output: {@code LogicalAggregationOperator -> LogicalVersionOperator -> child}
 */
public class IvmVersionAggregateRule extends TransformationRule {
    public IvmVersionAggregateRule() {
        super(RuleType.TF_IVM_VERSION_AGGREGATE,
                Pattern.create(OperatorType.LOGICAL_VERSION)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalVersionOperator version = (LogicalVersionOperator) input.getOp();
        LogicalAggregationOperator aggregate = input.inputAt(0).getOp().cast();
        OptExpression aggChild = input.inputAt(0).inputAt(0);

        OptExpression versionChild = OptExpression.create(
                new LogicalVersionOperator(version.getVersionRefType()), aggChild);
        return List.of(OptExpression.create(
                LogicalAggregationOperator.builder().withOperator(aggregate).build(),
                versionChild));
    }
}
