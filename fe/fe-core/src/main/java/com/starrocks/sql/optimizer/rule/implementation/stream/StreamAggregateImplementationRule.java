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


package com.starrocks.sql.optimizer.rule.implementation.stream;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamAggOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class StreamAggregateImplementationRule extends StreamImplementationRule {

    private static final StreamAggregateImplementationRule INSTANCE =
            new StreamAggregateImplementationRule(RuleType.IMP_STREAM_AGG);

    public static StreamAggregateImplementationRule getInstance() {
        return INSTANCE;
    }

    private StreamAggregateImplementationRule(RuleType type) {
        super(type, Pattern.create(OperatorType.LOGICAL_AGGR).addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator logical = (LogicalAggregationOperator) input.getOp();
        PhysicalStreamAggOperator physical = new PhysicalStreamAggOperator(
                logical.getGroupingKeys(),
                logical.getAggregations(),
                logical.getPredicate(),
                logical.getProjection());
        OptExpression result = OptExpression.create(physical, input.getInputs());
        return Lists.newArrayList(result);
    }
}
