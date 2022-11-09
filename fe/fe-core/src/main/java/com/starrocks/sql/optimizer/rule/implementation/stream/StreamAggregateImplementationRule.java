// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation.stream;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.stream.PhysicalStreamAggOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class StreamAggregateImplementationRule extends StreamImplementationRule {

    private static final com.starrocks.sql.optimizer.rule.implementation.stream.StreamAggregateImplementationRule INSTANCE =
            new com.starrocks.sql.optimizer.rule.implementation.stream.StreamAggregateImplementationRule(RuleType.IMP_STREAM_AGG);

    public static com.starrocks.sql.optimizer.rule.implementation.stream.StreamAggregateImplementationRule getInstance() {
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
