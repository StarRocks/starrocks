// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class HashAggImplementationRule extends ImplementationRule {

    public HashAggImplementationRule() {
        super(RuleType.IMP_HASH_AGGREGATE, Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator logical = (LogicalAggregationOperator) input.getOp();
        PhysicalHashAggregateOperator physical = new PhysicalHashAggregateOperator(logical.getType(),
                logical.getGroupingKeys(),
                logical.getPartitionByColumns(),
                logical.getAggregations(),
                logical.getSingleDistinctFunctionPos(),
                logical.isSplit(),
                logical.getLimit(),
                logical.getPredicate(),
                logical.getProjection());
        physical.setDistinctColumnDataSkew(logical.getDistinctColumnDataSkew());
        OptExpression result = OptExpression.create(physical, input.getInputs());
        return Lists.newArrayList(result);
    }
}
