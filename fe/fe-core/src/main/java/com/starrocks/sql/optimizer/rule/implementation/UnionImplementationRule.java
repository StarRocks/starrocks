// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalUnionOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class UnionImplementationRule extends ImplementationRule {
    public UnionImplementationRule() {
        super(RuleType.IMP_UNION,
                Pattern.create(OperatorType.LOGICAL_UNION)
                        .addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalUnionOperator union = (LogicalUnionOperator) input.getOp();
        PhysicalUnionOperator physicalUnion = new PhysicalUnionOperator(
                union.getOutputColumnRefOp(),
                union.getChildOutputColumns(),
                union.isUnionAll(),
                union.getLimit(),
                union.getPredicate(),
                union.getProjection());
        return Lists.newArrayList(OptExpression.create(physicalUnion, input.getInputs()));
    }
}
