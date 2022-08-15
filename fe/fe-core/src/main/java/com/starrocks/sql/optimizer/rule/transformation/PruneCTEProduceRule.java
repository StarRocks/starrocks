// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class PruneCTEProduceRule extends TransformationRule {
    public PruneCTEProduceRule() {
        super(RuleType.TF_PRUNE_CTE_PRODUCE, Pattern.create(OperatorType.LOGICAL_CTE_ANCHOR)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF))
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF, OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalCTEAnchorOperator anchor = (LogicalCTEAnchorOperator) input.getOp();
        return context.getCteContext().needInline(anchor.getCteId());
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        // Delete cte produce/anchor direct
        return Lists.newArrayList(input.getInputs().get(1));
    }
}
