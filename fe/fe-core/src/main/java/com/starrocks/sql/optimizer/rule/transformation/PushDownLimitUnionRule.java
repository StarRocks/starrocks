// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class PushDownLimitUnionRule extends TransformationRule {
    public PushDownLimitUnionRule() {
        super(RuleType.TF_PUSH_DOWN_LIMIT_UNION, Pattern.create(OperatorType.LOGICAL_LIMIT)
                .addChildren(Pattern.create(OperatorType.LOGICAL_UNION, OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalLimitOperator limit = (LogicalLimitOperator) input.getOp();
        return limit.isLocal();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalLimitOperator limit = (LogicalLimitOperator) input.getOp();
        Preconditions.checkState(!limit.hasOffset());

        OptExpression child = input.inputAt(0);
        LogicalOperator logicOperator = (LogicalOperator) child.getOp();

        // set limit
        logicOperator.setLimit(limit.getLimit());

        // push down
        List<OptExpression> newUnionChild = Lists.newArrayList();
        for (OptExpression unionChild : child.getInputs()) {
            OptExpression nl = new OptExpression(LogicalLimitOperator.local(limit.getLimit()));
            nl.getInputs().add(unionChild);
            newUnionChild.add(nl);
        }

        child.getInputs().clear();
        child.getInputs().addAll(newUnionChild);
        return Lists.newArrayList(child);
    }
}
