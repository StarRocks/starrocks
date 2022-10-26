// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

public abstract class StreamImplementationRule extends ImplementationRule {

    protected StreamImplementationRule(RuleType type, Pattern pattern) {
        super(type, pattern);
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        return context.getSessionVariable().isMVPlanner();
    }
}
