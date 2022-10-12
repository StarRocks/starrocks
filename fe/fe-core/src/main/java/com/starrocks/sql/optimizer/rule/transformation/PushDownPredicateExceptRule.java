// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class PushDownPredicateExceptRule extends TransformationRule {
    public PushDownPredicateExceptRule() {
        super(RuleType.TF_PUSH_DOWN_PREDICATE_EXCEPT,
                Pattern.create(OperatorType.LOGICAL_FILTER).
                        addChildren(Pattern.create(OperatorType.LOGICAL_EXCEPT, OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        return PushDownPredicateSetRule.process(input, context);
    }
}
