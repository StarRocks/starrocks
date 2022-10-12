// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;

public class PruneEmptyWindowRule extends TransformationRule {
    public PruneEmptyWindowRule() {
        super(RuleType.TF_PRUNE_EMPTY_WINDOW, Pattern.create(OperatorType.LOGICAL_WINDOW)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF, OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalWindowOperator windowOperator = (LogicalWindowOperator) input.getOp();

        if (windowOperator.getWindowCall().isEmpty()) {
            return input.getInputs();
        }

        return Collections.emptyList();
    }
}
