// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;

public class CollectCTEProduceRule extends TransformationRule {
    public CollectCTEProduceRule() {
        super(RuleType.TF_COLLECT_CTE_PRODUCE, Pattern.create(OperatorType.LOGICAL_CTE_PRODUCE));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalCTEProduceOperator produce = (LogicalCTEProduceOperator) input.getOp();
        context.getCteContext().addCTEProduce(produce.getCteId());
        return Collections.emptyList();
    }
}
