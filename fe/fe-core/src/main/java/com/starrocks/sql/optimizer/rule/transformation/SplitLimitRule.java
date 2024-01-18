// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

/*
 * Split Init-limit to Global-Limit and Local-Limit
 *
 *      Limit(Init)            Limit(Global)
 *          |          ==>         |
 *        Child                Limit(Local)
 *                                 |
 *                               Child
 * */
public class SplitLimitRule extends TransformationRule {
    public SplitLimitRule() {
        super(RuleType.TF_SPLIT_LIMIT, Pattern.create(OperatorType.LOGICAL_LIMIT, OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalLimitOperator limitOperator = (LogicalLimitOperator) input.getOp();
        return limitOperator.isInit();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalLimitOperator limit = (LogicalLimitOperator) input.getOp();

        LogicalLimitOperator global = LogicalLimitOperator.global(limit.getLimit());
        LogicalLimitOperator local = LogicalLimitOperator.local(limit.getLimit());

        return Lists.newArrayList(OptExpression.create(global, OptExpression.create(local, input.getInputs())));
    }
}
