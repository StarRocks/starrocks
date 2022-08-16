// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class MergeLimitWithSortRule extends TransformationRule {
    public MergeLimitWithSortRule() {
        super(RuleType.TF_MERGE_LIMIT_WITH_SORT, Pattern.create(OperatorType.LOGICAL_LIMIT)
                .addChildren(Pattern.create(OperatorType.LOGICAL_TOPN, OperatorType.PATTERN_LEAF)));
    }

    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topN = (LogicalTopNOperator) input.getInputs().get(0).getOp();
        LogicalLimitOperator limit = ((LogicalLimitOperator) input.getOp());

        // Merge Init-Limit/Local-limit and Sort
        // Local-limit may be generate at MergeLimitWithLimitRule
        return (limit.isInit() || limit.isLocal()) && !topN.hasLimit();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        // will transform to topN
        LogicalLimitOperator limit = (LogicalLimitOperator) input.getOp();
        LogicalTopNOperator sort = (LogicalTopNOperator) input.getInputs().get(0).getOp();

        OptExpression result = new OptExpression(
                new LogicalTopNOperator(sort.getOrderByElements(), limit.getLimit(), limit.getOffset()));
        result.getInputs().addAll(input.getInputs().get(0).getInputs());
        return Lists.newArrayList(result);
    }
}
