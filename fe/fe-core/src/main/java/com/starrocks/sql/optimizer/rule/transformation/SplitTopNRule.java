// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class SplitTopNRule extends TransformationRule {
    private SplitTopNRule() {
        super(RuleType.TF_SPLIT_TOPN, Pattern.create(OperatorType.LOGICAL_TOPN, OperatorType.PATTERN_LEAF));
    }

    private static final SplitTopNRule INSTANCE = new SplitTopNRule();

    public static SplitTopNRule getInstance() {
        return INSTANCE;
    }

    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topN = (LogicalTopNOperator) input.getOp();
        LogicalOperator child = (LogicalOperator) input.getInputs().get(0).getOp();
        // Only apply this rule if the sort phase is final and not split
        return topN.getSortPhase().isFinal() && !topN.isSplit() && !child.hasLimit();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator src = (LogicalTopNOperator) input.getOp();

        long limit = src.getLimit() + src.getOffset();
        LogicalTopNOperator partialSort = new LogicalTopNOperator(
                src.getOrderByElements(), limit, Operator.DEFAULT_OFFSET, SortPhase.PARTIAL);

        LogicalTopNOperator finalSort = LogicalTopNOperator.builder().withOperator(src).setIsSplit(true).build();
        OptExpression partialSortExpression = OptExpression.create(partialSort, input.getInputs());
        OptExpression finalSortExpression = OptExpression.create(finalSort, partialSortExpression);
        return Lists.newArrayList(finalSortExpression);
    }
}
