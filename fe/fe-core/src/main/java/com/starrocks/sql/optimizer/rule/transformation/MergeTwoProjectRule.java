// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MergeTwoProjectRule extends TransformationRule {
    public MergeTwoProjectRule() {
        super(RuleType.TF_MERGE_TWO_PROJECT, Pattern.create(OperatorType.LOGICAL_PROJECT)
                .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalProjectOperator secondProject = (LogicalProjectOperator) input.getInputs().get(0).getOp();
        Optional<ScalarOperator> assertColumn = secondProject.getColumnRefMap().values()
                .stream()
                .filter((op) -> {
                    if (!(op instanceof CallOperator)) {
                        return false;
                    }
                    return FunctionSet.ASSERT_TRUE.equals(((CallOperator) op).getFnName());
                })
                .findAny();
        return !assertColumn.isPresent();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalProjectOperator firstProject = (LogicalProjectOperator) input.getOp();
        LogicalProjectOperator secondProject = (LogicalProjectOperator) input.getInputs().get(0).getOp();

        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(secondProject.getColumnRefMap());
        Map<ColumnRefOperator, ScalarOperator> resultMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : firstProject.getColumnRefMap().entrySet()) {
            resultMap.put(entry.getKey(), rewriter.rewrite(entry.getValue()));
        }

        OptExpression optExpression = new OptExpression(
                new LogicalProjectOperator(resultMap, Math.min(firstProject.getLimit(), secondProject.getLimit())));
        optExpression.getInputs().addAll(input.getInputs().get(0).getInputs());
        return Lists.newArrayList(optExpression);
    }
}
