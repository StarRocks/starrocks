// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.SubqueryUtils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PushDownApplyProjectRule extends TransformationRule {
    public PushDownApplyProjectRule() {
        super(RuleType.TF_PUSH_DOWN_APPLY_PROJECT, Pattern.create(OperatorType.LOGICAL_APPLY).addChildren(
                Pattern.create(OperatorType.PATTERN_LEAF),
                Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        // Push down is unnecessary if isn't correlation subquery
        return SubqueryUtils.containsCorrelationSubquery(input);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();

        OptExpression child = input.getInputs().get(1);

        LogicalProjectOperator project = (LogicalProjectOperator) child.getOp();
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(project.getColumnRefMap());
        ScalarOperator newScalarOperator = rewriter.rewrite(apply.getSubqueryOperator());
        ScalarOperator newPredicate = null;
        if (null != apply.getPredicate()) {
            newPredicate = rewriter.rewrite(apply.getPredicate());
        }

        OptExpression newApply = new OptExpression(LogicalApplyOperator.builder().withOperator(apply)
                .setSubqueryOperator(newScalarOperator)
                .setPredicate(newPredicate).build());

        newApply.getInputs().add(input.getInputs().get(0));
        newApply.getInputs().addAll(child.getInputs());

        ColumnRefFactory factory = context.getColumnRefFactory();
        Map<ColumnRefOperator, ScalarOperator> allOutput = Maps.newHashMap();

        // add all left outer column
        Arrays.stream(input.getInputs().get(0).getOutputColumns().getColumnIds()).mapToObj(factory::getColumnRef)
                .forEach(d -> allOutput.put(d, d));
        allOutput.put(apply.getOutput(), apply.getOutput());

        OptExpression newProject = new OptExpression(new LogicalProjectOperator(allOutput));
        newProject.getInputs().add(newApply);

        return Lists.newArrayList(newProject);
    }

}
