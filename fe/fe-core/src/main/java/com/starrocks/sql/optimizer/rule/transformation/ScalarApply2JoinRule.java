// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ScalarApply2JoinRule extends TransformationRule {
    public ScalarApply2JoinRule() {
        super(RuleType.TF_SCALAR_APPLY_TO_JOIN,
                Pattern.create(OperatorType.LOGICAL_APPLY, OperatorType.PATTERN_LEAF, OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();
        // Or-Scope is same with And-Scope
        return apply.isScalar() && !Utils.containsCorrelationSubquery(input.getGroupExpression());
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();

        // Un-correlation subquery
        if (null == apply.getCorrelationConjuncts()) {
            return transformUnCorrelateCheckOneRows(input, apply, context);
        } else {
            return transformCorrelate(input, apply, context);
        }
    }

    public List<OptExpression> transformCorrelate(OptExpression input, LogicalApplyOperator apply,
                                                  OptimizerContext context) {
        if (!apply.isNeedCheckMaxRows()) {
            OptExpression joinOptExpression;

            joinOptExpression = new OptExpression(
                    new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN,
                            Utils.compoundAnd(apply.getCorrelationConjuncts(), apply.getPredicate())));

            joinOptExpression.getInputs().addAll(input.getInputs());

            Map<ColumnRefOperator, ScalarOperator> output = Maps.newHashMap();
            output.put(apply.getOutput(), apply.getSubqueryOperator());

            // add all left column
            ColumnRefFactory factory = context.getColumnRefFactory();
            Arrays.stream(input.getInputs().get(0).getOutputColumns().getColumnIds()).mapToObj(factory::getColumnRef)
                    .forEach(d -> output.put(d, d));

            OptExpression projectExpression = new OptExpression(new LogicalProjectOperator(output));
            projectExpression.getInputs().add(joinOptExpression);

            return Lists.newArrayList(projectExpression);
        }

        return Lists.newArrayList();
    }

    // UnCorrelation Scalar Subquery:
    //  Project(output: subqueryOperator)
    //              |
    //          Cross-Join
    //          /        \
    //       LEFT     AssertOneRows
    public List<OptExpression> transformUnCorrelateCheckOneRows(OptExpression input, LogicalApplyOperator apply,
                                                                OptimizerContext context) {
        // assert one rows will check rows, and fill null row if result is empty
        OptExpression assertOptExpression = new OptExpression(LogicalAssertOneRowOperator.createLessEqOne(""));
        assertOptExpression.getInputs().add(input.getInputs().get(1));

        // use hint, forbidden reorder un-correlate subquery
        OptExpression joinOptExpression = new OptExpression(
                LogicalJoinOperator.builder().setJoinType(JoinOperator.CROSS_JOIN).setJoinHint("broadcast").build());
        joinOptExpression.getInputs().add(input.getInputs().get(0));
        joinOptExpression.getInputs().add(assertOptExpression);

        ColumnRefFactory factory = context.getColumnRefFactory();
        Map<ColumnRefOperator, ScalarOperator> allOutput = Maps.newHashMap();
        allOutput.put(apply.getOutput(), apply.getSubqueryOperator());

        // add all left outer column
        Arrays.stream(input.getInputs().get(0).getOutputColumns().getColumnIds()).mapToObj(factory::getColumnRef)
                .forEach(d -> allOutput.put(d, d));

        OptExpression projectExpression = new OptExpression(new LogicalProjectOperator(allOutput));
        projectExpression.getInputs().add(joinOptExpression);

        return Lists.newArrayList(projectExpression);
    }
}
