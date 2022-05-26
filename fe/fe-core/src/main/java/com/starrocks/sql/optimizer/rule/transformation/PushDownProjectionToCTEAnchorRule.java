// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;

public class PushDownProjectionToCTEAnchorRule extends TransformationRule {

    public PushDownProjectionToCTEAnchorRule() {
        super(RuleType.TF_PUSH_DOWN_PROJECT_TO_CTE_ANCHOR,
                Pattern.create(OperatorType.LOGICAL_PROJECT).addChildren(
                        (Pattern.create(OperatorType.LOGICAL_CTE_ANCHOR)
                                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF))
                                .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT,
                                        OperatorType.PATTERN_LEAF)))
                ));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalProjectOperator projectOp = input.getOp().cast();

        OptExpression cteAnchor = input.inputAt(0);
        LogicalCTEAnchorOperator cteAnchorOp = cteAnchor.getOp().cast();

        OptExpression cteAnchorLeftChild = cteAnchor.inputAt(0);
        OptExpression cteAnchorRightChild = cteAnchor.inputAt(1);
        LogicalProjectOperator childProjectOp = cteAnchorRightChild.getOp().cast();

        Map<ColumnRefOperator, ScalarOperator> newProjectMap = Maps.newHashMap();
        newProjectMap.putAll(childProjectOp.getColumnRefMap());
        newProjectMap.putAll(projectOp.getColumnRefMap());

        OptExpression newChildProject = OptExpression.create(new LogicalProjectOperator.Builder()
                .withOperator(childProjectOp)
                .setColumnRefMap(newProjectMap)
                .build(), cteAnchorRightChild.getInputs());

        OptExpression newCteAnchor = OptExpression.create(new LogicalCTEAnchorOperator.Builder()
                .withOperator(cteAnchorOp)
                .build(), cteAnchorLeftChild, newChildProject);

        return Lists.newArrayList(newCteAnchor);
    }
}
