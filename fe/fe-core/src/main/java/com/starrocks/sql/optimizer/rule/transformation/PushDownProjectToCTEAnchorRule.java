// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

/*
 *           Project                                   CTEAnchor
 *              |                                     /        \
 *           CTEAnchor               --->        LEFT_CHILD   Project
 *          /        \                                          \
 *   LEFT_CHILD  RIGHT_CHILD                                  RIGHT_CHILD
 */
public class PushDownProjectToCTEAnchorRule extends TransformationRule {

    public PushDownProjectToCTEAnchorRule() {
        super(RuleType.TF_PUSH_DOWN_PROJECT_TO_CTE_ANCHOR,
                Pattern.create(OperatorType.LOGICAL_PROJECT).addChildren(
                        Pattern.create(OperatorType.LOGICAL_CTE_ANCHOR, OperatorType.PATTERN_LEAF,
                                OperatorType.PATTERN_LEAF)
                ));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalProjectOperator projectOp = input.getOp().cast();

        OptExpression cteAnchor = input.inputAt(0);
        LogicalCTEAnchorOperator cteAnchorOp = cteAnchor.getOp().cast();
        OptExpression cteAnchorLeftChild = cteAnchor.inputAt(0);
        OptExpression cteAnchorRightChild = cteAnchor.inputAt(1);

        OptExpression newProject = OptExpression.create(new LogicalProjectOperator.Builder()
                .withOperator(projectOp)
                .build(), cteAnchorRightChild);

        OptExpression newCteAnchor = OptExpression.create(new LogicalCTEAnchorOperator.Builder()
                .withOperator(cteAnchorOp)
                .build(), cteAnchorLeftChild, newProject);

        return Lists.newArrayList(newCteAnchor);
    }
}
