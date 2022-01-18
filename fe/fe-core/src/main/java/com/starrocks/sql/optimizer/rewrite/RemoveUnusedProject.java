// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class RemoveUnusedProject extends OptExpressionVisitor<OptExpression, Void> {
    public OptExpression rewrite(OptExpression optExpression) {
        return optExpression.getOp().accept(this, optExpression, null);
    }

    @Override
    public OptExpression visit(OptExpression optExpr, Void context) {
        for (int idx = 0; idx < optExpr.arity(); ++idx) {
            optExpr.setChild(idx, rewrite(optExpr.inputAt(idx)));
        }
        return optExpr;
    }

    @Override
    public OptExpression visitPhysicalProject(OptExpression optExpr, Void context) {
        PhysicalProjectOperator physicalProjectOperator = (PhysicalProjectOperator) optExpr.getOp();
        OptExpression childOpt = rewrite(optExpr.inputAt(0));

        ColumnRefSet childOutputColumns = new ColumnRefSet();
        if (childOpt.getOp() instanceof PhysicalHashJoinOperator) {
            PhysicalHashJoinOperator joinOperator = (PhysicalHashJoinOperator) childOpt.getOp();
            if (joinOperator.getJoinType().isLeftSemiAntiJoin()) {
                childOutputColumns.union(childOpt.inputAt(0).getOutputColumns());
            } else if (joinOperator.getJoinType().isRightSemiAntiJoin()) {
                childOutputColumns.union(childOpt.inputAt(1).getOutputColumns());
            } else {
                childOutputColumns.union(childOpt.inputAt(0).getOutputColumns());
                childOutputColumns.union(childOpt.inputAt(1).getOutputColumns());
            }
        } else if (childOpt.getOp() instanceof PhysicalProjectOperator) {
            PhysicalProjectOperator firstProject = physicalProjectOperator;
            PhysicalProjectOperator secondProject = (PhysicalProjectOperator) childOpt.getOp();

            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(secondProject.getColumnRefMap());
            Map<ColumnRefOperator, ScalarOperator> resultMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : firstProject.getColumnRefMap().entrySet()) {
                resultMap.put(entry.getKey(), entry.getValue().accept(rewriter, null));
            }

            PhysicalProjectOperator projectOperator = new PhysicalProjectOperator(resultMap, new HashMap<>());
            projectOperator.setLimit(Math.min(firstProject.getLimit(), secondProject.getLimit()));

            OptExpression optExpression = OptExpression.create(projectOperator, childOpt.getInputs());
            optExpression.setLogicalProperty(optExpr.getLogicalProperty());
            optExpression.setStatistics(optExpr.getStatistics());
            return optExpression;
        } else {
            childOutputColumns.union(childOpt.getOutputColumns());
        }

        ColumnRefSet projectColumns =
                new ColumnRefSet(new ArrayList<>(physicalProjectOperator.getColumnRefMap().keySet()));

        if (projectColumns.equals(childOutputColumns)) {
            return childOpt;
        }
        return optExpr;
    }
}
