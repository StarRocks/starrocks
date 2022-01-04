// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

public class MergeProjectWithChildRule extends OptExpressionVisitor<OptExpression, Void> {
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
        OptExpression childOpt = visit(optExpr.inputAt(0), context);

        PhysicalOperator childOperator = (PhysicalOperator) childOpt.getOp();
        if (childOperator.getProjection() != null) {
            ReplaceColumnRefRewriter rewriter =
                    new ReplaceColumnRefRewriter(childOperator.getProjection().getColumnRefMap());
            Map<ColumnRefOperator, ScalarOperator> resultMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : physicalProjectOperator.getColumnRefMap()
                    .entrySet()) {
                resultMap.put(entry.getKey(), entry.getValue().accept(rewriter, null));
            }
            childOperator.setProjection(new Projection(resultMap));
        } else {
            childOperator.setProjection(new Projection(physicalProjectOperator.getColumnRefMap()));
        }
        if (physicalProjectOperator.hasLimit()) {
            childOperator.setLimit(physicalProjectOperator.getLimit());
        }

        childOpt.setLogicalProperty(optExpr.getLogicalProperty());
        return childOpt;
    }
}
