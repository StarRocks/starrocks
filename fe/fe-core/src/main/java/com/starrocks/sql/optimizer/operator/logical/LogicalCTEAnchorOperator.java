// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;

/*
 * This operator denotes where a particular CTE is defined in the query.
 * It defines the scope of that CTE. A CTE can be referenced only in the
 * subtree rooted by the corresponding CTEAnchor operator
 *
 * */
public class LogicalCTEAnchorOperator extends LogicalOperator {
    private final String cteId;

    public LogicalCTEAnchorOperator(String cteId) {
        super(OperatorType.LOGICAL_CTE_ANCHOR);
        this.cteId = cteId;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        return expressionContext.getChildLogicalProperty(1).getOutputColumns();
    }

    public String getCteId() {
        return cteId;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalCTEAnchor(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalCTEAnchor(optExpression, context);
    }
}
