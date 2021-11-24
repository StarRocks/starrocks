// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;

public class PhysicalCTEConsumeOperator extends PhysicalOperator {
    private final String cteId;

    public PhysicalCTEConsumeOperator(String cteId) {
        super(OperatorType.PHYSICAL_CTE_CONSUME);
        this.cteId = cteId;
    }

    public String getCteId() {
        return cteId;
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalCTEConsume(optExpression, context);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalCTEConsume(this, context);
    }
}
