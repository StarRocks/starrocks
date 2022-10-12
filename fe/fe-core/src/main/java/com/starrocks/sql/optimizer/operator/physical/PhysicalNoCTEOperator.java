// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;

import java.util.Objects;

public class PhysicalNoCTEOperator extends PhysicalOperator {
    private final int cteId;

    public PhysicalNoCTEOperator(int cteId) {
        super(OperatorType.PHYSICAL_NO_CTE);
        this.cteId = cteId;
    }

    public PhysicalNoCTEOperator(int cteId, Projection projection) {
        super(OperatorType.PHYSICAL_NO_CTE);
        this.cteId = cteId;
        this.projection = projection;
    }

    public int getCteId() {
        return cteId;
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalNoCTE(optExpression, context);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalNoCTE(this, context);
    }

    @Override
    public boolean equals(Object o) {
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), cteId);
    }
}
