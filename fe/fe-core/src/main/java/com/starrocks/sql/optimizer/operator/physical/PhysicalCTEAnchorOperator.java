// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;

import java.util.Objects;

public class PhysicalCTEAnchorOperator extends PhysicalOperator {
    private final int cteId;
    private final int consumeNum;

    public PhysicalCTEAnchorOperator(int cteId, int consumeNum, Projection projection) {
        super(OperatorType.PHYSICAL_CTE_ANCHOR);
        this.cteId = cteId;
        this.consumeNum = consumeNum;
        this.projection = projection;
    }

    public int getCteId() {
        return cteId;
    }

    public int getConsumeNum() {
        return consumeNum;
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalCTEAnchor(optExpression, context);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalCTEAnchor(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        PhysicalCTEAnchorOperator that = (PhysicalCTEAnchorOperator) o;
        return Objects.equals(cteId, that.cteId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), cteId);
    }

    @Override
    public String toString() {
        return "PhysicalCTEAnchorOperator{" +
                "cteId='" + cteId + '\'' +
                '}';
    }
}
