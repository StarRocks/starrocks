// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;
import java.util.Objects;

public class PhysicalCTEConsumeOperator extends PhysicalOperator {
    private final int cteId;

    private final Map<ColumnRefOperator, ColumnRefOperator> cteOutputColumnRefMap;

    public PhysicalCTEConsumeOperator(int cteId, Map<ColumnRefOperator, ColumnRefOperator> cteOutputColumnRefMap,
                                      long limit, ScalarOperator predicate, Projection projection) {
        super(OperatorType.PHYSICAL_CTE_CONSUME);
        this.cteId = cteId;
        this.cteOutputColumnRefMap = cteOutputColumnRefMap;
        this.limit = limit;
        this.predicate = predicate;
        this.projection = projection;
    }

    public int getCteId() {
        return cteId;
    }

    public Map<ColumnRefOperator, ColumnRefOperator> getCteOutputColumnRefMap() {
        return cteOutputColumnRefMap;
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalCTEConsume(optExpression, context);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalCTEConsume(this, context);
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
        PhysicalCTEConsumeOperator that = (PhysicalCTEConsumeOperator) o;
        return Objects.equals(cteId, that.cteId) &&
                Objects.equals(cteOutputColumnRefMap, that.cteOutputColumnRefMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), cteId, cteOutputColumnRefMap);
    }

    @Override
    public String toString() {
        return "PhysicalCTEConsumeOperator{" +
                "cteId='" + cteId + '\'' +
                ", limit=" + limit +
                ", predicate=" + predicate +
                '}';
    }
}
