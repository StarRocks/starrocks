// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.OrderSpec;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Objects;

public class PhysicalTopNOperator extends PhysicalOperator {
    private final long offset;
    private final SortPhase sortPhase;
    private final boolean isSplit;

    private boolean isEnforced;

    // If limit is -1, means global sort
    public PhysicalTopNOperator(OrderSpec spec, long limit, long offset,
                                SortPhase sortPhase,
                                boolean isSplit,
                                boolean isEnforced,
                                ScalarOperator predicate,
                                Projection projection) {
        super(OperatorType.PHYSICAL_TOPN, spec);
        this.limit = limit;
        this.offset = offset;
        this.sortPhase = sortPhase;
        this.isSplit = isSplit;
        this.isEnforced = isEnforced;
        this.predicate = predicate;
        this.projection = projection;
    }

    public SortPhase getSortPhase() {
        return sortPhase;
    }

    public boolean isSplit() {
        return isSplit;
    }

    public long getOffset() {
        return offset;
    }

    public boolean isEnforced() {
        return isEnforced;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sortPhase, orderSpec);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PhysicalTopNOperator)) {
            return false;
        }

        PhysicalTopNOperator rhs = (PhysicalTopNOperator) obj;
        if (this == rhs) {
            return true;
        }

        return sortPhase.equals(rhs.sortPhase) &&
                orderSpec.equals(rhs.orderSpec);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalTopN(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalTopN(optExpression, context);
    }
}
