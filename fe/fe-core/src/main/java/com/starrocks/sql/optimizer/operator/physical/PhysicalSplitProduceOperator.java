package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;

import java.util.Objects;

public class PhysicalSplitProduceOperator extends PhysicalOperator {
    private final int splitId;

    public PhysicalSplitProduceOperator(int cteId) {
        super(OperatorType.PHYSICAL_SPLIT_PRODUCE);
        this.splitId = cteId;
    }

    public int getSplitId() {
        return splitId;
    }


    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalSplitProducer(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        PhysicalSplitProduceOperator that = (PhysicalSplitProduceOperator) o;
        return Objects.equals(splitId, that.splitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), splitId);
    }

    @Override
    public String toString() {
        return "PhysicalSplitProduceOperator{" +
                "splitId='" + splitId + '\'' +
                '}';
    }
}
