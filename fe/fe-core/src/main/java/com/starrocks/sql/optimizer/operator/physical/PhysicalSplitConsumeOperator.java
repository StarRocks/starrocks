package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Objects;

public class PhysicalSplitConsumeOperator extends PhysicalOperator {
    private final int splitId;
    private ScalarOperator splitPredicate;

    public PhysicalSplitConsumeOperator(int splitId, ScalarOperator splitPredicate) {
        super(OperatorType.PHYSICAL_SPLIT_CONSUME);
        this.splitId = splitId;
        this.splitPredicate = splitPredicate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        PhysicalSplitConsumeOperator that = (PhysicalSplitConsumeOperator) o;
        return Objects.equals(splitId, that.splitId) &&
                Objects.equals(splitPredicate, that.splitPredicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), splitId, splitPredicate);
    }

    @Override
    public String toString() {
        return "PhysicalSplitConsumeOperator{" +
                "splitId='" + splitId + '\'' +
                ", predicate=" + splitPredicate +
                '}';
    }

}
