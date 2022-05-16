// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.OrderSpec;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public class PhysicalTopNOperator extends PhysicalOperator {
    private final long offset;
    private final List<ColumnRefOperator> partitionByColumns;
    private final SortPhase sortPhase;
    private final boolean isSplit;
    private final boolean isEnforced;

    // If limit is -1, means global sort
    public PhysicalTopNOperator(OrderSpec spec, long limit, long offset,
                                List<ColumnRefOperator> partitionByColumns,
                                SortPhase sortPhase,
                                boolean isSplit,
                                boolean isEnforced,
                                ScalarOperator predicate,
                                Projection projection) {
        super(OperatorType.PHYSICAL_TOPN, spec);
        this.limit = limit;
        this.offset = offset;
        this.partitionByColumns = partitionByColumns;
        this.sortPhase = sortPhase;
        this.isSplit = isSplit;
        this.isEnforced = isEnforced;
        this.predicate = predicate;
        this.projection = projection;
    }

    public List<ColumnRefOperator> getPartitionByColumns() {
        return partitionByColumns;
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

    public void fillDisableDictOptimizeColumns(ColumnRefSet resultSet, Set<Integer> dictColIds) {
        // nothing to do
    }

    public boolean couldApplyStringDict(Set<Integer> childDictColumns) {
        Preconditions.checkState(!childDictColumns.isEmpty());
        ColumnRefSet dictSet = new ColumnRefSet();
        for (Integer id : childDictColumns) {
            dictSet.union(id);
        }

        for (Ordering orderDesc : orderSpec.getOrderDescs()) {
            if (orderDesc.getColumnRef().getUsedColumns().isIntersect(dictSet)) {
                return true;
            }
        }

        return false;
    }

}
