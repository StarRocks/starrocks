// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Objects;

public class PhysicalRepeatOperator extends PhysicalOperator {
    private final List<ColumnRefOperator> outputGrouping;
    private final List<List<ColumnRefOperator>> repeatColumnRef;
    private final List<List<Long>> groupingIds;

    public PhysicalRepeatOperator(List<ColumnRefOperator> outputGrouping, List<List<ColumnRefOperator>> repeatColumnRef,
                                  List<List<Long>> groupingIds, long limit,
                                  ScalarOperator predicate,
                                  Projection projection) {
        super(OperatorType.PHYSICAL_REPEAT);
        this.outputGrouping = outputGrouping;
        this.repeatColumnRef = repeatColumnRef;
        this.groupingIds = groupingIds;
        this.limit = limit;
        this.predicate = predicate;
        this.projection = projection;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalRepeat(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalRepeat(optExpression, context);
    }

    public List<List<ColumnRefOperator>> getRepeatColumnRef() {
        return repeatColumnRef;
    }

    public List<ColumnRefOperator> getOutputGrouping() {
        return outputGrouping;
    }

    public List<List<Long>> getGroupingIds() {
        return groupingIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalRepeatOperator that = (PhysicalRepeatOperator) o;
        return Objects.equals(outputGrouping, that.outputGrouping) &&
                Objects.equals(repeatColumnRef, that.repeatColumnRef);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputGrouping, repeatColumnRef);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet set = super.getUsedColumns();
        outputGrouping.forEach(set::union);
        repeatColumnRef.forEach(s -> s.forEach(set::union));
        return set;
    }
}
