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

public class PhysicalValuesOperator extends PhysicalOperator {
    private final List<ColumnRefOperator> columnRefSet;
    private final List<List<ScalarOperator>> rows;

    public PhysicalValuesOperator(List<ColumnRefOperator> columnRefSet, List<List<ScalarOperator>> rows,
                                  long limit,
                                  ScalarOperator predicate,
                                  Projection projection) {
        super(OperatorType.PHYSICAL_VALUES);
        this.columnRefSet = columnRefSet;
        this.rows = rows;
        this.limit = limit;
        this.predicate = predicate;
        this.projection = projection;
    }

    public List<ColumnRefOperator> getColumnRefSet() {
        return columnRefSet;
    }

    public List<List<ScalarOperator>> getRows() {
        return rows;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalValues(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalValues(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet set = super.getUsedColumns();
        rows.forEach(r -> r.forEach(s -> set.union(s.getUsedColumns())));
        return set;
    }
}
