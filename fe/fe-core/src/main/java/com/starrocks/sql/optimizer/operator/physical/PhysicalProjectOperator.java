// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.base.Objects;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

public class PhysicalProjectOperator extends PhysicalOperator {
    private final Map<ColumnRefOperator, ScalarOperator> columnRefMap;
    // Used for common operator compute result reuse
    private final Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap;

    public PhysicalProjectOperator(Map<ColumnRefOperator, ScalarOperator> columnRefMap,
                                   Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap) {
        super(OperatorType.PHYSICAL_PROJECT);
        this.columnRefMap = columnRefMap;
        this.commonSubOperatorMap = commonSubOperatorMap;
    }

    public Map<ColumnRefOperator, ScalarOperator> getColumnRefMap() {
        return columnRefMap;
    }

    public Map<ColumnRefOperator, ScalarOperator> getCommonSubOperatorMap() {
        return commonSubOperatorMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        PhysicalProjectOperator that = (PhysicalProjectOperator) o;
        return Objects.equal(columnRefMap, that.columnRefMap) &&
                Objects.equal(commonSubOperatorMap, that.commonSubOperatorMap);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), columnRefMap, commonSubOperatorMap);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalProject(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalProject(optExpression, context);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet set = super.getUsedColumns();
        columnRefMap.values().forEach(d -> set.union(d.getUsedColumns()));
        commonSubOperatorMap.values().forEach(d -> set.union(d.getUsedColumns()));
        return set;
    }
}
