// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;
import java.util.Set;

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
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalProjectOperator that = (PhysicalProjectOperator) o;
        return Objects.equal(columnRefMap, that.columnRefMap) &&
                Objects.equal(commonSubOperatorMap, that.commonSubOperatorMap);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(columnRefMap, commonSubOperatorMap);
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

    @Override
    public boolean couldApplyStringDict(Set<Integer> childDictColumns) {
        Preconditions.checkState(!childDictColumns.isEmpty());
        ColumnRefSet dictSet = new ColumnRefSet();
        for (Integer id : childDictColumns) {
            dictSet.union(id);
        }

        for (ScalarOperator operator : columnRefMap.values()) {
            if (!couldApplyStringDict(operator, dictSet)) {
                return false;
            }
        }

        for (ScalarOperator operator : commonSubOperatorMap.values()) {
            if (!couldApplyStringDict(operator, dictSet)) {
                return false;
            }
        }

        return true;
    }

    private boolean couldApplyStringDict(ScalarOperator operator, ColumnRefSet dictSet) {
        ColumnRefSet usedColumns = operator.getUsedColumns();
        if (usedColumns.isIntersect(dictSet)) {
            if (usedColumns.cardinality() > 1) {
                return false;
            }
            return operator instanceof ColumnRefOperator;
        }
        return true;
    }
}
