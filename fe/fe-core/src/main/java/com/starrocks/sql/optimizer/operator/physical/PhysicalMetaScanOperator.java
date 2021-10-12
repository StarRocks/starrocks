// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.base.Objects;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.Map;

public class PhysicalMetaScanOperator extends PhysicalOperator {
    private final Table table;
    private final Map<ColumnRefOperator, Column> colRefToColumnMetaMap;
    private final Map<Integer, String> aggColumnIdToNames;

    public PhysicalMetaScanOperator(Table table, Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                    Map<Integer, String> aggColumnIdToNames) {
        super(OperatorType.PHYSICAL_META_SCAN);

        this.table = table;
        this.colRefToColumnMetaMap = colRefToColumnMetaMap;
        this.aggColumnIdToNames = aggColumnIdToNames;
    }

    public Table getTable() {
        return table;
    }

    public Map<ColumnRefOperator, Column> getColRefToColumnMetaMap() {
        return colRefToColumnMetaMap;
    }

    public Map<Integer, String> getAggColumnIdToNames() {
        return aggColumnIdToNames;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalMetaScan(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalMetaScan(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalMetaScanOperator that = (PhysicalMetaScanOperator) o;
        return Objects.equal(table, that.table) && Objects.equal(colRefToColumnMetaMap, that.colRefToColumnMetaMap);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(table, colRefToColumnMetaMap);
    }
}