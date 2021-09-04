// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.base.Objects;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.external.elasticsearch.EsShardPartitions;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;
import java.util.Map;

public class PhysicalEsScan extends PhysicalOperator {
    private final Table table;
    private final Map<ColumnRefOperator, Column> columnRefMap;
    private final List<EsShardPartitions> selectedIndex;

    public PhysicalEsScan(Table table, Map<ColumnRefOperator, Column> columnRefMap,
                          List<EsShardPartitions> selectedIndex) {
        super(OperatorType.PHYSICAL_ES_SCAN);
        this.table = table;
        this.columnRefMap = columnRefMap;
        this.selectedIndex = selectedIndex;
    }

    public Table getTable() {
        return table;
    }

    public Map<ColumnRefOperator, Column> getColumnRefMap() {
        return columnRefMap;
    }

    public List<EsShardPartitions> getSelectedIndex() {
        return this.selectedIndex;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalEsScan(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalEsScan(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalEsScan that = (PhysicalEsScan) o;
        return Objects.equal(table, that.table) && Objects.equal(columnRefMap, that.columnRefMap) &&
                Objects.equal(selectedIndex, that.selectedIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(table, columnRefMap, selectedIndex);
    }
}
