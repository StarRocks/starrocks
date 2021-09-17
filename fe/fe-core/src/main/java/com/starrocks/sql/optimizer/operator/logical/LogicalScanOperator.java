// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class LogicalScanOperator extends LogicalOperator {
    protected final Table table;
    /**
     * colRefToColumnMetaMap is the map from column reference to starrocks column in meta
     * The ColumnRefMap contains Scan output columns and predicate used columns
     */
    protected Map<ColumnRefOperator, Column> colRefToColumnMetaMap;
    protected Map<Column, ColumnRefOperator> columnMetaToColRefMap;

    protected Map<String, PartitionColumnFilter> columnFilters;

    public LogicalScanOperator(OperatorType type, Table table, Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        super(type);
        this.table = Objects.requireNonNull(table, "table is null");
        this.columnFilters = Maps.newHashMap();

        this.colRefToColumnMetaMap = ImmutableMap.copyOf(colRefToColumnMetaMap);
        this.columnMetaToColRefMap = new HashMap<>();
        for (Map.Entry<ColumnRefOperator, Column> entry : colRefToColumnMetaMap.entrySet()) {
            columnMetaToColRefMap.put(entry.getValue(), entry.getKey());
        }
    }

    public Table getTable() {
        return table;
    }

    public List<ColumnRefOperator> getOutputColumns() {
        if (projection != null) {
            return new ArrayList<>(projection.getColumnRefMap().keySet());
        } else {
            return new ArrayList<>(colRefToColumnMetaMap.keySet());
        }
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        if (projection != null) {
            return new ColumnRefSet(new ArrayList<>(projection.getColumnRefMap().keySet()));
        } else {
            return new ColumnRefSet(new ArrayList<>(colRefToColumnMetaMap.keySet()));
        }
    }

    public Map<ColumnRefOperator, Column> getColRefToColumnMetaMap() {
        return colRefToColumnMetaMap;
    }

    public void setColRefToColumnMetaMap(Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        this.colRefToColumnMetaMap = ImmutableMap.copyOf(colRefToColumnMetaMap);
        columnMetaToColRefMap.clear();
        for (Map.Entry<ColumnRefOperator, Column> entry : colRefToColumnMetaMap.entrySet()) {
            columnMetaToColRefMap.put(entry.getValue(), entry.getKey());
        }
    }

    public ColumnRefOperator getColumnReference(Column column) {
        return columnMetaToColRefMap.get(column);
    }

    public Map<String, PartitionColumnFilter> getColumnFilters() {
        return columnFilters;
    }

    public void setColumnFilters(Map<String, PartitionColumnFilter> columnFilters) {
        this.columnFilters = columnFilters;
    }

    @Override
    public String toString() {
        return "LogicalScanOperator" + " {" +
                "table='" + table.getId() + '\'' +
                ", outputColumns='" + projection + '\'' +
                '}';
    }

    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalTableScan(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        LogicalScanOperator that = (LogicalScanOperator) o;
        return Objects.equals(table, that.table) && Objects.equals(colRefToColumnMetaMap.keySet(), that.colRefToColumnMetaMap.keySet()) &&
                Objects.equals(columnFilters, that.columnFilters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), table, colRefToColumnMetaMap, columnFilters);
    }
}
