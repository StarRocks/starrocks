// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.ColumnFilterConverter;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class LogicalScanOperator extends LogicalOperator {
    protected final Table table;
    protected final ImmutableList<ColumnRefOperator> outputColumns;
    /**
     * colRefToColumnMetaMap is the map from column reference to StarRocks column in meta
     * The ColumnRefMap contains Scan output columns and predicate used columns
     */
    protected final ImmutableMap<ColumnRefOperator, Column> colRefToColumnMetaMap;
    protected final ImmutableMap<Column, ColumnRefOperator> columnMetaToColRefMap;
    protected final ImmutableMap<String, PartitionColumnFilter> columnFilters;

    public LogicalScanOperator(
            OperatorType type,
            Table table,
            List<ColumnRefOperator> outputColumns,
            Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
            Map<Column, ColumnRefOperator> columnMetaToColRefMap,
            long limit,
            ScalarOperator predicate) {
        super(type, limit, predicate);
        this.table = Objects.requireNonNull(table, "table is null");
        this.outputColumns = ImmutableList.copyOf(outputColumns);
        this.colRefToColumnMetaMap = ImmutableMap.copyOf(colRefToColumnMetaMap);
        this.columnMetaToColRefMap = ImmutableMap.copyOf(columnMetaToColRefMap);

        this.columnFilters = ImmutableMap.copyOf(
                ColumnFilterConverter.convertColumnFilter(Utils.extractConjuncts(predicate)));
    }

    public Table getTable() {
        return table;
    }

    public Map<ColumnRefOperator, Column> getColRefToColumnMetaMap() {
        return colRefToColumnMetaMap;
    }

    public ColumnRefOperator getColumnReference(Column column) {
        return columnMetaToColRefMap.get(column);
    }

    public Map<Column, ColumnRefOperator> getColumnMetaToColRefMap() {
        return columnMetaToColRefMap;
    }

    public Map<String, PartitionColumnFilter> getColumnFilters() {
        return columnFilters;
    }

    public List<ColumnRefOperator> getOutputColumns() {
        return this.outputColumns;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        return new ColumnRefSet(this.outputColumns);
    }

    @Override
    public String toString() {
        return "LogicalScanOperator" + " {" +
                "table='" + table.getId() + '\'' +
                ", outputColumns='" + outputColumns + '\'' +
                '}';
    }

    @Override
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
        return Objects.equals(table.getId(), that.table.getId()) && outputColumns.equals(that.outputColumns) &&
                Objects.equals(colRefToColumnMetaMap.keySet(), that.getColRefToColumnMetaMap().keySet());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), table.getId(), outputColumns, colRefToColumnMetaMap.keySet());
    }
}
