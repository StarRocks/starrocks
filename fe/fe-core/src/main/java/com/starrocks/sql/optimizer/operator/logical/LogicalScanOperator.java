// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class LogicalScanOperator extends LogicalOperator {
    protected final Table table;

    protected List<ColumnRefOperator> outputColumns;
    /**
     * ColumnRefMap is the map from column reference to starrocks column in meta
     * The ColumnRefMap contains Scan output columns and predicate used columns
     */
    protected Map<ColumnRefOperator, Column> columnRefMap;

    protected Map<String, PartitionColumnFilter> columnFilters;

    public LogicalScanOperator(
            OperatorType type,
            Table table,
            List<ColumnRefOperator> outputColumns,
            Map<ColumnRefOperator, Column> columnRefMap) {
        super(type);
        this.table = Objects.requireNonNull(table, "table is null");
        this.outputColumns = outputColumns;
        this.columnRefMap = columnRefMap;
        this.columnFilters = Maps.newHashMap();
    }

    public Table getTable() {
        return table;
    }

    public Map<ColumnRefOperator, Column> getColumnRefMap() {
        return columnRefMap;
    }

    public List<ColumnRefOperator> getOutputColumns() {
        return this.outputColumns;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        return new ColumnRefSet(this.outputColumns);
    }

    public void setOutputColumns(List<ColumnRefOperator> outputColumns) {
        this.outputColumns = outputColumns;
    }

    public void setColumnRefMap(Map<ColumnRefOperator, Column> columnRefMap) {
        this.columnRefMap = columnRefMap;
    }

    public Map<String, PartitionColumnFilter> getColumnFilters() {
        return columnFilters;
    }

    public void setColumnFilters(Map<String, PartitionColumnFilter> columnFilters) {
        this.columnFilters = columnFilters;
    }

    @Override
    public int hashCode() {
        int hash = 17;
        hash = Utils.combineHash(hash, opType.hashCode());
        hash = Utils.combineHash(hash, (int) table.getId());
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LogicalScanOperator)) {
            return false;
        }

        LogicalScanOperator rhs = (LogicalScanOperator) obj;
        if (this == rhs) {
            return true;
        }

        return table.getId() == rhs.getTable().getId() &&
                outputColumns.equals(rhs.outputColumns);

    }

    @Override
    public String toString() {
        return "LogicalScanOperator" + " {" +
                "table='" + table.getId() + '\'' +
                ", outputColumns='" + outputColumns + '\'' +
                '}';
    }

    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalTableScan(optExpression, context);
    }

    public void tryExtendOutputColumns(Set<ColumnRefOperator> newOutputColumns) {
        if (outputColumns.size() == 0 || newOutputColumns.size() != 0) {
            return;
        }

        int smallestIndex = -1;
        int smallestColumnLength = Integer.MAX_VALUE;
        for (int index = 0; index < outputColumns.size(); ++index) {
            Type columnType = outputColumns.get(index).getType();
            if (smallestIndex == -1) {
                smallestIndex = index;
            }
            if (columnType.isScalarType()) {
                int columnLength = columnType.getSlotSize();
                if (columnLength < smallestColumnLength) {
                    smallestIndex = index;
                    smallestColumnLength = columnLength;
                }
            }
        }
        Preconditions.checkArgument(smallestIndex != -1);
        newOutputColumns.add(outputColumns.get(smallestIndex));
    }
}
