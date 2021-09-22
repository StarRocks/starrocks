// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class PhysicalScanOperator extends PhysicalOperator {
    protected final Table table;
    private final List<ColumnRefOperator> outputColumns;
    /**
     * ColumnRefMap is the map from column reference to starrocks column in meta
     * The ColumnRefMap contains Scan output columns and predicate used columns
     */
    protected Map<ColumnRefOperator, Column> colRefToColumnMetaMap;
    protected Map<Column, ColumnRefOperator> columnMetaToColRefMap;

    public PhysicalScanOperator(OperatorType type, Table table,
                                List<ColumnRefOperator> outputColumns,
                                Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        super(type);
        this.table = Objects.requireNonNull(table, "table is null");
        this.outputColumns = outputColumns;
        this.colRefToColumnMetaMap = colRefToColumnMetaMap;
        this.columnMetaToColRefMap = new HashMap<>();
        for (Map.Entry<ColumnRefOperator, Column> entry : colRefToColumnMetaMap.entrySet()) {
            columnMetaToColRefMap.put(entry.getValue(), entry.getKey());
        }
    }

    public List<ColumnRefOperator> getOutputColumns() {
        return outputColumns;
    }

    public ColumnRefOperator getColumnReference(Column column) {
        return columnMetaToColRefMap.get(column);
    }

    public Map<ColumnRefOperator, Column> getColRefToColumnMetaMap() {
        return colRefToColumnMetaMap;
    }

    public Table getTable() {
        return table;
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet set = super.getUsedColumns();
        colRefToColumnMetaMap.keySet().forEach(set::union);
        return set;
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
        PhysicalOlapScanOperator that = (PhysicalOlapScanOperator) o;
        return Objects.equals(table.getId(), that.table.getId()) && outputColumns.equals(that.getOutputColumns()) &&
                Objects.equals(colRefToColumnMetaMap, that.getColRefToColumnMetaMap());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), table.getId(), outputColumns, colRefToColumnMetaMap.keySet());
    }
}