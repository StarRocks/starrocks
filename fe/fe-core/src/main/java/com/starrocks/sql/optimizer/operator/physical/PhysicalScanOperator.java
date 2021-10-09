// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class PhysicalScanOperator extends PhysicalOperator {
    protected final Table table;
    protected final ImmutableList<ColumnRefOperator> outputColumns;
    /**
     * ColumnRefMap is the map from column reference to starrocks column in meta
     * The ColumnRefMap contains Scan output columns and predicate used columns
     */
    protected final ImmutableMap<ColumnRefOperator, Column> colRefToColumnMetaMap;

    public PhysicalScanOperator(OperatorType type, Table table,
                                List<ColumnRefOperator> outputColumns,
                                Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                long limit,
                                ScalarOperator predicate,
                                Projection projection) {
        super(type);
        this.table = Objects.requireNonNull(table, "table is null");
        this.outputColumns = ImmutableList.copyOf(outputColumns);
        this.colRefToColumnMetaMap = ImmutableMap.copyOf(colRefToColumnMetaMap);
        this.limit = limit;
        this.predicate = predicate;
        this.projection = projection;
    }

    public List<ColumnRefOperator> getOutputColumns() {
        return outputColumns;
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