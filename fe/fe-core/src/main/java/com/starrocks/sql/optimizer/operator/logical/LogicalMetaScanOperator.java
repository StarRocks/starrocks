// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class LogicalMetaScanOperator extends LogicalScanOperator {
    private final ImmutableMap<Integer, String> aggColumnIdToNames;

    public LogicalMetaScanOperator(Table table,
                                   List<ColumnRefOperator> outputColumns,
                                   Map<ColumnRefOperator, Column> columnRefMap) {
        super(OperatorType.LOGICAL_META_SCAN, table, outputColumns, columnRefMap, Maps.newHashMap(),
                -1, null);
        aggColumnIdToNames = ImmutableMap.of();
    }

    public LogicalMetaScanOperator(Table table,
                                   List<ColumnRefOperator> outputColumns,
                                   Map<ColumnRefOperator, Column> columnRefMap,
                                   Map<Integer, String> aggColumnIdToNames) {
        super(OperatorType.LOGICAL_META_SCAN, table, outputColumns, columnRefMap, Maps.newHashMap(),
                -1, null);
        this.aggColumnIdToNames = ImmutableMap.copyOf(aggColumnIdToNames);
    }

    public Map<Integer, String> getAggColumnIdToNames() {
        return aggColumnIdToNames;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalMetaScan(this, context);
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
        LogicalMetaScanOperator that = (LogicalMetaScanOperator) o;
        return table.getId() == that.getTable().getId() &&
                outputColumns.equals(that.outputColumns) &&
                aggColumnIdToNames.equals(that.aggColumnIdToNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table.getId(), outputColumns, aggColumnIdToNames);
    }
}