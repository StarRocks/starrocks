// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LogicalIcebergScanOperator extends LogicalScanOperator {
    private final Table.TableType tableType;

    private List<ScalarOperator> conjuncts = Lists.newArrayList();
    // List of conjuncts for min/max values that are used to skip data when scanning Parquet/Orc files.
    private List<ScalarOperator> minMaxConjuncts = new ArrayList<>();
    // Map of columnRefOperator to column which column in minMaxConjuncts
    private Map<ColumnRefOperator, Column> minMaxColumnRefMap = Maps.newHashMap();

    public LogicalIcebergScanOperator(Table table,
                                      Table.TableType tableType,
                                      Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                      Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                      long limit,
                                      ScalarOperator predicate) {
        super(OperatorType.LOGICAL_ICEBERG_SCAN,
                table,
                colRefToColumnMetaMap,
                columnMetaToColRefMap,
                limit,
                predicate, null);

        Preconditions.checkState(table instanceof IcebergTable);
        this.tableType = tableType;
    }

    private LogicalIcebergScanOperator(LogicalIcebergScanOperator.Builder builder) {
        super(OperatorType.LOGICAL_ICEBERG_SCAN,
                builder.table,
                builder.colRefToColumnMetaMap,
                builder.columnMetaToColRefMap,
                builder.getLimit(),
                builder.getPredicate(),
                builder.getProjection());

        this.tableType = builder.tableType;
        this.conjuncts = builder.conjuncts;
        this.minMaxConjuncts = builder.minMaxConjuncts;
        this.minMaxColumnRefMap = builder.minMaxColumnRefMap;
    }

    public Table.TableType getTableType() {
        return tableType;
    }

    public List<ScalarOperator> getConjuncts() {
        return conjuncts;
    }

    public List<ScalarOperator> getMinMaxConjuncts() {
        return minMaxConjuncts;
    }

    public Map<ColumnRefOperator, Column> getMinMaxColumnRefMap() {
        return minMaxColumnRefMap;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalIcebergScan(this, context);
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalIcebergScanOperator, LogicalIcebergScanOperator.Builder> {
        private Table.TableType tableType;
        private List<ScalarOperator> conjuncts = Lists.newArrayList();
        private List<ScalarOperator> minMaxConjuncts = new ArrayList<>();
        private Map<ColumnRefOperator, Column> minMaxColumnRefMap = Maps.newHashMap();

        @Override
        public LogicalIcebergScanOperator build() {
            return new LogicalIcebergScanOperator(this);
        }

        @Override
        public LogicalIcebergScanOperator.Builder withOperator(LogicalIcebergScanOperator scanOperator) {
            super.withOperator(scanOperator);

            this.tableType = scanOperator.tableType;
            this.conjuncts = scanOperator.conjuncts;
            this.minMaxConjuncts = scanOperator.minMaxConjuncts;
            this.minMaxColumnRefMap = scanOperator.minMaxColumnRefMap;
            return this;
        }
    }
}
