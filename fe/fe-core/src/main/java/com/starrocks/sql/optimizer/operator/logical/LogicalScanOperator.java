// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.ColumnFilterConverter;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class LogicalScanOperator extends LogicalOperator {
    protected Table table;

    /**
     * colRefToColumnMetaMap is the map from column reference to StarRocks column in meta
     * The ColumnRefMap contains Scan output columns and predicate used columns
     */
    protected ImmutableMap<ColumnRefOperator, Column> colRefToColumnMetaMap;
    protected ImmutableMap<Column, ColumnRefOperator> columnMetaToColRefMap;
    protected ImmutableMap<String, PartitionColumnFilter> columnFilters;
    protected Set<String> partitionColumns = Sets.newHashSet();
    protected ImmutableList<ColumnAccessPath> columnAccessPaths;
    protected boolean canUseAnyColumn;

    public LogicalScanOperator(
            OperatorType type,
            Table table,
            Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
            Map<Column, ColumnRefOperator> columnMetaToColRefMap,
            long limit,
            ScalarOperator predicate,
            Projection projection) {
        super(type, limit, predicate, projection);
        this.table = Objects.requireNonNull(table, "table is null");
        this.colRefToColumnMetaMap = ImmutableMap.copyOf(colRefToColumnMetaMap);
        this.columnMetaToColRefMap = ImmutableMap.copyOf(columnMetaToColRefMap);
        this.columnAccessPaths = ImmutableList.of();
        buildColumnFilters(predicate);
    }

    protected LogicalScanOperator(OperatorType type) {
        super(type);
        this.colRefToColumnMetaMap = ImmutableMap.of();
        this.columnMetaToColRefMap = ImmutableMap.of();
        this.columnAccessPaths = ImmutableList.of();
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

    private Optional<Map<String, ColumnRefOperator>> cachedColumnNameToColRefMap = Optional.empty();

    public Map<String, ColumnRefOperator> getColumnNameToColRefMap() {
        if (cachedColumnNameToColRefMap.isPresent()) {
            return cachedColumnNameToColRefMap.get();
        }

        Map<String, ColumnRefOperator> columnRefOperatorMap = columnMetaToColRefMap.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().getName(), Map.Entry::getValue));
        cachedColumnNameToColRefMap = Optional.of(columnRefOperatorMap);
        return columnRefOperatorMap;
    }

    public void setCanUseAnyColumn(boolean canUseAnyColumn) {
        this.canUseAnyColumn = canUseAnyColumn;
    }

    public boolean getCanUseAnyColumn() {
        return canUseAnyColumn;
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        return new RowOutputInfo(colRefToColumnMetaMap.keySet().stream()
                .collect(Collectors.toMap(Function.identity(), Function.identity())));
    }

    public void buildColumnFilters(ScalarOperator predicate) {
        this.columnFilters = ImmutableMap.copyOf(
                ColumnFilterConverter.convertColumnFilter(Utils.extractConjuncts(predicate), table));
    }

    public Map<String, PartitionColumnFilter> getColumnFilters() {
        return columnFilters;
    }

    public List<ColumnRefOperator> getOutputColumns() {
        if (projection != null) {
            return projection.getOutputColumns();
        }
        return new ArrayList<>(colRefToColumnMetaMap.keySet());
    }

    public Set<String> getPartitionColumns() {
        return partitionColumns;
    }

    public List<ColumnAccessPath> getColumnAccessPaths() {
        return columnAccessPaths;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        if (projection != null) {
            return new ColumnRefSet(projection.getOutputColumns());
        }
        return new ColumnRefSet(new ArrayList<>(colRefToColumnMetaMap.keySet()));
    }

    @Override
    public String toString() {
        return "LogicalScanOperator" + " {" +
                "table='" + table.getId() + '\'' +
                ", outputColumns='" + new ArrayList<>(colRefToColumnMetaMap.keySet()) + '\'' +
                '}';
    }

    public ScanOperatorPredicates getScanOperatorPredicates() throws AnalysisException {
        throw new AnalysisException("Operation getScanOperatorPredicates() is not supported by this ScanOperator.");
    }

    public void setScanOperatorPredicates(ScanOperatorPredicates predicates) throws AnalysisException {
        throw new AnalysisException("Operation setScanOperatorPredicates(...) is not supported by this ScanOperator.");
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

        if (!super.equals(o)) {
            return false;
        }

        LogicalScanOperator that = (LogicalScanOperator) o;
        return Objects.equals(table.getId(), that.table.getId()) &&
                Objects.equals(colRefToColumnMetaMap.keySet(), that.getColRefToColumnMetaMap().keySet());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), table.getId(), colRefToColumnMetaMap.keySet());
    }

    public abstract static class Builder<O extends LogicalScanOperator, B extends LogicalScanOperator.Builder>
            extends Operator.Builder<O, B> {
        @Override
        public B withOperator(O scanOperator) {
            super.withOperator(scanOperator);
            builder.table = scanOperator.table;
            builder.colRefToColumnMetaMap = scanOperator.colRefToColumnMetaMap;
            builder.columnMetaToColRefMap = scanOperator.columnMetaToColRefMap;
            builder.columnFilters = scanOperator.columnFilters;
            builder.columnAccessPaths = scanOperator.columnAccessPaths;
            builder.canUseAnyColumn = scanOperator.canUseAnyColumn;
            return (B) this;
        }

        @Override
        public O build() {
            builder.columnFilters = ImmutableMap.copyOf(
                    ColumnFilterConverter.convertColumnFilter(Utils.extractConjuncts(builder.predicate),
                            builder.table));
            return super.build();
        }

        public B setColRefToColumnMetaMap(Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
            builder.colRefToColumnMetaMap = ImmutableMap.copyOf(colRefToColumnMetaMap);
            return (B) this;
        }

        public B setColumnMetaToColRefMap(Map<Column, ColumnRefOperator> columnMetaToColRefMap) {
            builder.columnMetaToColRefMap = ImmutableMap.copyOf(columnMetaToColRefMap);
            return (B) this;
        }

        public B setColumnAccessPaths(List<ColumnAccessPath> columnAccessPaths) {
            builder.columnAccessPaths = ImmutableList.copyOf(columnAccessPaths);
            return (B) this;
        }

        public B setTable(Table table) {
            builder.table = table;
            return (B) this;
        }
    }
}
