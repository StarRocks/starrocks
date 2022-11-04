// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
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
import java.util.Set;

public abstract class LogicalScanOperator extends LogicalOperator {
    protected final Table table;
    private final Table.TableType tableType = Table.TableType.OLAP;

    /**
     * colRefToColumnMetaMap is the map from column reference to StarRocks column in meta
     * The ColumnRefMap contains Scan output columns and predicate used columns
     */
    protected final ImmutableMap<ColumnRefOperator, Column> colRefToColumnMetaMap;
    protected final ImmutableMap<Column, ColumnRefOperator> columnMetaToColRefMap;
    protected ImmutableMap<String, PartitionColumnFilter> columnFilters;
    protected Set<String> partitionColumns = Sets.newHashSet();

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
        buildColumnFilters(predicate);
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
        if (o == null || getClass() != o.getClass()) {
            return false;
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

    abstract static class Builder<O extends LogicalScanOperator, B extends LogicalScanOperator.Builder>
            extends Operator.Builder<O, B> {
        protected Table table;
        protected ImmutableMap<ColumnRefOperator, Column> colRefToColumnMetaMap;
        protected ImmutableMap<Column, ColumnRefOperator> columnMetaToColRefMap;
        protected ImmutableMap<String, PartitionColumnFilter> columnFilters;

        @Override
        public B withOperator(O scanOperator) {
            super.withOperator(scanOperator);

            this.table = scanOperator.table;
            this.colRefToColumnMetaMap = scanOperator.colRefToColumnMetaMap;
            this.columnMetaToColRefMap = scanOperator.columnMetaToColRefMap;
            this.columnFilters = scanOperator.columnFilters;
            return (B) this;
        }

        public B setTable(Table table) {
            this.table = table;
            return (B) this;
        }
    }
}
