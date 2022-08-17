// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.base.Objects;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;

public class PhysicalIcebergScanOperator extends PhysicalScanOperator {
    private final List<ScalarOperator> conjuncts;
    // List of conjuncts for min/max values that are used to skip data when scanning Parquet/Orc files.
    private final List<ScalarOperator> minMaxConjuncts;
    // Map of columnRefOperator to column which column in minMaxConjuncts
    private final Map<ColumnRefOperator, Column> minMaxColumnRefMap;

    public PhysicalIcebergScanOperator(Table table,
                                       Map<ColumnRefOperator, Column> columnRefMap,
                                       List<ScalarOperator> conjuncts,
                                       List<ScalarOperator> minMaxConjuncts,
                                       Map<ColumnRefOperator, Column> minMaxColumnRefMap,
                                       long limit,
                                       ScalarOperator predicate,
                                       Projection projection) {
        super(OperatorType.PHYSICAL_ICEBERG_SCAN, table, columnRefMap, limit, predicate, projection);
        this.conjuncts = conjuncts;
        this.minMaxConjuncts = minMaxConjuncts;
        this.minMaxColumnRefMap = minMaxColumnRefMap;
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
        return visitor.visitPhysicalIcebergScan(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalIcebergScan(optExpression, context);
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

        PhysicalIcebergScanOperator that = (PhysicalIcebergScanOperator) o;
        return Objects.equal(table, that.table) &&
                Objects.equal(conjuncts, that.conjuncts) &&
                Objects.equal(minMaxConjuncts, that.minMaxConjuncts) &&
                Objects.equal(minMaxColumnRefMap, that.minMaxColumnRefMap);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), table, conjuncts, minMaxConjuncts, minMaxColumnRefMap);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet refs = super.getUsedColumns();
        conjuncts.forEach(d -> refs.union(d.getUsedColumns()));
        minMaxConjuncts.forEach(d -> refs.union(d.getUsedColumns()));
        minMaxColumnRefMap.keySet().forEach(refs::union);
        return refs;
    }
}
