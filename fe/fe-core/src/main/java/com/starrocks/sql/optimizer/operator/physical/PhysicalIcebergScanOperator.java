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
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

public class PhysicalIcebergScanOperator extends PhysicalScanOperator {
    private ScanOperatorPredicates predicates;

    public PhysicalIcebergScanOperator(Table table,
                                       Map<ColumnRefOperator, Column> columnRefMap,
                                       ScanOperatorPredicates predicates,
                                       long limit,
                                       ScalarOperator predicate,
                                       Projection projection) {
        super(OperatorType.PHYSICAL_ICEBERG_SCAN, table, columnRefMap, limit, predicate, projection);
        this.predicates = predicates;
    }

    @Override
    public ScanOperatorPredicates getScanOperatorPredicates() {
        return this.predicates;
    }

    @Override
    public void setScanOperatorPredicates(ScanOperatorPredicates predicates) {
        this.predicates = predicates;
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
        ScanOperatorPredicates targetPredicts = ((PhysicalIcebergScanOperator) o).getScanOperatorPredicates();
        return Objects.equal(table, that.table) && predicates.equals(targetPredicts);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), table, predicates);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet refs = super.getUsedColumns();
        predicates.getNoEvalPartitionConjuncts().forEach(d -> refs.union(d.getUsedColumns()));
        predicates.getPartitionConjuncts().forEach(d -> refs.union(d.getUsedColumns()));
        predicates.getMinMaxConjuncts().forEach(d -> refs.union(d.getUsedColumns()));
        predicates.getMinMaxColumnRefMap().keySet().forEach(refs::union);
        return refs;
    }
}
