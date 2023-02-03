// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

public class PhysicalDeltaLakeScanOperator extends PhysicalScanOperator {
    private ScanOperatorPredicates predicates;

    public PhysicalDeltaLakeScanOperator(Table table,
                                         Map<ColumnRefOperator, Column> columnRefMap,
                                         ScanOperatorPredicates predicates,
                                         long limit,
                                         ScalarOperator predicate,
                                         Projection projection) {
        super(OperatorType.PHYSICAL_DELTALAKE_SCAN, table, columnRefMap, limit, predicate, projection);
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
        return visitor.visitPhysicalDeltaLakeScan(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalDeltaLakeScan(optExpression, context);
    }

}
