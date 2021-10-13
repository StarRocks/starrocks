// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;

public class PhysicalSchemaScanOperator extends PhysicalScanOperator {
    public PhysicalSchemaScanOperator(Table table,
                                      List<ColumnRefOperator> outputColumns,
                                      Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                      long limit,
                                      ScalarOperator predicate) {
        super(OperatorType.PHYSICAL_SCHEMA_SCAN, table, outputColumns, colRefToColumnMetaMap, limit, predicate);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalSchemaScan(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalSchemaScan(optExpression, context);
    }
}
