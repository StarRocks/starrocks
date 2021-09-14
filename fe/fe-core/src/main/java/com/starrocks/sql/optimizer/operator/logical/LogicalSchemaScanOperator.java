// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.Map;

public class LogicalSchemaScanOperator extends LogicalScanOperator {
    public LogicalSchemaScanOperator(Table table, Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        super(OperatorType.LOGICAL_SCHEMA_SCAN, table, colRefToColumnMetaMap);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalSchemaScan(this, context);
    }
}
