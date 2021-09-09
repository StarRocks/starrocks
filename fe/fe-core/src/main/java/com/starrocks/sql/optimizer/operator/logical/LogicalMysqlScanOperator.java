// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;
import java.util.Map;

public class LogicalMysqlScanOperator extends LogicalScanOperator {
    public LogicalMysqlScanOperator(Table table, List<ColumnRefOperator> outputColumns,
                                    Map<ColumnRefOperator, Column> columnRefMap) {
        super(OperatorType.LOGICAL_MYSQL_SCAN, table, outputColumns, columnRefMap);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalMysqlScan(this, context);
    }
}
