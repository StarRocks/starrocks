// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.base.Objects;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.Map;

public class PhysicalMysqlScanOperator extends PhysicalScanOperator {

    public PhysicalMysqlScanOperator(Table table, Map<ColumnRefOperator, Column> columnRefMap) {
        super(OperatorType.PHYSICAL_MYSQL_SCAN, table, columnRefMap);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalMysqlScan(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalMysqlScan(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalMysqlScanOperator that = (PhysicalMysqlScanOperator) o;
        return Objects.equal(table, that.table) && Objects.equal(colRefToColumnMetaMap, that.colRefToColumnMetaMap);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(table, colRefToColumnMetaMap);
    }
}
