// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.Map;

public class PhysicalJDBCScanOperator extends PhysicalScanOperator {
    public PhysicalJDBCScanOperator(Table table,
                                    Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                    long limit,
                                    ScalarOperator predicate,
                                    Projection projection) {
        super(OperatorType.PHYSICAL_JDBC_SCAN, table, colRefToColumnMetaMap, limit, predicate,
                projection);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalJDBCScan(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalJDBCScan(optExpression, context);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PhysicalJDBCScanOperator");
        sb.append(" {").append("table=").append(((JDBCTable) table).getJdbcTable())
                .append(", outputColumns=").append(new ArrayList<>(colRefToColumnMetaMap.keySet()))
                .append(", predicate=").append(predicate)
                .append(", limit=").append(limit)
                .append("}");
        return sb.toString();
    }
}
