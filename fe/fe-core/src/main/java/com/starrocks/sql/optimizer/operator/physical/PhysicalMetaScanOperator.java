// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;
import java.util.Objects;

public class PhysicalMetaScanOperator extends PhysicalScanOperator {
    private final Map<Integer, String> aggColumnIdToNames;

    public PhysicalMetaScanOperator(Map<Integer, String> aggColumnIdToNames,
                                    Table table,
                                    Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                    long limit,
                                    ScalarOperator predicate,
                                    Projection projection) {
        super(OperatorType.PHYSICAL_META_SCAN, table, colRefToColumnMetaMap, limit, predicate,
                projection);
        this.aggColumnIdToNames = aggColumnIdToNames;
    }

    public Map<Integer, String> getAggColumnIdToNames() {
        return aggColumnIdToNames;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalMetaScan(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalMetaScan(optExpression, context);
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
        PhysicalMetaScanOperator that = (PhysicalMetaScanOperator) o;
        return Objects.equals(aggColumnIdToNames, that.aggColumnIdToNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), aggColumnIdToNames);
    }

    @Override
    public boolean canUsePipeLine() {
        return false;
    }
}