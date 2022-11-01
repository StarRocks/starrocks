// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.physical.stream;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;

public class PhysicalStreamScanOperator extends PhysicalOperator {

    protected PhysicalStreamScanOperator() {
        super(OperatorType.PHYSICAL_STREAM_SCAN);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalStreamScan(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalStreamScan(optExpression, context);
    }

    @Override
    public String toString() {
        return "PhysicalStreamScanOperator";
    }

}
