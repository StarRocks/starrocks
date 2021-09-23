// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;

public abstract class LogicalOperator extends Operator {

    protected LogicalOperator(OperatorType opType) {
        super(opType);
    }

    @Override
    public boolean isLogical() {
        return true;
    }

    public abstract ColumnRefSet getOutputColumns(ExpressionContext expressionContext);
}
