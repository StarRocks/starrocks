// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.operator.NodeOperator;
import com.starrocks.sql.optimizer.operator.OperatorType;

public abstract class LogicalOperator extends NodeOperator {
    protected LogicalOperator(OperatorType opType) {
        super(opType);
    }

    @Override
    public boolean isLogical() {
        return true;
    }
}


