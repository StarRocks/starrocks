// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;

public class LogicalCTEProduceOperator extends LogicalOperator {
    private String cteId;

    public LogicalCTEProduceOperator(String cteId) {
        super(OperatorType.LOGICAL_CTE_PRODUCE);
        this.cteId = cteId;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        return null;
    }
}
