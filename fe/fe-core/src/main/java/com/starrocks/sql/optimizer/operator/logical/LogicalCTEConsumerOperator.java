// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

public class LogicalCTEConsumerOperator extends LogicalOperator {
    private String cteId;

    private Map<ColumnRefOperator, ColumnRefOperator> cteOutputColumnRefMap;

    public LogicalCTEConsumerOperator(String cteId, long limit, ScalarOperator predicate, Projection projection) {
        super(OperatorType.LOGICAL_CTE_CONSUME, limit, predicate, projection);
        this.cteId = cteId;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        return null;
    }

}
