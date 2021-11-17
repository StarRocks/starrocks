// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

/*
 * This operator denotes the place in the query where a CTE is referenced. The number of CTEConsumer
 * nodes is the same as the number of references to CTEs in the query. The id in the CTEConsumer
 * operator corresponds to the CTEProducer to which it refers. There can be multiple CTEConsumers
 * referring to the same CTEProducer.
 * */
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
