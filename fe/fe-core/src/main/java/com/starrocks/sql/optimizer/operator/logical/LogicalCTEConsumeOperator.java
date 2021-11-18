// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.Map;

/*
 * This operator denotes the place in the query where a CTE is referenced. The number of CTEConsumer
 * nodes is the same as the number of references to CTEs in the query. The id in the CTEConsumer
 * operator corresponds to the CTEProducer to which it refers. There can be multiple CTEConsumers
 * referring to the same CTEProducer.
 * */
public class LogicalCTEConsumeOperator extends LogicalOperator {
    private final String cteId;

    private Map<ColumnRefOperator, ColumnRefOperator> cteOutputColumnRefMap;

    public LogicalCTEConsumeOperator(long limit, ScalarOperator predicate, Projection projection, String cteId,
                                     Map<ColumnRefOperator, ColumnRefOperator> cteOutputColumnRefMap) {
        super(OperatorType.LOGICAL_CTE_CONSUME, limit, predicate, projection);
        this.cteId = cteId;
        this.cteOutputColumnRefMap = cteOutputColumnRefMap;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        if (projection != null) {
            return new ColumnRefSet(new ArrayList<>(projection.getColumnRefMap().keySet()));
        } else {
            return new ColumnRefSet(new ArrayList<>(cteOutputColumnRefMap.keySet()));
        }
    }

    public String getCteId() {
        return cteId;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalCTEConsume(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalCTEConsume(optExpression, context);
    }
}
