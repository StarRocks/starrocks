// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.stream.Collectors;

public abstract class LogicalOperator extends Operator {

    protected LogicalOperator(OperatorType opType) {
        super(opType);
    }

    protected LogicalOperator(OperatorType operatorType, long limit, ScalarOperator predicate, Projection projection) {
        super(operatorType, limit, predicate, projection);
    }

    @Override
    public boolean isLogical() {
        return true;
    }

    public abstract ColumnRefSet getOutputColumns(ExpressionContext expressionContext);

    public ColumnRefOperator getSmallestColumn(ColumnRefSet requiredCandidates,
                                               ColumnRefFactory columnRefFactory,
                                               OptExpression opt) {
        ColumnRefSet outputCandidates = getOutputColumns(new ExpressionContext(opt));
        if (requiredCandidates != null) {
            outputCandidates.intersect(requiredCandidates);
        }
        return Utils.findSmallestColumnRef(outputCandidates.getStream().
                map(columnRefFactory::getColumnRef).collect(Collectors.toList()));
    }

}
