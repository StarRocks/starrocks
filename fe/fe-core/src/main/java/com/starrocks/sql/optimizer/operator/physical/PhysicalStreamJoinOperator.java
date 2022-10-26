// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

public class PhysicalStreamJoinOperator extends PhysicalJoinOperator {
    public PhysicalStreamJoinOperator(JoinOperator joinType, ScalarOperator onPredicate, String joinHint, long limit,
            ScalarOperator predicate, Projection projection) {
        super(OperatorType.PHYSICAL_STREAM_JOIN, joinType, onPredicate, joinHint, limit, predicate, projection);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalStreamJoin(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalStreamJoin(optExpression, context);
    }

    @Override
    public String toString() {
        return "PhysicalStreamJoinOperator{"
                + "joinType=" + joinType + ", joinPredicate=" + onPredicate + ", limit=" + limit
                + ", predicate=" + predicate + '}';
    }

    @Override
    public String getJoinAlgo() {
        return "STREAM_JOIN";
    }
}
