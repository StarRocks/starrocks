// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.Objects;

public class LogicalJoinOperator extends LogicalOperator {
    private JoinOperator joinType;
    private ScalarOperator onPredicate;
    private final String joinHint;
    // For mark the node has been push  down join on clause, avoid dead-loop
    private boolean hasPushDownJoinOnClause = false;

    public LogicalJoinOperator(JoinOperator joinType, ScalarOperator onPredicate) {
        this(joinType, onPredicate, "");
    }

    public LogicalJoinOperator(JoinOperator joinType, ScalarOperator onPredicate, String joinHint) {
        super(OperatorType.LOGICAL_JOIN);
        this.joinType = joinType;
        this.onPredicate = onPredicate;
        Preconditions.checkNotNull(joinHint);
        this.joinHint = joinHint;
    }

    public LogicalJoinOperator(JoinOperator joinType, ScalarOperator onPredicate, long limit, String joinHint) {
        super(OperatorType.LOGICAL_JOIN);
        this.joinType = joinType;
        this.onPredicate = onPredicate;
        this.limit = limit;
        Preconditions.checkNotNull(joinHint);
        this.joinHint = joinHint;
    }

    // Constructor for UT, don't use this ctor except ut
    public LogicalJoinOperator() {
        super(OperatorType.LOGICAL_JOIN);
        this.joinType = JoinOperator.INNER_JOIN;
        this.joinHint = "";
    }

    public boolean isHasPushDownJoinOnClause() {
        return hasPushDownJoinOnClause;
    }

    public void setHasPushDownJoinOnClause(boolean hasPushDownJoinOnClause) {
        this.hasPushDownJoinOnClause = hasPushDownJoinOnClause;
    }

    public JoinOperator getJoinType() {
        return joinType;
    }

    public boolean isInnerOrCrossJoin() {
        return joinType.isInnerJoin() || joinType.isCrossJoin();
    }

    public void setJoinType(JoinOperator joinType) {
        this.joinType = joinType;
    }

    public ScalarOperator getOnPredicate() {
        return onPredicate;
    }

    public void setOnPredicate(ScalarOperator onPredicate) {
        this.onPredicate = onPredicate;
    }

    public String getJoinHint() {
        return joinHint;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        if (projection != null) {
            return new ColumnRefSet(new ArrayList<>(projection.getColumnRefMap().keySet()));
        } else {
            ColumnRefSet columns = new ColumnRefSet();
            for (int i = 0; i < expressionContext.arity(); ++i) {
                columns.union(expressionContext.getChildLogicalProperty(i).getOutputColumns());
            }
            return columns;
        }
    }

    public ColumnRefSet getUsedColumns() {
        ColumnRefSet result = new ColumnRefSet();
        if (onPredicate != null) {
            result.union(onPredicate.getUsedColumns());
        }
        if (predicate != null) {
            result.union(predicate.getUsedColumns());
        }

        if (projection != null) {
            for (ScalarOperator value : projection.getColumnRefMap().values()) {
                result.union(value.getUsedColumns());
            }
        }

        return result;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalJoin(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalJoin(optExpression, context);
    }

    public static Long totalTime = 0L;
    @Override
    public boolean equals(Object o) {
        Long start = System.currentTimeMillis();
        if (!(o instanceof LogicalJoinOperator)) {
            return false;
        }

        LogicalJoinOperator rhs = (LogicalJoinOperator) o;
        if (this == rhs) {
            return true;
        }
        boolean b = joinType == rhs.joinType && Objects.equals(onPredicate, rhs.onPredicate)
                && Objects.equals(predicate, rhs.predicate) && Objects.equals(projection, rhs.projection);

        Long end = System.currentTimeMillis();
        totalTime += (end - start);
        return b;
    }

    @Override
    public int hashCode() {
        return Objects.hash(opType, joinType, onPredicate, predicate, projection);
    }

    @Override
    public String toString() {
        return "LOGICAL_JOIN" + " {" +
                joinType.toString() +
                ", onPredicate = " + onPredicate + ' ' +
                ", Predicate = " + predicate +
                '}';
    }
}
