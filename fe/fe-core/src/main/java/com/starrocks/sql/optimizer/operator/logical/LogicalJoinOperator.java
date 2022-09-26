// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.Objects;

public class LogicalJoinOperator extends LogicalOperator {
    private final JoinOperator joinType;
    private ScalarOperator onPredicate;
    private final String joinHint;
    // For mark the node has been push  down join on clause, avoid dead-loop
    private boolean hasPushDownJoinOnClause = false;
    private boolean hasDeriveIsNotNullPredicate = false;

    public LogicalJoinOperator(JoinOperator joinType, ScalarOperator onPredicate) {
        this(joinType, onPredicate, "", Operator.DEFAULT_LIMIT, null, false);
    }

    private LogicalJoinOperator(JoinOperator joinType, ScalarOperator onPredicate, String joinHint,
                                long limit, ScalarOperator predicate,
                                boolean hasPushDownJoinOnClause) {
        super(OperatorType.LOGICAL_JOIN, limit, predicate, null);
        this.joinType = joinType;
        this.onPredicate = onPredicate;
        Preconditions.checkNotNull(joinHint);
        this.joinHint = joinHint;

        this.hasPushDownJoinOnClause = hasPushDownJoinOnClause;
        this.hasDeriveIsNotNullPredicate = false;
    }

    private LogicalJoinOperator(Builder builder) {
        super(OperatorType.LOGICAL_JOIN, builder.getLimit(), builder.getPredicate(), builder.getProjection());
        this.joinType = builder.joinType;
        this.onPredicate = builder.onPredicate;
        this.joinHint = builder.joinHint;

        this.hasPushDownJoinOnClause = builder.hasPushDownJoinOnClause;
        this.hasDeriveIsNotNullPredicate = builder.hasDeriveIsNotNullPredicate;
    }

    // Constructor for UT, don't use this ctor except ut
    public LogicalJoinOperator() {
        super(OperatorType.LOGICAL_JOIN);
        this.onPredicate = null;
        this.joinType = JoinOperator.INNER_JOIN;
        this.joinHint = "";
    }

    public boolean hasPushDownJoinOnClause() {
        return hasPushDownJoinOnClause;
    }

    public void setHasPushDownJoinOnClause(boolean hasPushDownJoinOnClause) {
        this.hasPushDownJoinOnClause = hasPushDownJoinOnClause;
    }

    public boolean hasDeriveIsNotNullPredicate() {
        return hasDeriveIsNotNullPredicate;
    }

    public void setHasDeriveIsNotNullPredicate(boolean hasDeriveIsNotNullPredicate) {
        this.hasDeriveIsNotNullPredicate = hasDeriveIsNotNullPredicate;
    }

    public JoinOperator getJoinType() {
        return joinType;
    }

    public boolean isInnerOrCrossJoin() {
        return joinType.isInnerJoin() || joinType.isCrossJoin();
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

    public ColumnRefSet getRequiredChildInputColumns() {
        ColumnRefSet result = new ColumnRefSet();
        if (onPredicate != null) {
            result.union(onPredicate.getUsedColumns());
        }
        if (predicate != null) {
            result.union(predicate.getUsedColumns());
        }

        if (projection != null) {
            projection.getColumnRefMap().values().forEach(s -> result.union(s.getUsedColumns()));
            result.except(new ColumnRefSet(new ArrayList<>(projection.getCommonSubOperatorMap().keySet())));
            projection.getCommonSubOperatorMap().values().forEach(s -> result.union(s.getUsedColumns()));
        }
        return result;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        if (projection != null) {
            return new ColumnRefSet(projection.getOutputColumns());
        } else {
            ColumnRefSet columns = new ColumnRefSet();
            for (int i = 0; i < expressionContext.arity(); ++i) {
                columns.union(expressionContext.getChildLogicalProperty(i).getOutputColumns());
            }
            return columns;
        }
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalJoin(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalJoin(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        LogicalJoinOperator rhs = (LogicalJoinOperator) o;

        return joinType == rhs.joinType && Objects.equals(onPredicate, rhs.onPredicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), joinType, onPredicate);
    }

    @Override
    public String toString() {
        return "LOGICAL_JOIN" + " {" +
                joinType.toString() +
                ", onPredicate = " + onPredicate + ' ' +
                ", Predicate = " + predicate +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends LogicalOperator.Builder<LogicalJoinOperator, LogicalJoinOperator.Builder> {
        private JoinOperator joinType;
        private ScalarOperator onPredicate;
        private String joinHint = "";
        private boolean hasPushDownJoinOnClause = false;
        private boolean hasDeriveIsNotNullPredicate = false;

        @Override
        public LogicalJoinOperator build() {
            return new LogicalJoinOperator(this);
        }

        @Override
        public LogicalJoinOperator.Builder withOperator(LogicalJoinOperator joinOperator) {
            super.withOperator(joinOperator);
            this.joinType = joinOperator.joinType;
            this.onPredicate = joinOperator.onPredicate;
            this.joinHint = joinOperator.joinHint;
            this.hasPushDownJoinOnClause = joinOperator.hasPushDownJoinOnClause;
            this.hasDeriveIsNotNullPredicate = joinOperator.hasDeriveIsNotNullPredicate;
            return this;
        }

        public Builder setJoinType(JoinOperator joinType) {
            this.joinType = joinType;
            return this;
        }

        public Builder setOnPredicate(ScalarOperator onPredicate) {
            this.onPredicate = onPredicate;
            return this;
        }

        public Builder setProjection(Projection projection) {
            this.projection = projection;
            return this;
        }

        public Builder setJoinHint(String joinHint) {
            this.joinHint = joinHint;
            return this;
        }
    }
}
