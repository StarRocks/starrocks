// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.ColumnOutputInfo;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class LogicalJoinOperator extends LogicalOperator {
    private final JoinOperator joinType;
    private final ScalarOperator onPredicate;
    private final String joinHint;
    // For mark the node has been push down join on clause, avoid dead-loop
    private boolean hasPushDownJoinOnClause = false;
    private boolean hasDeriveIsNotNullPredicate = false;

    // NOTE: we keep the original onPredicate for MV's rewrite to distinguish on-predicates and
    // where-predicates. Take care to pass through original on-predicates when creating a new JoinOperator.
    private final ScalarOperator originalOnPredicate;

    private int transformMask;

    public LogicalJoinOperator(JoinOperator joinType, ScalarOperator onPredicate) {
        this(joinType, onPredicate, "", Operator.DEFAULT_LIMIT, null, false, onPredicate);
    }

    public LogicalJoinOperator(JoinOperator joinType, ScalarOperator onPredicate, String joinHint) {
        this(joinType, onPredicate, joinHint, Operator.DEFAULT_LIMIT, null, false, onPredicate);
    }

    private LogicalJoinOperator(JoinOperator joinType, ScalarOperator onPredicate, String joinHint,
                                long limit, ScalarOperator predicate,
                                boolean hasPushDownJoinOnClause,
                                ScalarOperator originalOnPredicate) {
        super(OperatorType.LOGICAL_JOIN, limit, predicate, null);
        this.joinType = joinType;
        this.onPredicate = onPredicate;
        Preconditions.checkNotNull(joinHint);
        this.joinHint = StringUtils.upperCase(joinHint);

        this.hasPushDownJoinOnClause = hasPushDownJoinOnClause;
        this.hasDeriveIsNotNullPredicate = false;
        this.originalOnPredicate = originalOnPredicate;
    }

    private LogicalJoinOperator(Builder builder) {
        super(OperatorType.LOGICAL_JOIN, builder.getLimit(), builder.getPredicate(), builder.getProjection());
        this.joinType = builder.joinType;
        this.onPredicate = builder.onPredicate;
        this.joinHint = builder.joinHint;
        this.rowOutputInfo = builder.rowOutputInfo;

        this.hasPushDownJoinOnClause = builder.hasPushDownJoinOnClause;
        this.hasDeriveIsNotNullPredicate = builder.hasDeriveIsNotNullPredicate;
        this.originalOnPredicate = builder.originalOnPredicate;
        this.transformMask = builder.transformMask;
    }

    // Constructor for UT, don't use this ctor except ut
    public LogicalJoinOperator() {
        super(OperatorType.LOGICAL_JOIN);
        this.onPredicate = null;
        this.joinType = JoinOperator.INNER_JOIN;
        this.joinHint = "";
        this.originalOnPredicate = null;
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

    public ScalarOperator getOriginalOnPredicate() {
        if (originalOnPredicate != null) {
            return originalOnPredicate;
        }
        // `onPredicate` maybe null first, but set by `setOnPredicate` later.
        return onPredicate;
    }

    public String getJoinHint() {
        return joinHint;
    }

    public int getTransformMask() {
        return transformMask;
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
    public ColumnRefOperator getSmallestColumn(ColumnRefSet required, ColumnRefFactory columnRefFactory,
                                               OptExpression expr) {
        ColumnRefSet candidate;
        if (joinType.isLeftSemiAntiJoin()) {
            candidate = expr.getChildOutputColumns(0);
        } else if (joinType.isRightSemiAntiJoin()) {
            candidate = expr.getChildOutputColumns(1);
        } else {
            candidate = getOutputColumns(new ExpressionContext(expr));
        }
        if (required != null) {
            candidate.intersect(required);
        }
        return Utils.findSmallestColumnRef(
                candidate.getStream().map(columnRefFactory::getColumnRef).collect(Collectors.toList()));
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
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        List<ColumnOutputInfo> entryList = Lists.newArrayList();
        for (OptExpression input : inputs) {
            for (ColumnOutputInfo entry : input.getRowOutputInfo().getColumnOutputInfo()) {
                entryList.add(new ColumnOutputInfo(entry.getColumnRef(), entry.getColumnRef()));
            }
        }
        return new RowOutputInfo(entryList);
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

        private RowOutputInfo rowOutputInfo;

        private ScalarOperator originalOnPredicate;

        private int transformMask;

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
            this.originalOnPredicate = joinOperator.originalOnPredicate;
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

        public Builder setOriginalOnPredicate(ScalarOperator originalOnPredicate) {
            this.originalOnPredicate = originalOnPredicate;
            return this;
        }

        public Builder setTransformMask(int mask) {
            this.transformMask = mask;
            return this;
        }
    }
}
