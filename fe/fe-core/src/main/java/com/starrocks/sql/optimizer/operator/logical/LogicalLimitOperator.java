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
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;

import java.util.ArrayList;
import java.util.List;

public class LogicalLimitOperator extends LogicalOperator {
    public enum Phase {
        INIT, // Init will split to LOCAL/GLOBAL
        LOCAL,  // Only push down LOCAL limit
        GLOBAL, // GLOBAL limit will gather child
    }

    private final long offset;

    private final Phase phase;

    public LogicalLimitOperator(long limit, long offset, Phase phase) {
        super(OperatorType.LOGICAL_LIMIT);
        Preconditions.checkState(limit < 0 || limit + offset >= 0,
                String.format("limit(%d) + offset(%d) is too large and yields an overflow result(%d)", limit, offset,
                        limit + offset));
        this.limit = limit;
        this.offset = offset;
        this.phase = phase;
    }

    public static LogicalLimitOperator init(long limit) {
        return new LogicalLimitOperator(limit, DEFAULT_OFFSET, Phase.INIT);
    }

    public static LogicalLimitOperator init(long limit, long offset) {
        return new LogicalLimitOperator(limit, offset, Phase.INIT);
    }

    public static LogicalLimitOperator global(long limit) {
        return global(limit, DEFAULT_OFFSET);
    }

    public static LogicalLimitOperator global(long limit, long offset) {
        return new LogicalLimitOperator(limit, offset, Phase.GLOBAL);
    }

    public static LogicalLimitOperator local(long limit) {
        return local(limit, DEFAULT_OFFSET);
    }

    public static LogicalLimitOperator local(long limit, long offset) {
        return new LogicalLimitOperator(limit, offset, Phase.LOCAL);
    }

    private LogicalLimitOperator(Builder builder) {
        super(OperatorType.LOGICAL_LIMIT, builder.getLimit(), builder.getPredicate(), builder.getProjection());
        this.limit = builder.getLimit();
        this.offset = builder.offset;
        this.phase = builder.phase;
    }

    public boolean hasOffset() {
        return offset > DEFAULT_OFFSET;
    }

    public long getOffset() {
        return offset;
    }

    public Phase getPhase() {
        return phase;
    }

    public boolean isInit() {
        return phase == Phase.INIT;
    }

    public boolean isLocal() {
        return phase == Phase.LOCAL;
    }

    public boolean isGlobal() {
        return phase == Phase.GLOBAL;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        if (projection != null) {
            return new ColumnRefSet(new ArrayList<>(projection.getColumnRefMap().keySet()));
        } else {
            return expressionContext.getChildLogicalProperty(0).getOutputColumns();
        }
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        return projectInputRow(inputs.get(0).getRowOutputInfo());
    }

    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalLimit(optExpression, context);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalLimit(this, context);
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    public static class Builder extends LogicalOperator.Builder<LogicalLimitOperator, LogicalLimitOperator.Builder> {
        private long offset = DEFAULT_OFFSET;

        private Phase phase = Phase.INIT;

        @Override
        public LogicalLimitOperator build() {
            return new LogicalLimitOperator(this);
        }

        @Override
        public LogicalLimitOperator.Builder withOperator(LogicalLimitOperator operator) {
            super.withOperator(operator);
            this.offset = operator.offset;
            this.phase = operator.phase;
            return this;
        }

        public void setPhase(Phase phase) {
            this.phase = phase;
        }

        public Builder setOffset(long offset) {
            this.offset = offset;
            return this;
        }
    }
}
