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
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.ColumnOutputInfo;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.property.DomainProperty;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A logical marker operator for incremental maintenance rewrite.
 * It should be eliminated by IVM delta rewrite rules before physical optimization.
 */
public class LogicalDeltaOperator extends LogicalOperator {
    private boolean isRootDelta;
    private ColumnRefOperator actionColumn;
    private Map<ColumnRefOperator, Column> mvColumnMapping;

    public LogicalDeltaOperator() {
        this(false, null, Maps.newHashMap());
    }

    public LogicalDeltaOperator(boolean isRootDelta, ColumnRefOperator actionColumn) {
        this(isRootDelta, actionColumn, Maps.newHashMap());
    }

    public LogicalDeltaOperator(boolean isRootDelta, ColumnRefOperator actionColumn,
                                Map<ColumnRefOperator, Column> mvColumnMapping) {
        super(OperatorType.LOGICAL_DELTA);
        Preconditions.checkArgument(actionColumn == null || !actionColumn.isNullable(),
                "Logical delta action column must be non-nullable");
        this.isRootDelta = isRootDelta;
        this.actionColumn = actionColumn;
        this.mvColumnMapping = Collections.unmodifiableMap(
                mvColumnMapping == null ? Maps.newHashMap() : Maps.newHashMap(mvColumnMapping));
    }

    public boolean isRootDelta() {
        return isRootDelta;
    }

    public ColumnRefOperator getActionColumn() {
        return actionColumn;
    }

    public Map<ColumnRefOperator, Column> getMvColumnMapping() {
        return mvColumnMapping;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        ColumnRefSet outputColumns = expressionContext.getChildLogicalProperty(0).getOutputColumns().clone();
        if (actionColumn != null) {
            outputColumns.union(actionColumn);
        }
        return outputColumns;
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        RowOutputInfo rowOutputInfo = projectInputRow(inputs.get(0).getRowOutputInfo());
        if (actionColumn == null) {
            return rowOutputInfo;
        }
        return rowOutputInfo.addColsToRow(List.of(new ColumnOutputInfo(actionColumn, actionColumn)), false);
    }

    @Override
    public DomainProperty deriveDomainProperty(List<OptExpression> inputs) {
        return inputs.get(0).getDomainProperty();
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalDelta(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalDelta(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!super.equals(o)) {
            return false;
        }
        LogicalDeltaOperator that = (LogicalDeltaOperator) o;
        return isRootDelta == that.isRootDelta
                && java.util.Objects.equals(actionColumn, that.actionColumn)
                && java.util.Objects.equals(mvColumnMapping, that.mvColumnMapping);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(super.hashCode(), isRootDelta, actionColumn, mvColumnMapping);
    }

    public static class Builder extends LogicalOperator.Builder<LogicalDeltaOperator, LogicalDeltaOperator.Builder> {
        @Override
        protected LogicalDeltaOperator newInstance() {
            return new LogicalDeltaOperator();
        }

        @Override
        public Builder withOperator(LogicalDeltaOperator operator) {
            super.withOperator(operator);
            builder.isRootDelta = operator.isRootDelta;
            builder.actionColumn = operator.actionColumn;
            builder.mvColumnMapping = operator.mvColumnMapping;

            return this;
        }

        public Builder setRootDelta(boolean isRootDelta) {
            builder.isRootDelta = isRootDelta;
            return this;
        }

        public Builder setActionColumn(ColumnRefOperator actionColumn) {
            builder.actionColumn = actionColumn;
            return this;
        }
    }

}
