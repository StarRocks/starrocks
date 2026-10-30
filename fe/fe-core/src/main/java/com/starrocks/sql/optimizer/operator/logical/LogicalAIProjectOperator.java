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

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.property.DomainProperty;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Logical boundary for evaluating AI calls. The output map may contain only identity pass-through
 * expressions and AI calls; deterministic reusable expressions live in the common map.
 */
public final class LogicalAIProjectOperator extends LogicalOperator {
    private Map<ColumnRefOperator, ScalarOperator> columnRefMap;
    private Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap;

    public LogicalAIProjectOperator(Map<ColumnRefOperator, ScalarOperator> columnRefMap) {
        this(columnRefMap, Map.of());
    }

    public LogicalAIProjectOperator(Map<ColumnRefOperator, ScalarOperator> columnRefMap,
                                    Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap) {
        super(OperatorType.LOGICAL_AI_PROJECT);
        this.columnRefMap = new LinkedHashMap<>(columnRefMap);
        this.commonSubOperatorMap = new LinkedHashMap<>(commonSubOperatorMap);
    }

    private LogicalAIProjectOperator() {
        super(OperatorType.LOGICAL_AI_PROJECT);
    }

    public Map<ColumnRefOperator, ScalarOperator> getColumnRefMap() {
        return columnRefMap;
    }

    public Map<ColumnRefOperator, ScalarOperator> getCommonSubOperatorMap() {
        return commonSubOperatorMap;
    }

    public ColumnRefSet getRequiredChildInputColumns() {
        return new Projection(columnRefMap, commonSubOperatorMap).getUsedColumns();
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        return new ColumnRefSet(new ArrayList<>(columnRefMap.keySet()));
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        return new RowOutputInfo(columnRefMap, commonSubOperatorMap);
    }

    @Override
    public DomainProperty deriveDomainProperty(List<OptExpression> inputs) {
        if (CollectionUtils.isEmpty(inputs)) {
            return new DomainProperty(Map.of());
        }
        return inputs.get(0).getDomainProperty().projectDomainProperty(columnRefMap);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalAIProject(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalAIProject(optExpression, context);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!super.equals(object)) {
            return false;
        }
        LogicalAIProjectOperator that = (LogicalAIProjectOperator) object;
        return Objects.equals(columnRefMap, that.columnRefMap)
                && Objects.equals(commonSubOperatorMap, that.commonSubOperatorMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), columnRefMap, commonSubOperatorMap);
    }

    @Override
    public String toString() {
        return "LogicalAIProjectOperator " + columnRefMap;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder
            extends Operator.Builder<LogicalAIProjectOperator, LogicalAIProjectOperator.Builder> {
        @Override
        protected LogicalAIProjectOperator newInstance() {
            return new LogicalAIProjectOperator();
        }

        @Override
        public Builder withOperator(LogicalAIProjectOperator operator) {
            super.withOperator(operator);
            builder.columnRefMap = new LinkedHashMap<>(operator.getColumnRefMap());
            builder.commonSubOperatorMap = new LinkedHashMap<>(operator.getCommonSubOperatorMap());
            return this;
        }
    }
}
