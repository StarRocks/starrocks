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
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.property.DomainProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/*
 * CK-compatible logical-sink MV "Fake Root" (Option X). An N-ary, NON-merging container that sits at the plan
 * root: each child is an independent branch sub-plan (a CTE consumer of the shared base load, optionally joined
 * with dimension tables, projected to a target table's shape). The Fake Root does NOT merge the branch data --
 * it exists only to keep the plan single-rooted so the optimizer can generate every branch (incl. joins) end to
 * end. It is translated (PlanFragmentBuilder.visitPhysicalMultiSink) into the collector fragment that reuses
 * MultiSinkDispatchNode + MultiSink, writing branch i to targetTableIds[i]. See CH_REALTIME_MV_DESIGN.md.
 */
public class LogicalMultiSinkOperator extends LogicalOperator {
    // Per-branch (child order) target OLAP table id: branch 0 = base table, the rest = MV target tables.
    private List<Long> targetTableIds;

    public LogicalMultiSinkOperator(List<Long> targetTableIds) {
        super(OperatorType.LOGICAL_MULTI_SINK);
        this.targetTableIds = targetTableIds;
    }

    private LogicalMultiSinkOperator() {
        super(OperatorType.LOGICAL_MULTI_SINK);
    }

    public List<Long> getTargetTableIds() {
        return targetTableIds;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        // Fake root produces nothing meaningful; report child 0's columns so downstream code has a valid set.
        if (projection != null) {
            return new ColumnRefSet(new ArrayList<>(projection.getColumnRefMap().keySet()));
        }
        return expressionContext.getChildLogicalProperty(0).getOutputColumns();
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        return projectInputRow(inputs.get(0).getRowOutputInfo());
    }

    @Override
    public DomainProperty deriveDomainProperty(List<OptExpression> inputs) {
        return new DomainProperty(Map.of());
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalMultiSink(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalMultiSink(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!super.equals(o)) {
            return false;
        }
        LogicalMultiSinkOperator that = (LogicalMultiSinkOperator) o;
        return Objects.equals(targetTableIds, that.targetTableIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), targetTableIds);
    }

    @Override
    public String toString() {
        return "LogicalMultiSinkOperator{targetTableIds=" + targetTableIds + '}';
    }

    public static class Builder
            extends LogicalOperator.Builder<LogicalMultiSinkOperator, LogicalMultiSinkOperator.Builder> {
        @Override
        protected LogicalMultiSinkOperator newInstance() {
            return new LogicalMultiSinkOperator();
        }

        @Override
        public LogicalMultiSinkOperator.Builder withOperator(LogicalMultiSinkOperator operator) {
            super.withOperator(operator);
            builder.targetTableIds = operator.targetTableIds;
            return this;
        }
    }
}
