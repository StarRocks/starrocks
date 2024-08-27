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
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

// the logical operator to scan view just like LogicalOlapScanOperator to scan olap table,
// which is a virtual logical operator used by view based mv rewrite and has no corresponding physical operator.
// So the final plan will never contain an operator of this type.
public class LogicalViewScanOperator  extends LogicalScanOperator {
    private int relationId;
    private ColumnRefSet outputColumnSet;
    private Map<Expr, ColumnRefOperator> expressionToColumns;
    // Original plan evaluator(inlined view) for the input logical plan tree which has not done rule based rewrite yet,
    // this is only used when the view scan operator cannot be rewritten by view-based rewrite rules.
    private OptPlanEvaluator optPlanEvaluator;

    public LogicalViewScanOperator(
            int relationId,
            Table table,
            Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
            Map<Column, ColumnRefOperator> columnMetaToColRefMap,
            ColumnRefSet outputColumnSet,
            Map<Expr, ColumnRefOperator> expressionToColumns) {
        super(OperatorType.LOGICAL_VIEW_SCAN, table, colRefToColumnMetaMap,
                columnMetaToColRefMap, Operator.DEFAULT_LIMIT, null, null);
        this.relationId = relationId;
        this.outputColumnSet = outputColumnSet;
        this.expressionToColumns = expressionToColumns;
    }

    private LogicalViewScanOperator() {
        super(OperatorType.LOGICAL_VIEW_SCAN);
    }

    public ColumnRefOperator getExpressionMapping(Expr expr) {
        if (expressionToColumns == null) {
            return null;
        }
        return expressionToColumns.get(expr);
    }

    public ColumnRefSet getOutputColumnSet() {
        return outputColumnSet;
    }

    public int getRelationId() {
        return relationId;
    }

    public void setOriginalPlanEvaluator(OptPlanEvaluator optPlanEvaluator) {
        this.optPlanEvaluator = optPlanEvaluator;
    }

    public OptExpression getOriginalPlanEvaluator() {
        return this.optPlanEvaluator.evaluate();
    }

    /**
     * Evaluate the original plan and return the optimized plan for the input logical plan tree which is with inlined view and
     * has not done rule based rewrite yet.
     */
    public static class OptPlanEvaluator {
        private final OptExpression logicalTree;
        private final ConnectContext connectContext;
        private final ColumnRefSet requiredColumns;
        private final ColumnRefFactory columnRefFactory;

        private Optional<OptExpression> originalPlan = Optional.empty();

        public OptPlanEvaluator(OptExpression logicalTree,
                                ConnectContext connectContext,
                                ColumnRefSet requiredColumns,
                                ColumnRefFactory columnRefFactory) {
            this.logicalTree = logicalTree;
            this.connectContext = connectContext;
            this.requiredColumns = requiredColumns;
            this.columnRefFactory = columnRefFactory;
        }

        public OptExpression evaluate() {
            if (!originalPlan.isPresent()) {
                OptExpression optExpression = MvUtils.optimizeViewPlan(logicalTree, connectContext, requiredColumns,
                        columnRefFactory);
                originalPlan = Optional.of(optExpression);
            }
            Preconditions.checkArgument(originalPlan.isPresent(), "Original plan is not set");
            return originalPlan.get();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalViewScanOperator that = (LogicalViewScanOperator) o;
        return relationId == that.relationId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(relationId);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalViewScan(this, context);
    }

    public static LogicalViewScanOperator.Builder builder() {
        return new LogicalViewScanOperator.Builder();
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalViewScanOperator, LogicalViewScanOperator.Builder> {
        @Override
        protected LogicalViewScanOperator newInstance() {
            return new LogicalViewScanOperator();
        }

        @Override
        public LogicalViewScanOperator.Builder withOperator(LogicalViewScanOperator scanOperator) {
            super.withOperator(scanOperator);
            builder.relationId = scanOperator.relationId;
            builder.expressionToColumns = scanOperator.expressionToColumns;
            builder.outputColumnSet = scanOperator.outputColumnSet;
            builder.optPlanEvaluator = scanOperator.optPlanEvaluator;
            return this;
        }
    }
}
