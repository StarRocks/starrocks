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
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.MultiInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;

public class LogicalApplyOperator extends LogicalOperator {
    /**
     * Expressions that use sub-query.
     * Indicates which expressions apply is constructed from
     * <p>
     * x in (select y from t) will convert to
     * col-ref1 in col-ref2(project by inner plan output)
     */
    private final ScalarOperator subqueryOperator;

    /**
     * output columnRef for subqueryOperator
     */
    private final ColumnRefOperator output;

    /**
     * Correlation column which from outer table and used in the filter of sub-query(inner table)
     */
    private final List<ColumnRefOperator> correlationColumnRefs;

    /**
     * Correlation conjuncts, will fill if this is a correlation subquery when push down ApplyNode to Filter
     */
    private final ScalarOperator correlationConjuncts;

    /**
     * For scalar subquery, mark isn't need check subquery's return rows
     */
    private final boolean needCheckMaxRows;

    /**
     * Mark the subquery can be cast to Semi/Anti-Join
     */
    private final boolean useSemiAnti;

    private final boolean needOutputRightChildColumns;

    /**
     * Record un-correlation subquery outer table column, use for push down apply node
     * e.g.
     * SQL: select * from t1 where t1.v1 > (....);
     * OuterPredicateColumns: t1.v1
     */
    private final ColumnRefSet unCorrelationSubqueryPredicateColumns;

    private LogicalApplyOperator(Builder builder) {
        super(OperatorType.LOGICAL_APPLY, builder.getLimit(), builder.getPredicate(), builder.getProjection());
        subqueryOperator = builder.subqueryOperator;
        output = builder.output;
        correlationColumnRefs = builder.correlationColumnRefs;
        correlationConjuncts = builder.correlationConjuncts;
        needCheckMaxRows = builder.needCheckMaxRows;
        useSemiAnti = builder.useSemiAnti;
        needOutputRightChildColumns = builder.needOutputRightChildColumns;
        unCorrelationSubqueryPredicateColumns = builder.unCorrelationSubqueryPredicateColumns;
    }

    public ColumnRefOperator getOutput() {
        return output;
    }

    public boolean isQuantified() {
        return subqueryOperator instanceof InPredicateOperator || subqueryOperator instanceof MultiInPredicateOperator;
    }

    public boolean isExistential() {
        return subqueryOperator instanceof ExistsPredicateOperator;
    }

    public boolean isScalar() {
        return !isQuantified() && !isExistential();
    }

    public ScalarOperator getSubqueryOperator() {
        return subqueryOperator;
    }

    public List<ColumnRefOperator> getCorrelationColumnRefs() {
        return correlationColumnRefs;
    }

    public ScalarOperator getCorrelationConjuncts() {
        return correlationConjuncts;
    }

    public boolean isNeedCheckMaxRows() {
        return needCheckMaxRows;
    }

    public boolean isUseSemiAnti() {
        return useSemiAnti;
    }

    public ColumnRefSet getUnCorrelationSubqueryPredicateColumns() {
        return unCorrelationSubqueryPredicateColumns;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        ColumnRefSet outputColumns = expressionContext.getChildLogicalProperty(0).getOutputColumns().clone();
        if (needOutputRightChildColumns) {
            outputColumns.union(expressionContext.getChildLogicalProperty(1).getOutputColumns());
        } else if (output != null) {
            outputColumns.union(output);
        }
        return outputColumns;
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalApply(optExpression, context);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends LogicalOperator.Builder<LogicalApplyOperator, LogicalApplyOperator.Builder> {
        private ScalarOperator subqueryOperator = null;
        private ColumnRefOperator output = null;
        private List<ColumnRefOperator> correlationColumnRefs = null;
        private ScalarOperator correlationConjuncts = null;
        private boolean useSemiAnti = true;
        private boolean needCheckMaxRows = false;
        private boolean needOutputRightChildColumns = false;
        private ColumnRefSet unCorrelationSubqueryPredicateColumns = null;

        public Builder setSubqueryOperator(ScalarOperator subqueryOperator) {
            this.subqueryOperator = subqueryOperator;
            return this;
        }

        public Builder setOutput(ColumnRefOperator output) {
            this.output = output;
            return this;
        }

        public Builder setCorrelationColumnRefs(List<ColumnRefOperator> correlationColumnRefs) {
            this.correlationColumnRefs = correlationColumnRefs;
            return this;
        }

        public Builder setCorrelationConjuncts(ScalarOperator correlationConjuncts) {
            this.correlationConjuncts = correlationConjuncts;
            return this;
        }

        public Builder setNeedCheckMaxRows(boolean needCheckMaxRows) {
            this.needCheckMaxRows = needCheckMaxRows;
            return this;
        }

        public Builder setUseSemiAnti(boolean useSemiAnti) {
            this.useSemiAnti = useSemiAnti;
            return this;
        }

        public Builder setNeedOutputRightChildColumns(boolean needOutputRightChildColumns) {
            this.needOutputRightChildColumns = needOutputRightChildColumns;
            return this;
        }

        public Builder setUnCorrelationSubqueryPredicateColumns(ColumnRefSet unCorrelationSubqueryPredicateColumns) {
            this.unCorrelationSubqueryPredicateColumns = unCorrelationSubqueryPredicateColumns;
            return this;
        }

        @Override
        public LogicalApplyOperator build() {
            return new LogicalApplyOperator(this);
        }

        @Override
        public LogicalApplyOperator.Builder withOperator(LogicalApplyOperator applyOperator) {
            super.withOperator(applyOperator);
            subqueryOperator = applyOperator.subqueryOperator;
            output = applyOperator.output;
            correlationColumnRefs = applyOperator.correlationColumnRefs;
            correlationConjuncts = applyOperator.correlationConjuncts;
            needCheckMaxRows = applyOperator.needCheckMaxRows;
            useSemiAnti = applyOperator.useSemiAnti;
            needOutputRightChildColumns = applyOperator.needOutputRightChildColumns;
            unCorrelationSubqueryPredicateColumns = applyOperator.unCorrelationSubqueryPredicateColumns;
            return this;
        }
    }
}
