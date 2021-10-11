// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
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

    public LogicalApplyOperator(ColumnRefOperator output, ScalarOperator subqueryOperator,
                                List<ColumnRefOperator> correlationColumnRefs, boolean useSemiAnti) {
        super(OperatorType.LOGICAL_APPLY);
        this.output = output;
        this.subqueryOperator = subqueryOperator;
        this.correlationColumnRefs = correlationColumnRefs;
        this.correlationConjuncts = null;
        this.needCheckMaxRows = isScalar();
        this.useSemiAnti = useSemiAnti;
    }

    public LogicalApplyOperator(ColumnRefOperator output, ScalarOperator subqueryOperator,
                                List<ColumnRefOperator> correlationColumnRefs, ScalarOperator correlationConjuncts,
                                ScalarOperator predicate, boolean needCheckMaxRows, boolean useSemiAnti) {
        super(OperatorType.LOGICAL_APPLY);
        this.output = output;
        this.subqueryOperator = subqueryOperator;
        this.correlationColumnRefs = correlationColumnRefs;
        this.correlationConjuncts = correlationConjuncts;
        this.predicate = predicate;
        this.needCheckMaxRows = needCheckMaxRows;
        this.useSemiAnti = useSemiAnti;
    }

    public ColumnRefOperator getOutput() {
        return output;
    }

    public boolean isQuantified() {
        return subqueryOperator instanceof InPredicateOperator;
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

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        ColumnRefSet outputColumns =
                (ColumnRefSet) expressionContext.getChildLogicalProperty(0).getOutputColumns().clone();
        outputColumns.union(expressionContext.getChildLogicalProperty(1).getOutputColumns());
        if (output != null) {
            outputColumns.union(output);
        }
        return outputColumns;
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalApply(optExpression, context);
    }
}