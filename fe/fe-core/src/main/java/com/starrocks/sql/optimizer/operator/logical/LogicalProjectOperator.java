// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class LogicalProjectOperator extends LogicalOperator {
    private final Map<ColumnRefOperator, ScalarOperator> columnRefMap;
    // Used for common operator compute result reuse, we need to compute
    // common sub operators firstly in BE
    private final Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap;

    public LogicalProjectOperator(Map<ColumnRefOperator, ScalarOperator> columnRefMap) {
        this(columnRefMap, new HashMap<>());
    }

    public LogicalProjectOperator(Map<ColumnRefOperator, ScalarOperator> newMap,
                                  Map<ColumnRefOperator, ScalarOperator> commonSubOperators) {
        super(OperatorType.LOGICAL_PROJECT);
        this.columnRefMap = newMap;
        this.commonSubOperatorMap = commonSubOperators;
    }

    public Map<ColumnRefOperator, ScalarOperator> getColumnRefMap() {
        return columnRefMap;
    }

    public Map<ColumnRefOperator, ScalarOperator> getCommonSubOperatorMap() {
        return commonSubOperatorMap;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        if (columnRefMap.isEmpty()) {
            return expressionContext.getChildOutputColumns(0);
        }

        ColumnRefSet columns = new ColumnRefSet();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : columnRefMap.entrySet()) {
            columns.union(kv.getKey());
        }
        return columns;
    }

    @Override
    public int hashCode() {
        return Objects.hash(opType, columnRefMap);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LogicalProjectOperator)) {
            return false;
        }
        LogicalProjectOperator rhs = (LogicalProjectOperator) obj;
        if (this == rhs) {
            return true;
        }

        return columnRefMap.keySet().equals(rhs.columnRefMap.keySet());
    }

    @Override
    public String toString() {
        return "LogicalProjectOperator " + columnRefMap.keySet();
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalProject(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalProject(optExpression, context);
    }
}