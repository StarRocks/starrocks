// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.transformer;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;

public class LogicalPlan {
    private final OptExprBuilder root;
    private final List<ColumnRefOperator> outputColumn;
    private final List<ColumnRefOperator> correlation;

    public LogicalPlan(OptExprBuilder root, List<ColumnRefOperator> outputColumns,
                       List<ColumnRefOperator> correlation) {
        this.root = root;
        this.outputColumn = outputColumns;
        this.correlation = correlation;
    }

    public OptExpression getRoot() {
        return root.getRoot();
    }

    public OptExprBuilder getRootBuilder() {
        return root;
    }

    public List<ColumnRefOperator> getOutputColumn() {
        return outputColumn;
    }

    public List<ColumnRefOperator> getCorrelation() {
        return correlation;
    }

    public boolean canUsePipeline() {
        return root.getRoot().canUsePipeLine();
    }
}