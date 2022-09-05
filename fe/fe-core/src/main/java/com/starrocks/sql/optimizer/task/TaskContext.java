// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.task;

import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;

import java.util.Collections;
import java.util.List;

// The context for optimizer task
public class TaskContext {
    private final OptimizerContext optimizerContext;
    private final PhysicalPropertySet requiredProperty;
    private ColumnRefSet requiredColumns;
    private double upperBoundCost;
    private List<LogicalOlapScanOperator> allScanOperators;
    // record rewrite phase plan change
    private int rewriteNum;

    public TaskContext(OptimizerContext context,
                       PhysicalPropertySet physicalPropertySet,
                       ColumnRefSet requiredColumns,
                       double cost) {
        this.optimizerContext = context;
        this.requiredProperty = physicalPropertySet;
        this.requiredColumns = requiredColumns;
        this.upperBoundCost = cost;
        this.allScanOperators = Collections.emptyList();
        this.rewriteNum = 0;
    }

    public TaskContext(OptimizerContext optimizerContext,
                       PhysicalPropertySet requiredProperty,
                       ColumnRefSet requiredColumns, double upperBoundCost,
                       List<LogicalOlapScanOperator> allScanOperators) {
        this.optimizerContext = optimizerContext;
        this.requiredProperty = requiredProperty;
        this.requiredColumns = requiredColumns;
        this.upperBoundCost = upperBoundCost;
        this.allScanOperators = allScanOperators;
        this.rewriteNum = 0;
    }

    public OptimizerContext getOptimizerContext() {
        return optimizerContext;
    }

    public double getUpperBoundCost() {
        return upperBoundCost;
    }

    public PhysicalPropertySet getRequiredProperty() {
        return requiredProperty;
    }

    public ColumnRefSet getRequiredColumns() {
        return requiredColumns;
    }

    public void setRequiredColumns(ColumnRefSet requiredColumns) {
        this.requiredColumns = requiredColumns;
    }

    public void setUpperBoundCost(double upperBoundCost) {
        this.upperBoundCost = upperBoundCost;
    }

    public void setAllScanOperators(List<LogicalOlapScanOperator> allScanOperators) {
        this.allScanOperators = allScanOperators;
    }

    public List<LogicalOlapScanOperator> getAllScanOperators() {
        return allScanOperators;
    }

    public void resetRewriteNum() {
        rewriteNum = 0;
    }

    public void incrementRewriteNum() {
        rewriteNum++;
    }

    public boolean hasRewrite() {
        return rewriteNum > 0;
    }
}
