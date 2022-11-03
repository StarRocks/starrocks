// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;
import java.util.Set;

public class MaterializationContext {
    private MaterializedView mv;
    // scan materialized view operator
    private Operator scanMvOperator;
    // Logical OptExpression for query of materialized view
    private OptExpression mvExpression;

    // for column -> relationId mapping, column -> table mapping
    private ColumnRefFactory mvColumnRefFactory;

    // output expressions of mv define sql
    List<ColumnRefOperator> mvOutputExpressions;

    // output expressions of select * from mv
    List<ColumnRefOperator> scanMvOutputExpressions;

    // for partitioned mv, records the partitions that need to be refreshed
    Set<String> partitionNamesToRefresh;

    // Logical OptExpression for query
    private OptExpression queryExpression;

    private ColumnRefFactory queryRefFactory;

    private OptimizerContext optimizerContext;

    public MaterializationContext(MaterializedView mv,
                                  OptExpression mvExpression,
                                  ColumnRefFactory columnRefFactory,
                                  List<ColumnRefOperator> mvOutputExpressions,
                                  Set<String> partitionNamesToRefresh) {
        this.mv = mv;
        this.mvExpression = mvExpression;
        this.mvColumnRefFactory = columnRefFactory;
        this.mvOutputExpressions = mvOutputExpressions;
        this.partitionNamesToRefresh = partitionNamesToRefresh;
    }

    public MaterializedView getMv() {
        return mv;
    }

    public Operator getScanMvOperator() {
        return scanMvOperator;
    }

    public void setScanMvOperator(Operator scanMvOperator) {
        this.scanMvOperator = scanMvOperator;
    }

    public OptExpression getMvExpression() {
        return mvExpression;
    }

    public ColumnRefFactory getMvColumnRefFactory() {
        return mvColumnRefFactory;
    }

    public List<ColumnRefOperator> getMvOutputExpressions() {
        return mvOutputExpressions;
    }

    public List<ColumnRefOperator> getScanMvOutputExpressions() {
        return scanMvOutputExpressions;
    }

    public void setScanMvOutputExpressions(List<ColumnRefOperator> scanMvOutputExpressions) {
        this.scanMvOutputExpressions = scanMvOutputExpressions;
    }

    public OptExpression getQueryExpression() {
        return queryExpression;
    }

    public void setQueryExpression(OptExpression queryExpression) {
        this.queryExpression = queryExpression;
    }

    public ColumnRefFactory getQueryRefFactory() {
        return queryRefFactory;
    }

    public void setQueryRefFactory(ColumnRefFactory queryRefFactory) {
        this.queryRefFactory = queryRefFactory;
    }

    public OptimizerContext getOptimizerContext() {
        return optimizerContext;
    }

    public void setOptimizerContext(OptimizerContext optimizerContext) {
        this.optimizerContext = optimizerContext;
    }

    public Set<String> getPartitionNamesToRefresh() {
        return partitionNamesToRefresh;
    }
}
