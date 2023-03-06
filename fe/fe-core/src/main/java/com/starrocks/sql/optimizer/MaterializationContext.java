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


package com.starrocks.sql.optimizer;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class MaterializationContext {
    private MaterializedView mv;
    // scan materialized view operator
    private LogicalOlapScanOperator scanMvOperator;
    // logical OptExpression for query of materialized view
    private OptExpression mvExpression;

    private ColumnRefFactory mvColumnRefFactory;

    // logical OptExpression for query
    private OptExpression queryExpression;

    private ColumnRefFactory queryRefFactory;

    private OptimizerContext optimizerContext;

    private Map<ColumnRefOperator, ColumnRefOperator> outputMapping;

    private Set<String> mvPartitionNamesToRefresh;

    private List<Table> baseTables;

    private Set<ColumnRefOperator> originQueryColumns;

    // tables both in query and mv
    private final List<Table> commonTables;

    private List<Table> queryTables;

    public MaterializationContext(MaterializedView mv,
                                  OptExpression mvExpression,
                                  ColumnRefFactory queryColumnRefFactory,
                                  ColumnRefFactory mvColumnRefFactory,
                                  Set<String> mvPartitionNamesToRefresh,
                                  List<Table> baseTables,
                                  Set<ColumnRefOperator> originQueryColumns,
                                  List<Table> commonTables) {
        this.mv = mv;
        this.mvExpression = mvExpression;
        this.queryRefFactory = queryColumnRefFactory;
        this.mvColumnRefFactory = mvColumnRefFactory;
        this.mvPartitionNamesToRefresh = mvPartitionNamesToRefresh;
        this.baseTables = baseTables;
        this.originQueryColumns = originQueryColumns;
        this.commonTables = commonTables;
    }

    public MaterializedView getMv() {
        return mv;
    }

    public LogicalOlapScanOperator getScanMvOperator() {
        return scanMvOperator;
    }

    public void setScanMvOperator(LogicalOlapScanOperator scanMvOperator) {
        this.scanMvOperator = scanMvOperator;
    }

    public OptExpression getMvExpression() {
        return mvExpression;
    }

    public ColumnRefFactory getMvColumnRefFactory() {
        return mvColumnRefFactory;
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

    public Map<ColumnRefOperator, ColumnRefOperator> getOutputMapping() {
        return outputMapping;
    }

    public void setOutputMapping(Map<ColumnRefOperator, ColumnRefOperator> outputMapping) {
        this.outputMapping = outputMapping;
    }

    public Set<String> getMvPartitionNamesToRefresh() {
        return mvPartitionNamesToRefresh;
    }

    public List<Table> getBaseTables() {
        return baseTables;
    }

    public boolean hasMultiTables() {
        return baseTables != null && baseTables.size() > 1;
    }

    public Set<ColumnRefOperator> getOriginQueryColumns() {
        return originQueryColumns;
    }

    public List<Table> getCommonTables() {
        return commonTables;
    }

    public List<Table> getQueryTables() {
        return queryTables;
    }

    public void setQueryTables(List<Table> queryTables) {
        this.queryTables = queryTables;
    }
}
