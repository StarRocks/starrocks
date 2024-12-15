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

package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Type;
import com.starrocks.sql.parser.NodePosition;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AnalyzeStmt extends StatementBase {
    private final TableName tbl;
    private final boolean isSample;
    private boolean isAsync;
    private boolean isExternal = false;
    private final PartitionNames partitionNames;
    private List<Long> partitionIds = null;
    private Map<String, String> properties;
    private final AnalyzeTypeDesc analyzeTypeDesc;

    // Mode 1: specified columns
    private List<Expr> columns;
    private List<String> columnNames = Lists.newArrayList();
    // Mode 2: predicate columns
    private final boolean usePredicateColumns;
    // Mode 3: all columns, identical to empty columns

    public AnalyzeStmt(TableName tbl, List<Expr> columns, PartitionNames partitionNames, Map<String, String> properties,
                       boolean isSample, boolean isAsync, boolean usePredicateColumns,
                       AnalyzeTypeDesc analyzeTypeDesc, NodePosition pos) {
        super(pos);
        this.tbl = tbl;
        this.columns = columns;
        this.partitionNames = partitionNames;
        this.isSample = isSample;
        this.isAsync = isAsync;
        this.usePredicateColumns = usePredicateColumns;
        this.properties = properties;
        this.analyzeTypeDesc = analyzeTypeDesc;
    }

    public List<Expr> getColumns() {
        return columns;
    }

    public List<Type> getColumnTypes() {
        return columns.stream().map(Expr::getType).collect(Collectors.toList());
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public TableName getTableName() {
        return tbl;
    }

    public boolean isSample() {
        return isSample;
    }

    public boolean isAsync() {
        return isAsync;
    }

    public boolean isAllColumns() {
        return CollectionUtils.isEmpty(columns);
    }

    public boolean isUsePredicateColumns() {
        return usePredicateColumns;
    }

    public void setIsAsync(boolean value) {
        this.isAsync = value;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public AnalyzeTypeDesc getAnalyzeTypeDesc() {
        return analyzeTypeDesc;
    }

    public boolean isExternal() {
        return isExternal;
    }

    public void setExternal(boolean isExternal) {
        this.isExternal = isExternal;
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    public void setPartitionIds(List<Long> partitionIds) {
        this.partitionIds = partitionIds;
    }

    public List<Long> getPartitionIds() {
        return partitionIds;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAnalyzeStatement(this, context);
    }
}
