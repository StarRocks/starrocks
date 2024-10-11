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
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Type;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.statistic.StatsConstants;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CreateAnalyzeJobStmt extends DdlStmt {
    private String catalogName;
    private long dbId;
    private long tableId;
    private final TableName tbl;
    private final StatsConstants.AnalyzeType analyzeType;
    private final AnalyzeTypeDesc analyzeTypeDesc;

    private List<Expr> columns;
    private List<String> columnNames = Lists.newArrayList();
    private final boolean isSample;
    private Map<String, String> properties;

    public CreateAnalyzeJobStmt(boolean isSample, Map<String, String> properties, NodePosition pos) {
        this(null, Lists.newArrayList(), isSample, properties,
                isSample ? StatsConstants.AnalyzeType.SAMPLE : StatsConstants.AnalyzeType.FULL,
                null,
                pos);
    }

    public CreateAnalyzeJobStmt(String db, boolean isSample, Map<String, String> properties, NodePosition pos) {
        this(new TableName(db, null), Lists.newArrayList(), isSample, properties,
                isSample ? StatsConstants.AnalyzeType.SAMPLE : StatsConstants.AnalyzeType.FULL, null, pos);
    }

    public CreateAnalyzeJobStmt(TableName tbl, List<Expr> columns, boolean isSample,
                                Map<String, String> properties, StatsConstants.AnalyzeType analyzeType,
                                AnalyzeTypeDesc analyzeTypeDesc,
                                NodePosition pos) {
        super(pos);
        this.catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
        this.tbl = tbl;
        this.dbId = StatsConstants.DEFAULT_ALL_ID;
        this.tableId = StatsConstants.DEFAULT_ALL_ID;
        this.columns = columns;
        this.isSample = isSample;
        this.properties = properties;
        this.analyzeType = analyzeType;
        this.analyzeTypeDesc = analyzeTypeDesc;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public TableName getTableName() {
        return tbl;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public List<Expr> getColumns() {
        return columns;
    }

    public List<Type> getColumnTypes() {
        return columns.stream().map(Expr::getType).collect(Collectors.toList());
    }


    public boolean isSample() {
        return isSample;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public boolean isNative() {
        return this.catalogName.equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public StatsConstants.AnalyzeType getAnalyzeType() {
        return analyzeType;
    }

    public AnalyzeTypeDesc getAnalyzeTypeDesc() {
        return analyzeTypeDesc;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateAnalyzeJobStatement(this, context);
    }
}
