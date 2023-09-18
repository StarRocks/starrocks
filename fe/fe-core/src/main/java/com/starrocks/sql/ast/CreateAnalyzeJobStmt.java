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
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.statistic.StatsConstants;

import java.util.List;
import java.util.Map;

public class CreateAnalyzeJobStmt extends DdlStmt {
    private String catalogName;
    private long dbId;
    private long tableId;
    private final TableName tbl;
    private final List<String> columnNames;
    private final boolean isSample;
    private Map<String, String> properties;

    public CreateAnalyzeJobStmt(boolean isSample, Map<String, String> properties, NodePosition pos) {
        this(null, Lists.newArrayList(), isSample, properties, pos);
    }

    public CreateAnalyzeJobStmt(String db, boolean isSample, Map<String, String> properties, NodePosition pos) {
        this(new TableName(db, null), Lists.newArrayList(), isSample, properties, pos);
    }

    public CreateAnalyzeJobStmt(TableName tbl, List<String> columnNames, boolean isSample,
                                Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
        this.tbl = tbl;
        this.dbId = StatsConstants.DEFAULT_ALL_ID;
        this.tableId = StatsConstants.DEFAULT_ALL_ID;
        this.columnNames = columnNames;
        this.isSample = isSample;
        this.properties = properties;
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

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateAnalyzeJobStatement(this, context);
    }
}
