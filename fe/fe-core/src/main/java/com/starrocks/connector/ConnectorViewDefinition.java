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

package com.starrocks.connector;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.sql.ast.AlterViewClause;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.CreateViewStmt;

import java.util.List;
import java.util.Map;

public class ConnectorViewDefinition {
    private final String catalogName;
    private final String databaseName;
    private final String viewName;
    private final String comment;
    private final List<Column> columns;
    private final String inlineViewDef;
    private final AlterViewStmt.AlterDialectType alterDialect;
    private final Map<String, String> properties;

    public ConnectorViewDefinition(String catalogName,
                                   String databaseName,
                                   String viewName,
                                   String comment,
                                   List<Column> columns,
                                   String inlineViewDef,
                                   AlterViewStmt.AlterDialectType alterDialect,
                                   Map<String, String> properties) {
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.viewName = viewName;
        this.comment = comment;
        this.columns = columns;
        this.inlineViewDef = inlineViewDef;
        this.alterDialect = alterDialect;
        this.properties = properties;
    }

    public static ConnectorViewDefinition fromCreateViewStmt(CreateViewStmt stmt) {
        return new ConnectorViewDefinition(
                stmt.getCatalog(),
                stmt.getDbName(),
                stmt.getTable(),
                stmt.getComment(),
                stmt.getColumns(),
                stmt.getInlineViewDef(),
                AlterViewStmt.AlterDialectType.NONE,
                Maps.newHashMap());
    }

    public static ConnectorViewDefinition fromAlterViewStmt(AlterViewStmt stmt) {
        AlterViewClause alterViewClause = stmt.getAlterClause();
        return new ConnectorViewDefinition(
                stmt.getCatalog(),
                stmt.getDbName(),
                stmt.getTable(),
                "",
                alterViewClause == null ? null : alterViewClause.getColumns(),
                alterViewClause == null ? null : alterViewClause.getInlineViewDef(),
                stmt.getAlterDialectType(),
                stmt.getProperties());
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getViewName() {
        return viewName;
    }

    public String getComment() {
        return comment;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public String getInlineViewDef() {
        return inlineViewDef;
    }

    public AlterViewStmt.AlterDialectType getAlterDialectType() {
        return alterDialect;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
