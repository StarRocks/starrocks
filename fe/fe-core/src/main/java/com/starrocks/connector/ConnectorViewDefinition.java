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

import com.starrocks.catalog.Column;
import com.starrocks.sql.ast.CreateViewStmt;

import java.util.List;

public class ConnectorViewDefinition {
    private final String catalogName;
    private final String databaseName;
    private final String viewName;
    private final String comment;
    private final List<Column> columns;
    private final String inlineViewDef;

    public ConnectorViewDefinition(String catalogName,
                                   String databaseName,
                                   String viewName,
                                   String comment,
                                   List<Column> columns,
                                   String inlineViewDef) {
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.viewName = viewName;
        this.comment = comment;
        this.columns = columns;
        this.inlineViewDef = inlineViewDef;
    }

    public static ConnectorViewDefinition fromCreateViewStmt(CreateViewStmt stmt) {
        return new ConnectorViewDefinition(
                stmt.getCatalog(),
                stmt.getDbName(),
                stmt.getTable(),
                stmt.getComment(),
                stmt.getColumns(),
                stmt.getInlineViewDef());
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
}
