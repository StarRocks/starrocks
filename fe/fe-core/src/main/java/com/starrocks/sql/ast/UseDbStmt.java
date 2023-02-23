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

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.sql.parser.NodePosition;

/**
 * Representation of a USE [catalog.]db statement.
 * Queries from MySQL client will not generate UseDbStmt, it will be handled by the COM_INIT_DB protocol.
 * Queries from JDBC will be handled by COM_QUERY protocol, it will generate UseDbStmt.
 */
public class UseDbStmt extends StatementBase {
    private String catalog;
    private final String database;

    public UseDbStmt(String catalog, String database) {
        this(catalog, database, NodePosition.ZERO);
    }

    public UseDbStmt(String catalog, String database, NodePosition pos) {
        super(pos);
        this.catalog = catalog;
        this.database = database;
    }

    public String getCatalogName() {
        return this.catalog;
    }

    public void setCatalogName(String catalogName) {
        this.catalog = catalogName;
    }

    public String getDbName() {
        return this.database;
    }

    public String getIdentifier() {
        if (catalog == null) {
            return database;
        } else {
            return catalog + "." + database;
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitUseDbStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}
