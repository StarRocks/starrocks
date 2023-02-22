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

import com.starrocks.analysis.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.UUID;

/**
 * syntax:
 * CANCEL EXPORT FROM example_db WHERE queryid = "921d8f80-7c9d-11eb-9342-acde48001122"
 */
public class CancelExportStmt extends DdlStmt {
    private String dbName;
    private Expr whereClause;

    private UUID queryId;

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setQueryId(UUID queryId) {
        this.queryId = queryId;
    }

    public Expr getWhereClause() {
        return whereClause;
    }

    public String getDbName() {
        return dbName;
    }

    public UUID getQueryId() {
        return queryId;
    }

    public CancelExportStmt(String dbName, Expr whereClause) {
        this(dbName, whereClause, NodePosition.ZERO);
    }

    public CancelExportStmt(String dbName, Expr whereClause, NodePosition pos) {
        super(pos);
        this.dbName = dbName;
        this.whereClause = whereClause;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCancelExportStatement(this, context);
    }
}

