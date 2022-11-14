// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;

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
        this.dbName = dbName;
        this.whereClause = whereClause;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCancelExportStatement(this, context);
    }
}

