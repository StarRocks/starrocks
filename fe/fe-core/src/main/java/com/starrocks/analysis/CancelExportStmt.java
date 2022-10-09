// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.google.common.base.Strings;
import com.starrocks.sql.ast.AstVisitor;

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

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CANCEL EXPORT");
        if (!Strings.isNullOrEmpty(dbName)) {
            stringBuilder.append(" FROM " + dbName);
        }

        if (whereClause != null) {
            stringBuilder.append(" WHERE " + whereClause.toSql());
        }
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
