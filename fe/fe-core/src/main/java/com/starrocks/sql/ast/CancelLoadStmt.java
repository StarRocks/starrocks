// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;

public class CancelLoadStmt extends DdlStmt {

    private String dbName;
    private String label;

    private final Expr whereClause;

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public Expr getWhereClause() {
        return whereClause;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public CancelLoadStmt(String dbName, Expr whereClause) {
        this.dbName = dbName;
        this.whereClause = whereClause;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCancelLoadStatement(this, context);
    }
}
