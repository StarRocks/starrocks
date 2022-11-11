// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

public class RecoverDbStmt extends DdlStmt {
    private String catalog;
    private String dbName;

    public RecoverDbStmt(String dbName) {
        this.dbName = dbName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbname) {
        this.dbName = dbname;
    }

    public String getCatalogName() {
        return this.catalog;
    }

    public void setCatalogName(String catalogName) {
        this.catalog = catalogName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRecoverDbStatement(this, context);
    }
}
