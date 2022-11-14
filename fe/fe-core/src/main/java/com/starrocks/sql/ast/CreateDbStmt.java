// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

public class CreateDbStmt extends DdlStmt {
    private final boolean ifNotExists;
    private final String dbName;

    public CreateDbStmt(boolean ifNotExists, String dbName) {
        this.ifNotExists = ifNotExists;
        this.dbName = dbName;
    }

    public String getFullDbName() {
        return dbName;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateDbStatement(this, context);
    }
}
