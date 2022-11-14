// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

public class CancelBackupStmt extends CancelStmt {

    private String dbName;
    private final boolean isRestore;

    public CancelBackupStmt(String dbName, boolean isRestore) {
        this.dbName = dbName;
        this.isRestore = isRestore;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public boolean isRestore() {
        return isRestore;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCancelBackupStatement(this, context);
    }
}
