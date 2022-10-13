// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

public class AlterDatabaseRename extends DdlStmt {
    private String dbName;
    private final String newDbName;

    public AlterDatabaseRename(String dbName, String newDbName) {
        this.dbName = dbName;
        this.newDbName = newDbName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getNewDbName() {
        return newDbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterDatabaseRename(this, context);
    }

}
