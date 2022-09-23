// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.TableName;

// DROP TABLE
public class DropTableStmt extends DdlStmt {
    private final boolean ifExists;
    private final TableName tableName;
    private final boolean isView;
    private final boolean forceDrop;

    public DropTableStmt(boolean ifExists, TableName tableName, boolean forceDrop) {
        this.ifExists = ifExists;
        this.tableName = tableName;
        this.isView = false;
        this.forceDrop = forceDrop;
    }

    public DropTableStmt(boolean ifExists, TableName tableName, boolean isView, boolean forceDrop) {
        this.ifExists = ifExists;
        this.tableName = tableName;
        this.isView = isView;
        this.forceDrop = forceDrop;
    }

    public boolean isSetIfExists() {
        return ifExists;
    }

    public TableName getTbl() {
        return tableName;
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public String getTableName() {
        return tableName.getTbl();
    }

    public TableName getTableNameObject() {
        return tableName;
    }

    public boolean isView() {
        return isView;
    }

    public boolean isForceDrop() {
        return this.forceDrop;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropTableStatement(this, context);
    }
}
