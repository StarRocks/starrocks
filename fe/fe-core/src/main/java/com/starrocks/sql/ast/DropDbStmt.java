// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.DdlStmt;

// DROP DB Statement
public class DropDbStmt extends DdlStmt {
    private final boolean ifExists;
    private String dbName;
    private final boolean forceDrop;

    public DropDbStmt(boolean ifExists, String dbName, boolean forceDrop) {
        this.ifExists = ifExists;
        this.dbName = dbName;
        this.forceDrop = forceDrop;
    }

    public boolean isSetIfExists() {
        return ifExists;
    }

    public String getDbName() {
        return this.dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public boolean isForceDrop() {
        return this.forceDrop;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropDbStatement(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

}
