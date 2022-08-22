// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.TableName;

public class CreateTableLikeStmt extends DdlStmt {

    private final boolean ifNotExists;
    private final TableName tableName;
    private final TableName existedTableName;

    public CreateTableLikeStmt(boolean ifNotExists, TableName tableName, TableName existedTableName) {
        this.ifNotExists = ifNotExists;
        this.tableName = tableName;
        this.existedTableName = existedTableName;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public String getTableName() {
        return tableName.getTbl();
    }

    public String getExistedDbName() {
        return existedTableName.getDb();
    }

    public String getExistedTableName() {
        return existedTableName.getTbl();
    }

    public TableName getDbTbl() {
        return tableName;
    }

    public TableName getExistedDbTbl() {
        return existedTableName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateTableLikeStatement(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
