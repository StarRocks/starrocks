// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.TableName;

public class RecoverTableStmt extends DdlStmt {
    private final TableName dbTblName;

    public RecoverTableStmt(TableName dbTblName) {
        this.dbTblName = dbTblName;
    }

    public String getDbName() {
        return dbTblName.getDb();
    }

    public String getTableName() {
        return dbTblName.getTbl();
    }

    public TableName getTableNameObject() {
        return dbTblName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRecoverTableStatement(this, context);
    }
}
