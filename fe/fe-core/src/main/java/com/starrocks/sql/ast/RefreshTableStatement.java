// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.TableName;

public class RefreshTableStatement extends DdlStmt {
    private TableName tableName;

    public RefreshTableStatement(TableName tableName) {
        this.tableName = tableName;
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public String getTableName() {
        return tableName.getTbl();
    }

    public TableName getTbl() {
        return tableName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRefreshTableStatement(this, context);
    }
}
