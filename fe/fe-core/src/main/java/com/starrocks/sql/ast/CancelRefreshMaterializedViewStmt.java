// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.starrocks.analysis.TableName;

public class CancelRefreshMaterializedViewStmt extends DdlStmt {
    private final TableName mvName;

    public CancelRefreshMaterializedViewStmt(TableName mvName) {
        this.mvName = mvName;
    }

    public TableName getMvName() {
        return mvName;
    }


    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCancelRefreshMaterializedViewStatement(this, context);
    }
}
