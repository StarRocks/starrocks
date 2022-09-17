// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.TableName;

public class RefreshMaterializedViewStatement extends DdlStmt {
    private final TableName mvName;

    public RefreshMaterializedViewStatement(TableName mvName) {
        this.mvName = mvName;
    }

    public TableName getMvName() {
        return mvName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRefreshMaterializedViewStatement(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
