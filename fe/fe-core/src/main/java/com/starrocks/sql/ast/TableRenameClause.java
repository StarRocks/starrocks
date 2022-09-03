// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;

public class TableRenameClause extends AlterTableClause {
    private final String newTableName;

    public TableRenameClause(String newTableName) {
        super(AlterOpType.RENAME);
        this.newTableName = newTableName;
        this.needTableStable = false;
    }

    public String getNewTableName() {
        return newTableName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitTableRenameClause(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
