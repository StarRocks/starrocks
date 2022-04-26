// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;
import com.starrocks.analysis.AlterTableClause;

import java.util.Map;

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
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitTableRenameClause(this, context);
    }
}
