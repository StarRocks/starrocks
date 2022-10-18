// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;

import java.util.Map;

// rename column
public class ColumnRenameClause extends AlterTableClause {
    private final String colName;
    private final String newColName;

    public ColumnRenameClause(String colName, String newColName) {
        super(AlterOpType.SCHEMA_CHANGE);
        this.colName = colName;
        this.newColName = newColName;
        this.needTableStable = false;
    }

    public String getColName() {
        return colName;
    }

    public String getNewColName() {
        return newColName;
    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitColumnRenameClause(this, context);
    }
}
