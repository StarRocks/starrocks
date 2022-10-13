// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;

import java.util.Map;

// Drop one column
public class DropColumnClause extends AlterTableColumnClause {
    private final String colName;

    public String getColName() {
        return colName;
    }

    public DropColumnClause(String colName, String rollupName, Map<String, String> properties) {
        super(AlterOpType.SCHEMA_CHANGE, rollupName, properties);
        this.colName = colName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropColumnClause(this, context);
    }
}
