// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.ColumnPosition;

import java.util.Map;

// clause which is used to add one column to
public class AddColumnClause extends AlterTableColumnClause {
    private final ColumnDef columnDef;
    // Column position
    private final ColumnPosition colPos;

    public ColumnPosition getColPos() {
        return colPos;
    }

    public ColumnDef getColumnDef() {
        return columnDef;
    }

    public AddColumnClause(ColumnDef columnDef, ColumnPosition colPos, String rollupName,
                           Map<String, String> properties) {
        super(AlterOpType.SCHEMA_CHANGE, rollupName, properties);
        this.columnDef = columnDef;
        this.colPos = colPos;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAddColumnClause(this, context);
    }
}
