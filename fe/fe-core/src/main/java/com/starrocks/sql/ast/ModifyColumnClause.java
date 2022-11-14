// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.ColumnPosition;

import java.util.Map;

// modify one column
public class ModifyColumnClause extends AlterTableColumnClause {
    private final ColumnDef columnDef;
    private final ColumnPosition colPos;

    public ColumnDef getColumnDef() {
        return columnDef;
    }

    public ColumnPosition getColPos() {
        return colPos;
    }

    public ModifyColumnClause(ColumnDef columnDef, ColumnPosition colPos, String rollup,
                              Map<String, String> properties) {
        super(AlterOpType.SCHEMA_CHANGE, rollup, properties);
        this.columnDef = columnDef;
        this.colPos = colPos;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitModifyColumnClause(this, context);
    }
}
