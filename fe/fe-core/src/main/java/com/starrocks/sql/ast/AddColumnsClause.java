// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;
import com.starrocks.analysis.ColumnDef;

import java.util.List;
import java.util.Map;

// add some columns to one index.
public class AddColumnsClause extends AlterTableColumnClause {
    private final List<ColumnDef> columnDefs;

    public List<ColumnDef> getColumnDefs() {
        return columnDefs;
    }

    public AddColumnsClause(List<ColumnDef> columnDefs, String rollupName, Map<String, String> properties) {
        super(AlterOpType.SCHEMA_CHANGE, rollupName, properties);
        this.columnDefs = columnDefs;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAddColumnsClause(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
