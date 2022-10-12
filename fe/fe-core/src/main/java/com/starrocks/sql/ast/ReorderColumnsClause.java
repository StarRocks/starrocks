// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;

import java.util.List;
import java.util.Map;

// reorder column
public class ReorderColumnsClause extends AlterTableColumnClause {
    private final List<String> columnsByPos;

    public List<String> getColumnsByPos() {
        return columnsByPos;
    }

    public ReorderColumnsClause(List<String> cols, String rollup, Map<String, String> properties) {
        super(AlterOpType.SCHEMA_CHANGE, rollup, properties);
        this.columnsByPos = cols;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitReorderColumnsClause(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
