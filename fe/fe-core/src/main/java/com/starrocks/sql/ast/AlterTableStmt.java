// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.TableName;

import java.util.List;

// Alter table statement.
public class AlterTableStmt extends DdlStmt {
    private TableName tbl;
    private final List<AlterClause> ops;

    public AlterTableStmt(TableName tbl, List<AlterClause> ops) {
        this.tbl = tbl;
        this.ops = ops;
    }

    public void setTableName(String newTableName) {
        tbl = new TableName(tbl.getDb(), newTableName);
    }

    public TableName getTbl() {
        return tbl;
    }

    public List<AlterClause> getOps() {
        return ops;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterTableStatement(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
