// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.TableName;

import java.util.List;

// Alter view statement
public class AlterViewStmt extends BaseViewStmt {
    public AlterViewStmt(TableName tbl, List<ColWithComment> cols, QueryStatement queryStatement) {
        super(tbl, cols, queryStatement);
    }

<<<<<<< HEAD
    public TableName getTbl() {
=======
    public static AlterViewStmt fromReplaceStmt(CreateViewStmt stmt) {
        AlterViewClause alterViewClause = new AlterViewClause(
                stmt.getColWithComments(), stmt.getQueryStatement(), NodePosition.ZERO);
        alterViewClause.setInlineViewDef(stmt.getInlineViewDef());
        alterViewClause.setColumns(stmt.getColumns());
        return new AlterViewStmt(stmt.getTableName(), alterViewClause, NodePosition.ZERO);
    }

    public TableName getTableName() {
>>>>>>> 6e1d5ec99b ([Feature] support create or replace view (#27768))
        return tableName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterViewStatement(this, context);
    }
}
