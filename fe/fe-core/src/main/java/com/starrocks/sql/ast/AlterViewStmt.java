// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.TableName;

import java.util.List;

// Alter view statement
public class AlterViewStmt extends BaseViewStmt {
    public AlterViewStmt(TableName tbl, List<ColWithComment> cols, QueryStatement queryStatement) {
        super(tbl, cols, queryStatement);
    }

    public TableName getTbl() {
        return tableName;
    }

    public static AlterViewStmt fromReplaceStmt(CreateViewStmt stmt) {
        return new AlterViewStmt(stmt.getTableName(), stmt.getCols(), stmt.getQueryStatement());
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterViewStatement(this, context);
    }
}
