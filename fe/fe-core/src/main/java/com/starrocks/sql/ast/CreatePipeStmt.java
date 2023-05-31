package com.starrocks.sql.ast;

import com.starrocks.sql.parser.NodePosition;

public class CreatePipeStmt extends DdlStmt {

    private boolean ifNotExists;
    private String pipeName;
    private InsertStmt insertStmt;

    public CreatePipeStmt(boolean ifNotExists, String pipeName, InsertStmt insertStmt, NodePosition pos) {
        super(pos);
        ifNotExists = ifNotExists;
        pipeName = pipeName;
        insertStmt = insertStmt;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public String getPipeName() {
        return pipeName;
    }

    public InsertStmt getInsertStmt() {
        return insertStmt;
    }

    @Override
    public String toSql() {
        return "CREATE PIPE " + pipeName + " AS " + insertStmt.toSql();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreatePipeStatement(this, context);
    }
}
