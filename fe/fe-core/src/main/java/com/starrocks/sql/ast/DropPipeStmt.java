package com.starrocks.sql.ast;

import com.starrocks.sql.parser.NodePosition;

public class DropPipeStmt extends DdlStmt {

    private String pipeName;

    public DropPipeStmt(String pipeName, NodePosition pos) {
        super(pos);
        this.pipeName = pipeName;
    }

    public String getPipeName() {
        return pipeName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropPipeStatement(this, context);
    }
}
