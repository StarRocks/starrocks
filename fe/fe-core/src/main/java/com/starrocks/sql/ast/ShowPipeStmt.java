package com.starrocks.sql.ast;

import com.starrocks.sql.parser.NodePosition;

public class ShowPipeStmt extends DdlStmt {

    protected ShowPipeStmt(NodePosition pos) {
        super(pos);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowPipeStatement(this, context);
    }
}
