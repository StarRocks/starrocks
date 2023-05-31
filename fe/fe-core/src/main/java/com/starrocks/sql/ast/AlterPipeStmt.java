package com.starrocks.sql.ast;

import com.starrocks.sql.parser.NodePosition;

public class AlterPipeStmt extends DdlStmt {

    protected AlterPipeStmt(NodePosition pos) {
        super(pos);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterPipeStatement(this, context);
    }
}
