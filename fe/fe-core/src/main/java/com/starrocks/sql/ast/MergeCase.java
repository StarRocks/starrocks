package com.starrocks.sql.ast;

import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.parser.NodePosition;

public abstract class MergeCase implements ParseNode {
    protected final NodePosition pos;

    protected MergeCase(NodePosition pos) {
        this.pos = pos;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }


    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitMergeCase(this, context);
    }
}
