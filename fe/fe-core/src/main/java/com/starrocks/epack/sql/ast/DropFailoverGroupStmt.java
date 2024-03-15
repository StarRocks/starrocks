// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.parser.NodePosition;

public class DropFailoverGroupStmt extends DdlStmt {
    private final boolean ifExists;
    private final String failoverGroupName;

    public DropFailoverGroupStmt(
            boolean ifExists,
            String failoverGroupName,
            NodePosition pos) {
        super(pos);
        this.ifExists = ifExists;
        this.failoverGroupName = failoverGroupName;
    }

    public boolean getIfExists() {
        return ifExists;
    }

    public String getFailoverGroupName() {
        return failoverGroupName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        if (visitor instanceof AstVisitorEPack) {
            return ((AstVisitorEPack<R, C>) visitor).visitDropFailoverGroupStatement(this, context);
        } else {
            return null;
        }
    }
}
