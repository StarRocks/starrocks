// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.parser.NodePosition;

public class AlterFailoverGroupPrimaryStmt extends DdlStmt {
    private final boolean ifExists;
    private final String failoverGroupName;

    public AlterFailoverGroupPrimaryStmt(
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
        return ((AstVisitorEPack<R, C>) visitor).visitAlterFailoverGroupPrimaryStatement(this, context);
    }
}
