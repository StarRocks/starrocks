// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.parser.NodePosition;

public class CreateSecondaryFailoverGroupStmt extends DdlStmt {
    private final boolean ifNotExists;
    private final String failoverGroupName;
    private final String primaryMember;

    public CreateSecondaryFailoverGroupStmt(
            boolean ifNotExists,
            String failoverGroupName,
            String primaryMember,
            NodePosition pos) {
        super(pos);
        this.ifNotExists = ifNotExists;
        this.failoverGroupName = failoverGroupName;
        this.primaryMember = primaryMember;
    }

    public boolean getIfNotExists() {
        return ifNotExists;
    }

    public String getFailoverGroupName() {
        return failoverGroupName;
    }

    public String getPrimaryMember() {
        return primaryMember;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorEPack<R, C>) visitor).visitCreateSecondaryFailoverGroupStatement(this, context);
    }
}
