// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.UserIdentity;

public class DropUserStmt extends DdlStmt {
    private final UserIdentity userIdent;

    public DropUserStmt(UserIdentity userIdent) {
        this.userIdent = userIdent;
    }

    public UserIdentity getUserIdentity() {
        return userIdent;
    }

    public UserIdentity getUserIdent() {
        return userIdent;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropUserStatement(this, context);
    }
}
