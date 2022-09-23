// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.UserIdentity;

// drop user cmy@['domain'];
// drop user cmy  <==> drop user cmy@'%'
// drop user cmy@'192.168.1.%'
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
