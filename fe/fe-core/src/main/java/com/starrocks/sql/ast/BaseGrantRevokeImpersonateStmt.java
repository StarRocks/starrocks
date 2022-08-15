// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.UserIdentity;

// GrantImpersonateStmt and RevokeImpersonateStmt share the same parameter and check logic
public abstract class BaseGrantRevokeImpersonateStmt extends DdlStmt {
    protected UserIdentity authorizedUser;
    protected UserIdentity securedUser;
    private String operationName;   // GRANT or REVOKE
    private String prepositionName; // TO or FROM

    public BaseGrantRevokeImpersonateStmt(
            UserIdentity authorizedUser,
            UserIdentity securedUser,
            String operationName,
            String prepositionName) {
        this.authorizedUser = authorizedUser;
        this.securedUser = securedUser;
        this.operationName = operationName;
        this.prepositionName = prepositionName;
    }

    public UserIdentity getAuthorizedUser() {
        return authorizedUser;
    }

    public UserIdentity getSecuredUser() {
        return securedUser;
    }

    @Override
    public String toString() {
        return String.format("%s IMPERSONATE ON %s %s %s",
                operationName, securedUser.toString(), prepositionName, authorizedUser.toString());
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGrantRevokeImpersonateStatement(this, context);
    }
}
