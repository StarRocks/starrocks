// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.UserIdentity;

// GrantImpersonateStmt and RevokeImpersonateStmt share the same parameter and check logic
public abstract class BaseGrantRevokeImpersonateStmt extends DdlStmt {
    protected UserIdentity authorizedUser;
    protected String authorizedRoleName;
    protected UserIdentity securedUser;
    private String operationName;   // GRANT or REVOKE
    private String prepositionName; // TO or FROM

    // GRANT IMPERSONATE ON securedUser To authorizedUser
    public BaseGrantRevokeImpersonateStmt(
            UserIdentity authorizedUser,
            UserIdentity securedUser,
            String operationName,
            String prepositionName) {
        this.authorizedUser = authorizedUser;
        this.authorizedRoleName = null;
        this.securedUser = securedUser;
        this.operationName = operationName;
        this.prepositionName = prepositionName;
    }

    // GRANT IMPERSONATE ON securedUser To ROLE authorizedRoleName
    public BaseGrantRevokeImpersonateStmt(
            String authorizedRoleName,
            UserIdentity securedUser,
            String operationName,
            String prepositionName) {
        this.authorizedUser = null;
        this.authorizedRoleName = authorizedRoleName;
        this.securedUser = securedUser;
        this.operationName = operationName;
        this.prepositionName = prepositionName;
    }

    public UserIdentity getAuthorizedUser() {
        return authorizedUser;
    }

    public String getAuthorizedRoleName() {
        return authorizedRoleName;
    }

    public UserIdentity getSecuredUser() {
        return securedUser;
    }

    public void setAuthorizedRoleName(String authorizedRoleName) {
        this.authorizedRoleName = authorizedRoleName;
    }

    @Override
    public String toString() {
        String authorizedEntity = authorizedUser == null ? "ROLE '" + authorizedRoleName + "'" : authorizedUser.toString();
        return String.format("%s IMPERSONATE ON %s %s %s",
                operationName, securedUser.toString(), prepositionName, authorizedEntity);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGrantRevokeImpersonateStatement(this, context);
    }
}
