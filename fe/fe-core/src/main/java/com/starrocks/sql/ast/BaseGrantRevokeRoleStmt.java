// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.UserIdentity;

// GrantRoleStmt and RevokeRoleStmt share the same parameter and check logic with GrantRoleStmt
public abstract class BaseGrantRevokeRoleStmt extends DdlStmt {
    protected String role;
    protected UserIdentity userIdent;
    protected String qualifiedRole;

    public BaseGrantRevokeRoleStmt(String role, UserIdentity userIdent) {
        this.role = role;
        this.userIdent = userIdent;
    }

    public UserIdentity getUserIdent() {
        return userIdent;
    }

    public String getQualifiedRole() {
        return qualifiedRole;
    }

    public String getRole() {
        return role;
    }

    public void setQualifiedRole(String qualifiedRole) {
        this.qualifiedRole = qualifiedRole;
    }

    /**
     * return GRANT or REVOKE
     */
    public abstract String getOperationName();

    /**
     * return TO or FROM
     */
    public abstract String getPrepositionName();


    @Override
    public String toString() {
        return String.format("%s '%s' %s %s",
                getOperationName(), qualifiedRole, getPrepositionName(), userIdent.getQualifiedUser());
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGrantRevokeRoleStatement(this, context);
    }
}
