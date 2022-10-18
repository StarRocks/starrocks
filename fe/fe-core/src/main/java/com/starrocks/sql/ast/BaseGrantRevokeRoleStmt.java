// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.UserIdentity;

// GrantRoleStmt and RevokeRoleStmt share the same parameter and check logic with GrantRoleStmt
public abstract class BaseGrantRevokeRoleStmt extends DdlStmt {
    protected String role;
    protected UserIdentity userIdent;
    protected String qualifiedRole;
    private String operationName;   // GRANT or REVOKE
    private String prepositionName; // TO or FROM

    public BaseGrantRevokeRoleStmt(String role, UserIdentity userIdent, String operationName, String prepositionName) {
        this.role = role;
        this.userIdent = userIdent;
        this.operationName = operationName;
        this.prepositionName = prepositionName;
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

    @Override
    public String toString() {
        return String.format("%s '%s' %s %s",
                operationName, qualifiedRole, prepositionName, userIdent.toString());
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGrantRevokeRoleStatement(this, context);
    }
}
