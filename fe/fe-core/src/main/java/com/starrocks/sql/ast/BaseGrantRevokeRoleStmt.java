// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.UserIdentity;

// GrantRoleStmt and RevokeRoleStmt share the same parameter and check logic with GrantRoleStmt
// GRANT rolex TO userx
// GRANT role1 TO ROLE role2   supported on new RBAC framework
public abstract class BaseGrantRevokeRoleStmt extends DdlStmt {
    protected String granteeRole;
    protected UserIdentity userIdent;
    protected String role;
    private String operationName;   // GRANT or REVOKE
    private String prepositionName; // TO or FROM

    public BaseGrantRevokeRoleStmt(String granteeRole, UserIdentity userIdent, String operationName, String prepositionName) {
        this.granteeRole = granteeRole;
        this.userIdent = userIdent;
        this.role = null;
        this.operationName = operationName;
        this.prepositionName = prepositionName;
    }

    public BaseGrantRevokeRoleStmt(String granteeRole, String role, String operationName, String prepositionName) {
        this.granteeRole = granteeRole;
        this.userIdent = null;
        this.role = role;
        this.operationName = operationName;
        this.prepositionName = prepositionName;
    }

    public UserIdentity getUserIdent() {
        return userIdent;
    }

    public String getGranteeRole() {
        return granteeRole;
    }

    public String getRole() {
        return role;
    }

    @Override
    public String toString() {
        if (role == null) {
            return String.format("%s '%s' %s %s",
                    operationName, granteeRole, prepositionName, userIdent.toString());
        } else {
            return String.format("%s '%s' %s ROLE %s",
                    operationName, granteeRole, prepositionName, role);
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGrantRevokeRoleStatement(this, context);
    }
}
