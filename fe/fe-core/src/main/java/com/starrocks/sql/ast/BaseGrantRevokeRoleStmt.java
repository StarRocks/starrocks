// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


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

    protected BaseGrantRevokeRoleStmt(String granteeRole, UserIdentity userIdent, String operationName, String prepositionName) {
        this.granteeRole = granteeRole;
        this.userIdent = userIdent;
        this.role = null;
        this.operationName = operationName;
        this.prepositionName = prepositionName;
    }

    protected BaseGrantRevokeRoleStmt(String granteeRole, String role, String operationName, String prepositionName) {
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
