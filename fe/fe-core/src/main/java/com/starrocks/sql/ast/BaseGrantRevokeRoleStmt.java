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

import com.starrocks.sql.parser.NodePosition;

import java.util.List;

// GrantRoleStmt and RevokeRoleStmt share the same parameter and check logic with GrantRoleStmt
// GRANT rolex TO userx
// GRANT role1 TO ROLE role2   supported on new RBAC framework
public abstract class BaseGrantRevokeRoleStmt extends DdlStmt {
    protected List<String> granteeRole;
    protected UserIdentity userIdentity;
    protected String role;

    protected BaseGrantRevokeRoleStmt(List<String> granteeRole, UserIdentity userIdentity) {
        this(granteeRole, userIdentity, NodePosition.ZERO);
    }

    protected BaseGrantRevokeRoleStmt(List<String> granteeRole, UserIdentity userIdentity, NodePosition pos) {
        super(pos);
        this.granteeRole = granteeRole;
        this.userIdentity = userIdentity;
        this.role = null;
    }

    protected BaseGrantRevokeRoleStmt(List<String> granteeRole, String role) {
        this(granteeRole, role, NodePosition.ZERO);

    }

    protected BaseGrantRevokeRoleStmt(List<String> granteeRole, String role, NodePosition pos) {
        super(pos);
        this.granteeRole = granteeRole;
        this.userIdentity = null;
        this.role = role;
    }
    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public List<String> getGranteeRole() {
        return granteeRole;
    }

    public String getRole() {
        return role;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGrantRevokeRoleStatement(this, context);
    }
}
