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
public abstract class BaseGrantRevokeRoleStmt extends DdlStmt {
    protected List<String> granteeRole;
    protected UserRef user;
    protected String roleOrGroup;
    protected GrantType grantType;

    protected BaseGrantRevokeRoleStmt(List<String> granteeRole, UserRef user, NodePosition pos) {
        super(pos);
        this.granteeRole = granteeRole;
        this.user = user;
        this.roleOrGroup = null;
        this.grantType = GrantType.USER;
    }

    protected BaseGrantRevokeRoleStmt(List<String> granteeRole, String roleOrGroup, GrantType grantType, NodePosition pos) {
        super(pos);
        this.granteeRole = granteeRole;
        this.user = null;
        this.roleOrGroup = roleOrGroup;
        this.grantType = grantType;
    }

    public UserRef getUser() {
        return user;
    }

    public List<String> getGranteeRole() {
        return granteeRole;
    }

    public String getRoleOrGroup() {
        return roleOrGroup;
    }

    public GrantType getGrantType() {
        return grantType;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGrantRevokeRoleStatement(this, context);
    }
}
