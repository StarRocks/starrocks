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

// GRANT rolex TO userx
// GRANT role1 TO ROLE role2
// share the same parameter and check logic with RevokeRoleStmt
public class GrantRoleStmt extends BaseGrantRevokeRoleStmt {

    public GrantRoleStmt(List<String> granteeRole, UserIdentity userIdent) {
        super(granteeRole, userIdent);
    }

    public GrantRoleStmt(List<String> granteeRole, UserIdentity userIdent, NodePosition pos) {
        super(granteeRole, userIdent, pos);
    }

    public GrantRoleStmt(List<String> granteeRole, String role) {
        super(granteeRole, role);
    }

    public GrantRoleStmt(List<String> granteeRole, String role, NodePosition pos) {
        super(granteeRole, role, pos);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGrantRoleStatement(this, context);
    }
}
