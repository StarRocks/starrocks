// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.UserIdentity;

// GRANT rolex TO userx
// GRANT role1 TO ROLE role2
// share the same parameter and check logic with RevokeRoleStmt
public class GrantRoleStmt extends BaseGrantRevokeRoleStmt {

    public GrantRoleStmt(String granteeRole, UserIdentity userIdent) {
        super(granteeRole, userIdent, "GRANT", "TO");
    }
    public GrantRoleStmt(String granteeRole, String role) {
        super(granteeRole, role, "GRANT", "TO");
    }
}
