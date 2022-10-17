// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import java.util.List;

public class RevokePrivilegeStmt extends BaseGrantRevokePrivilegeStmt {
    public RevokePrivilegeStmt(
            List<String> privList,
            String privType,
            GrantRevokeClause grantRevokeClause,
            GrantRevokePrivilegeObjects objects) {
        super(privList, privType, grantRevokeClause, objects);
    }
}
