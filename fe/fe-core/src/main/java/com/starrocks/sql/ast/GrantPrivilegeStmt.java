// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.UserIdentity;

import java.util.List;

public class GrantPrivilegeStmt extends BaseGrantRevokePrivilegeStmt {
    public GrantPrivilegeStmt(List<String> privList, String privType, UserIdentity userIdentity) {
        super(privList, privType, userIdentity);
    }

    public GrantPrivilegeStmt(List<String> privList, String privType, String role) {
        super(privList, privType, role);
    }
}
