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
package com.starrocks.qe;

import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.StarRocksException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.SetDefaultRoleStmt;
import com.starrocks.sql.ast.SetRoleType;
import com.starrocks.sql.ast.UserRef;

import java.util.HashSet;
import java.util.Set;

public class SetDefaultRoleExecutor {
    public static void execute(SetDefaultRoleStmt stmt, ConnectContext context) throws StarRocksException, PrivilegeException {
        AuthorizationMgr manager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();

        UserRef user = stmt.getUser();
        UserIdentity userIdentity = new UserIdentity(user.getUser(), user.getHost(), user.isDomain());
        Set<Long> roleIdsForUser = manager.getRoleIdsByUser(userIdentity);
        Set<Long> roleIds;

        if (stmt.getSetRoleType().equals(SetRoleType.NONE)) {
            roleIds = new HashSet<>();
        } else if (stmt.getSetRoleType().equals(SetRoleType.ALL)) {
            roleIds = roleIdsForUser;
        } else {
            // set role 'role1', 'role2'
            roleIds = new HashSet<>();
            for (String roleName : stmt.getRoles()) {
                Long roleId = manager.getRoleIdByNameAllowNull(roleName);
                if (roleId != null && roleIdsForUser.contains(roleId)) {
                    roleIds.add(roleId);
                }
            }
        }

        manager.setUserDefaultRole(roleIds, userIdentity);
    }
}
