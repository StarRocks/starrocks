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

import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.UserException;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.privilege.PrivilegeManager;
import com.starrocks.sql.ast.SetRoleStmt;
import com.starrocks.sql.ast.SetRoleType;

import java.util.HashSet;
import java.util.Set;

public class SetRoleExecutor {

    private static long getValidRoleId(PrivilegeManager manager, Set<Long> roleIdsForUser, String roleName,
                                       UserIdentity userIdentity) throws UserException {
        Long id = manager.getRoleIdByNameAllowNull(roleName);
        if (id == null) {
            throw new UserException("Cannot find role " + roleName);
        }

        if (!roleIdsForUser.contains(id)) {
            throw new UserException("Role " + roleName + " is not granted to " + userIdentity.toString());
        }
        return id;
    }

    public static void execute(SetRoleStmt stmt, ConnectContext context) throws UserException, PrivilegeException {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        UserIdentity user = context.getCurrentUserIdentity();
        Set<Long> roleIdsForUser = manager.getRoleIdsByUser(user);
        Set<Long> roleIds;

        if (stmt.getSetRoleType().equals(SetRoleType.NONE)) {
            roleIds = new HashSet<>();
        } else if (stmt.getSetRoleType().equals(SetRoleType.DEFAULT)) {
            roleIds = manager.getDefaultRoleIdsByUser(user);
        } else if (stmt.getSetRoleType().equals(SetRoleType.ALL)) {
            roleIds = roleIdsForUser;

            // SET ROLE ALL EXCEPT
            for (String roleName : stmt.getRoles()) {
                roleIds.remove(getValidRoleId(manager, roleIdsForUser, roleName, user));
            }
        } else {
            // set role 'role1', 'role2'
            roleIds = new HashSet<>();
            for (String roleName : stmt.getRoles()) {
                roleIds.add(getValidRoleId(manager, roleIdsForUser, roleName, user));
            }
        }
        context.setCurrentRoleIds(roleIds);
    }
}