// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.qe;

import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.UserException;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.privilege.PrivilegeManager;
import com.starrocks.sql.ast.SetRoleStmt;

import java.util.HashSet;
import java.util.Set;

public class SetRoleExecutor {

    private static long getValidRoleId(PrivilegeManager manager, Set<Long> roleIdsForUser, String roleName)
            throws UserException {
        Long id = manager.getRoleIdByNameAllowNull(roleName);
        if (id == null) {
            throw new UserException("Cannot find role " + roleName);
        }

        if (! roleIdsForUser.contains(id)) {
            throw new UserException("Role " + roleName + " is not granted");
        }
        return id;
    }

    public static void execute(SetRoleStmt stmt, ConnectContext context) throws UserException, PrivilegeException {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        UserIdentity user = context.getCurrentUserIdentity();
        Set<Long> roleIdsForUser = manager.getRoleIdsByUser(user);
        Set<Long> roleIds;
        if (stmt.isAll()) {
            roleIds = roleIdsForUser;
            // SET ROLE ALL EXCEPT
            if (stmt.getRoles() != null) {
                for (String roleName : stmt.getRoles()) {
                    roleIds.remove(getValidRoleId(manager, roleIdsForUser, roleName));
                }
            }
        } else {
            // set role 'role1', 'role2'
            roleIds = new HashSet<>();
            for (String roleName : stmt.getRoles()) {
                roleIds.add(getValidRoleId(manager, roleIdsForUser, roleName));
            }
        }
        context.setCurrentRoleIds(roleIds);
    }
}