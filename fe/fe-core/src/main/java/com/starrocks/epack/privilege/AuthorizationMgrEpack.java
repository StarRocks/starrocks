// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.privilege;

import com.google.common.base.Preconditions;
import com.starrocks.common.Pair;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.privilege.AuthorizationProvider;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AuthorizationMgrEpack extends AuthorizationMgr {

    public AuthorizationMgrEpack(GlobalStateMgr globalStateMgr, AuthorizationProvider provider) {
        super(globalStateMgr, provider);
    }

    @Override
    protected void invalidateRolesInCacheRoleUnlocked(long roleId) throws PrivilegeException {
        Set<Long> badRoles = getAllDescendantsUnlocked(roleId);
        List<Pair<UserIdentity, Set<Long>>> badKeys = new ArrayList<>();
        for (Pair<UserIdentity, Set<Long>> pair : ctxToMergedPrivilegeCollections.asMap().keySet()) {
            UserIdentity userIdentity = pair.first;
            Set<Long> roleIds = pair.second;

            if (userIdentity.isEphemeral()) {
                Preconditions.checkNotNull(roleIds);
            }
            if (roleIds == null) {
                roleIds = getRoleIdsByUser(userIdentity);
            }

            for (long badRoleId : badRoles) {
                if (roleIds.contains(badRoleId)) {
                    badKeys.add(pair);
                    break;
                }
            }
        }
        for (Pair<UserIdentity, Set<Long>> pair : badKeys) {
            ctxToMergedPrivilegeCollections.invalidate(pair);
        }
    }

    @Override
    protected Set<Long> getRoleIdsByUserUnlocked(UserIdentity user) throws PrivilegeException {
        Set<Long> ret = new HashSet<>();

        if (!user.isEphemeral()) {
            for (long roleId : getUserPrivilegeCollectionUnlocked(user).getAllRoles()) {
                // role may be removed
                if (getRolePrivilegeCollectionUnlocked(roleId, false) != null) {
                    ret.add(roleId);
                }
            }
        } else {
            ret = ConnectContext.get().getCurrentRoleIds();
        }

        return ret;
    }
}
