// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.privilege;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.common.Pair;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.privilege.AuthorizationProvider;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PEntryObject;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.privilege.RolePrivilegeCollectionV2;
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

    @Override
    public void initBuiltinRolesAndUsers() {
        try {
            super.initBuiltinRolesAndUsers();

            RolePrivilegeCollectionV2 rolePrivilegeCollection =
                    getRolePrivilegeCollection(PrivilegeBuiltinConstants.CLUSTER_ADMIN_ROLE_ID);

            initPrivilegeCollections(
                    rolePrivilegeCollection,
                    ObjectType.SYSTEM,
                    List.of(PrivilegeTypeEPack.CREATE_WAREHOUSE),
                    null,
                    false);

            List<PEntryObject> objects = new ArrayList<>();
            objects.add(provider.generateObject(ObjectTypeEPack.WAREHOUSE,
                    Lists.newArrayList("*"), GlobalStateMgr.getCurrentState()));
            rolePrivilegeCollection.grant(ObjectTypeEPack.WAREHOUSE,
                    provider.getAvailablePrivType(ObjectTypeEPack.WAREHOUSE), objects, false);
        } catch (PrivilegeException e) {
            // all initial privileges are supposed to be legal
            throw new RuntimeException("Fatal error when initializing built-in role and user", e);
        }
    }
}
