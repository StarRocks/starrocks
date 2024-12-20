// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.authorization;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.AuthorizationProvider;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PEntryObject;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.authorization.RolePrivilegeCollectionV2;
import com.starrocks.common.Pair;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AuthorizationMgrEPack extends AuthorizationMgr {

    public AuthorizationMgrEPack(GlobalStateMgr globalStateMgr, AuthorizationProvider provider) {
        super(globalStateMgr, provider);
        initBuiltinRolesAndUsersEPack();
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

    public void initBuiltinRolesAndUsersEPack() {
        try {
            List<PEntryObject> allWarehousePriv = new ArrayList<>();
            allWarehousePriv.add(provider.generateObject(ObjectTypeEPack.WAREHOUSE,
                    Lists.newArrayList("*"), globalStateMgr));
            List<PEntryObject> allFailoverGroupPriv = new ArrayList<>();
            allFailoverGroupPriv.add(provider.generateObject(ObjectTypeEPack.FAILOVER_GROUP,
                    Lists.newArrayList("*"), globalStateMgr));

            RolePrivilegeCollectionV2 rootPrivCollection = getRolePrivilegeCollection(PrivilegeBuiltinConstants.ROOT_ROLE_ID);
            rootPrivCollection.grantWithoutAssertMutable(ObjectTypeEPack.WAREHOUSE,
                    provider.getAvailablePrivType(ObjectTypeEPack.WAREHOUSE), allWarehousePriv, false);
            rootPrivCollection.grantWithoutAssertMutable(ObjectTypeEPack.FAILOVER_GROUP,
                    provider.getAvailablePrivType(ObjectTypeEPack.FAILOVER_GROUP), allFailoverGroupPriv, false);

            RolePrivilegeCollectionV2 dbAdminPrivCollection = getRolePrivilegeCollection(
                    PrivilegeBuiltinConstants.DB_ADMIN_ROLE_ID);

            dbAdminPrivCollection.grantWithoutAssertMutable(ObjectType.SYSTEM,
                    List.of(PrivilegeTypeEPack.CREATE_FAILOVER_GROUP),
                    Arrays.asList(new PEntryObject[] { null }),
                    false);

            dbAdminPrivCollection.grantWithoutAssertMutable(ObjectTypeEPack.FAILOVER_GROUP,
                    provider.getAvailablePrivType(ObjectTypeEPack.FAILOVER_GROUP), allFailoverGroupPriv, false);

            RolePrivilegeCollectionV2 clusterAdminPrivCollection =
                    getRolePrivilegeCollection(PrivilegeBuiltinConstants.CLUSTER_ADMIN_ROLE_ID);

            clusterAdminPrivCollection.grantWithoutAssertMutable(ObjectType.SYSTEM,
                    List.of(PrivilegeTypeEPack.CREATE_WAREHOUSE),
                    Arrays.asList(new PEntryObject[] {null}),
                    false);

            clusterAdminPrivCollection.grantWithoutAssertMutable(ObjectTypeEPack.WAREHOUSE,
                    provider.getAvailablePrivType(ObjectTypeEPack.WAREHOUSE), allWarehousePriv, false);            
        } catch (PrivilegeException e) {
            // all initial privileges are supposed to be legal
            throw new RuntimeException("Fatal error when initializing built-in role and user", e);
        }
    }

    public void loadV2(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        super.loadV2(reader);
        initBuiltinRolesAndUsersEPack();
    }
}
