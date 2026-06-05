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
import com.starrocks.catalog.UserIdentity;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.qe.ConnectContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AuthorizationMgrEPack extends AuthorizationMgr {

    public AuthorizationMgrEPack(AuthorizationProvider provider) {
        super(provider);
        initBuiltinRolesAndUsersEPack();
    }

    @Override
    protected void invalidateRolesInCacheRoleUnlocked(long roleId) throws PrivilegeException {
        // The `public` role is implicitly granted to every user and role, so it never
        // appears in any cached entry's roleIds and the per-role reverse lookup below
        // would match nothing. A change to it can affect every cached merged collection,
        // so drop the whole cache instead.
        if (roleId == PrivilegeBuiltinConstants.PUBLIC_ROLE_ID) {
            ctxToMergedPrivilegeCollections.invalidateAll();
            return;
        }
        Set<Long> badRoles = getAllDescendantsUnlocked(roleId);
        List<UserPrivKey> badKeys = new ArrayList<>();
        for (UserPrivKey userPrivKey : ctxToMergedPrivilegeCollections.asMap().keySet()) {
            UserIdentity userIdentity = userPrivKey.userIdentity;
            Set<Long> roleIds = userPrivKey.roleIds;

            if (userIdentity.isEphemeral()) {
                Preconditions.checkNotNull(roleIds);
            }
            if (roleIds == null) {
                roleIds = getRoleIdsByUser(userIdentity);
            }

            for (long badRoleId : badRoles) {
                if (roleIds.contains(badRoleId)) {
                    badKeys.add(userPrivKey);
                    break;
                }
            }
        }
        for (UserPrivKey pair : badKeys) {
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
            RolePrivilegeCollectionV2 rootPrivCollection = getRolePrivilegeCollection(PrivilegeBuiltinConstants.ROOT_ROLE_ID);

            rootPrivCollection.grantWithoutAssertMutable(
                    ObjectTypeEPack.MASKING_POLICY,
                    provider.getAvailablePrivType(ObjectTypeEPack.MASKING_POLICY),
                    List.of(provider.generateObject(ObjectTypeEPack.MASKING_POLICY, Lists.newArrayList("*", "*", "*"))),
                    false
            );

            rootPrivCollection.grantWithoutAssertMutable(
                    ObjectTypeEPack.ROW_ACCESS_POLICY,
                    provider.getAvailablePrivType(ObjectTypeEPack.ROW_ACCESS_POLICY),
                    List.of(provider.generateObject(ObjectTypeEPack.ROW_ACCESS_POLICY, Lists.newArrayList("*", "*", "*"))),
                    false
            );

            rootPrivCollection.grantWithoutAssertMutable(
                    ObjectTypeEPack.FAILOVER_GROUP,
                    provider.getAvailablePrivType(ObjectTypeEPack.FAILOVER_GROUP),
                    List.of(provider.generateObject(ObjectTypeEPack.FAILOVER_GROUP, Lists.newArrayList("*"))),
                    false);

            RolePrivilegeCollectionV2 dbAdminPrivCollection =
                    getRolePrivilegeCollection(PrivilegeBuiltinConstants.DB_ADMIN_ROLE_ID);

            dbAdminPrivCollection.grantWithoutAssertMutable(
                    ObjectTypeEPack.MASKING_POLICY,
                    provider.getAvailablePrivType(ObjectTypeEPack.MASKING_POLICY),
                    List.of(provider.generateObject(ObjectTypeEPack.MASKING_POLICY, Lists.newArrayList("*", "*", "*"))),
                    false
            );

            dbAdminPrivCollection.grantWithoutAssertMutable(
                    ObjectTypeEPack.ROW_ACCESS_POLICY,
                    provider.getAvailablePrivType(ObjectTypeEPack.ROW_ACCESS_POLICY),
                    List.of(provider.generateObject(ObjectTypeEPack.ROW_ACCESS_POLICY, Lists.newArrayList("*", "*", "*"))),
                    false
            );

            dbAdminPrivCollection.grantWithoutAssertMutable(
                    ObjectType.SYSTEM,
                    List.of(PrivilegeTypeEPack.CREATE_FAILOVER_GROUP),
                    Arrays.asList(new PEntryObject[] {null}),
                    false);

            dbAdminPrivCollection.grantWithoutAssertMutable(
                    ObjectTypeEPack.FAILOVER_GROUP,
                    provider.getAvailablePrivType(ObjectTypeEPack.FAILOVER_GROUP),
                    List.of(provider.generateObject(ObjectTypeEPack.FAILOVER_GROUP, Lists.newArrayList("*"))),
                    false);

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
