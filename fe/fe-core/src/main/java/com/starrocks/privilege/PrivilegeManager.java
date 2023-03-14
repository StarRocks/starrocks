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

package com.starrocks.privilege;

import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.SystemId;
import com.starrocks.persist.RolePrivilegeCollectionInfo;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.sql.ast.RevokeRoleStmt;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class PrivilegeManager {
    private static final Logger LOG = LogManager.getLogger(PrivilegeManager.class);

    @SerializedName(value = "r")
    private final Map<String, Long> roleNameToId;
    @SerializedName(value = "i")
    private short pluginId;
    @SerializedName(value = "v")
    private short pluginVersion;

    protected AuthorizationProvider provider;

    private GlobalStateMgr globalStateMgr;

    protected Map<UserIdentity, UserPrivilegeCollection> userToPrivilegeCollection;

    private static final int MAX_NUM_CACHED_MERGED_PRIVILEGE_COLLECTION = 1000;
    private static final int CACHED_MERGED_PRIVILEGE_COLLECTION_EXPIRE_MIN = 60;
    protected LoadingCache<Pair<UserIdentity, Set<Long>>, PrivilegeCollection> ctxToMergedPrivilegeCollections =
            CacheBuilder.newBuilder()
                    .maximumSize(MAX_NUM_CACHED_MERGED_PRIVILEGE_COLLECTION)
                    .expireAfterAccess(CACHED_MERGED_PRIVILEGE_COLLECTION_EXPIRE_MIN, TimeUnit.MINUTES)
                    .build(new CacheLoader<Pair<UserIdentity, Set<Long>>, PrivilegeCollection>() {
                        @Override
                        public PrivilegeCollection load(@NotNull Pair<UserIdentity, Set<Long>> userIdentitySetPair)
                                throws Exception {
                            return loadPrivilegeCollection(userIdentitySetPair.first, userIdentitySetPair.second);
                        }
                    });

    private final ReentrantReadWriteLock userLock;

    // set by load() to distinguish brand-new environment with upgraded environment
    private boolean isLoaded = false;

    protected Map<Long, RolePrivilegeCollection> roleIdToPrivilegeCollection;
    private final ReentrantReadWriteLock roleLock;

    // only used in deserialization
    protected PrivilegeManager() {
        roleNameToId = new HashMap<>();
        userToPrivilegeCollection = new HashMap<>();
        roleIdToPrivilegeCollection = new HashMap<>();
        userLock = new ReentrantReadWriteLock();
        roleLock = new ReentrantReadWriteLock();
    }

    public PrivilegeManager(GlobalStateMgr globalStateMgr, AuthorizationProvider provider) {
        this.globalStateMgr = globalStateMgr;
        if (provider == null) {
            this.provider = new DefaultAuthorizationProvider();
        } else {
            this.provider = provider;
        }
        pluginId = this.provider.getPluginId();
        pluginVersion = this.provider.getPluginVersion();
        roleNameToId = new HashMap<>();
        userLock = new ReentrantReadWriteLock();
        roleLock = new ReentrantReadWriteLock();
        userToPrivilegeCollection = new HashMap<>();
        roleIdToPrivilegeCollection = new HashMap<>();
        initBuiltinRolesAndUsers();
    }

    public void initBuiltinRolesAndUsers() {
        try {
            // built-in role ids are hard-coded negative numbers because globalStateMgr.getNextId() cannot be called by a follower
            // 1. builtin root role
            RolePrivilegeCollection rolePrivilegeCollection = initBuiltinRoleUnlocked(PrivilegeBuiltinConstants.ROOT_ROLE_ID,
                    PrivilegeBuiltinConstants.ROOT_ROLE_NAME);
            // GRANT ALL ON ALL
            for (ObjectType objectType : provider.getAllPrivObjectTypes()) {
                initPrivilegeCollectionAllObjects(rolePrivilegeCollection, objectType, provider.getAvailablePrivType(objectType));
            }
            rolePrivilegeCollection.disableMutable();  // not mutable

            // 2. builtin db_admin role
            rolePrivilegeCollection = initBuiltinRoleUnlocked(PrivilegeBuiltinConstants.DB_ADMIN_ROLE_ID,
                    PrivilegeBuiltinConstants.DB_ADMIN_ROLE_NAME);
            // ALL system but GRANT AND NODE
            List<PrivilegeType> actionWithoutNodeGrant = provider.getAvailablePrivType(ObjectType.SYSTEM).stream().filter(
                    x -> !x.equals(PrivilegeType.GRANT) && !x.equals(PrivilegeType.NODE)).collect(Collectors.toList());
            initPrivilegeCollections(rolePrivilegeCollection, ObjectType.SYSTEM, actionWithoutNodeGrant, null,
                    false);
            for (ObjectType t : Arrays.asList(
                    ObjectType.CATALOG,
                    ObjectType.DATABASE,
                    ObjectType.TABLE,
                    ObjectType.VIEW,
                    ObjectType.MATERIALIZED_VIEW,
                    ObjectType.RESOURCE,
                    ObjectType.RESOURCE_GROUP,
                    ObjectType.FUNCTION,
                    ObjectType.GLOBAL_FUNCTION)) {
                initPrivilegeCollectionAllObjects(rolePrivilegeCollection, t, provider.getAvailablePrivType(t));
            }
            rolePrivilegeCollection.disableMutable(); // not mutable

            // 3. cluster_admin
            rolePrivilegeCollection = initBuiltinRoleUnlocked(PrivilegeBuiltinConstants.CLUSTER_ADMIN_ROLE_ID,
                    PrivilegeBuiltinConstants.CLUSTER_ADMIN_ROLE_NAME);
            // GRANT NODE ON SYSTEM
            initPrivilegeCollections(
                    rolePrivilegeCollection,
                    ObjectType.SYSTEM,
                    Collections.singletonList(PrivilegeType.NODE),
                    null,
                    false);
            rolePrivilegeCollection.disableMutable(); // not mutable

            // 4. user_admin
            rolePrivilegeCollection = initBuiltinRoleUnlocked(PrivilegeBuiltinConstants.USER_ADMIN_ROLE_ID,
                    PrivilegeBuiltinConstants.USER_ADMIN_ROLE_NAME);
            // GRANT GRANT ON SYSTEM
            initPrivilegeCollections(
                    rolePrivilegeCollection,
                    ObjectType.SYSTEM,
                    Collections.singletonList(PrivilegeType.GRANT),
                    null,
                    false);
            ObjectType t = ObjectType.USER;
            initPrivilegeCollectionAllObjects(rolePrivilegeCollection, t, provider.getAvailablePrivType(t));
            rolePrivilegeCollection.disableMutable(); // not mutable

            // 5. public
            rolePrivilegeCollection = initBuiltinRoleUnlocked(PrivilegeBuiltinConstants.PUBLIC_ROLE_ID,
                    PrivilegeBuiltinConstants.PUBLIC_ROLE_NAME);
            // GRANT SELECT ON ALL TABLES IN information_schema
            List<PEntryObject> object = Collections.singletonList(new TablePEntryObject(
                    Long.toString(SystemId.INFORMATION_SCHEMA_DB_ID), TablePEntryObject.ALL_TABLES_UUID));
            rolePrivilegeCollection.grant(ObjectType.TABLE, Collections.singletonList(PrivilegeType.SELECT), object,
                    false);

            // 6. builtin user root
            UserPrivilegeCollection rootCollection = new UserPrivilegeCollection();
            rootCollection.grantRole(PrivilegeBuiltinConstants.ROOT_ROLE_ID);
            rootCollection.setDefaultRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
            userToPrivilegeCollection.put(UserIdentity.ROOT, rootCollection);
        } catch (PrivilegeException e) {
            // all initial privileges are supposed to be legal
            throw new RuntimeException("Fatal error when initializing built-in role and user", e);
        }
    }

    // called by initBuiltinRolesAndUsers()
    private void initPrivilegeCollections(PrivilegeCollection collection, ObjectType objectType,
                                          List<PrivilegeType> actionList,
                                          List<String> tokens, boolean isGrant) throws PrivilegeException {
        List<PEntryObject> object;
        if (tokens != null) {
            object = Collections.singletonList(provider.generateObject(objectType, tokens, globalStateMgr));
        } else {
            object = Arrays.asList(new PEntryObject[] {null});
        }
        collection.grant(objectType, actionList, object, isGrant);
    }

    // called by initBuiltinRolesAndUsers()
    private void initPrivilegeCollectionAllObjects(
            PrivilegeCollection collection, ObjectType objectType, List<PrivilegeType> actionList)
            throws PrivilegeException {
        List<PEntryObject> objects = new ArrayList<>();
        switch (objectType) {
            case TABLE:
                objects.add(provider.generateObject(objectType,
                        Lists.newArrayList("*", "*", "*"), globalStateMgr));
                collection.grant(objectType, actionList, objects, false);
                break;
            case VIEW:
            case MATERIALIZED_VIEW:
            case FUNCTION:
            case DATABASE:
                objects.add(provider.generateObject(objectType,
                        Lists.newArrayList("*", "*"), globalStateMgr));
                collection.grant(objectType, actionList, objects, false);
                break;

            case USER:
                objects.add(provider.generateUserObject(objectType, null, globalStateMgr));
                collection.grant(objectType, actionList, objects, false);
                break;

            case RESOURCE:
            case CATALOG:
            case RESOURCE_GROUP:
            case GLOBAL_FUNCTION:
                objects.add(provider.generateObject(objectType,
                        Lists.newArrayList("*"), globalStateMgr));
                collection.grant(objectType, actionList, objects, false);
                break;

            case SYSTEM:
                collection.grant(objectType, actionList, Arrays.asList(new PEntryObject[] {null}), false);
                break;

            default:
                throw new PrivilegeException("unsupported type " + objectType);
        }
    }

    // called by initBuiltinRolesAndUsers()
    private RolePrivilegeCollection initBuiltinRoleUnlocked(long roleId, String name) {
        RolePrivilegeCollection collection = new RolePrivilegeCollection(
                name, RolePrivilegeCollection.RoleFlags.MUTABLE);
        roleIdToPrivilegeCollection.put(roleId, collection);
        roleNameToId.put(name, roleId);
        LOG.info("create built-in role {}[{}]", name, roleId);
        return collection;
    }

    private void userReadLock() {
        userLock.readLock().lock();
    }

    private void userReadUnlock() {
        userLock.readLock().unlock();
    }

    private void userWriteLock() {
        userLock.writeLock().lock();
    }

    private void userWriteUnlock() {
        userLock.writeLock().unlock();
    }

    private void roleReadLock() {
        roleLock.readLock().lock();
    }

    private void roleReadUnlock() {
        roleLock.readLock().unlock();
    }

    private void roleWriteLock() {
        roleLock.writeLock().lock();
    }

    private void roleWriteUnlock() {
        roleLock.writeLock().unlock();
    }

    public void grant(GrantPrivilegeStmt stmt) throws DdlException {
        try {
            if (stmt.getRole() != null) {
                grantToRole(
                        stmt.getObjectType(),
                        stmt.getPrivilegeTypes(),
                        stmt.getObjectList(),
                        stmt.isWithGrantOption(),
                        stmt.getRole());
            } else {
                grantToUser(
                        stmt.getObjectType(),
                        stmt.getPrivilegeTypes(),
                        stmt.getObjectList(),
                        stmt.isWithGrantOption(),
                        stmt.getUserIdentity());
            }
        } catch (PrivilegeException e) {
            throw new DdlException("failed to grant: " + e.getMessage(), e);
        }
    }

    protected void grantToUser(
            ObjectType type,
            List<PrivilegeType> privilegeTypes,
            List<PEntryObject> objects,
            boolean isGrant,
            UserIdentity userIdentity) throws PrivilegeException {
        userWriteLock();
        try {
            UserPrivilegeCollection collection = getUserPrivilegeCollectionUnlocked(userIdentity);
            collection.grant(type, privilegeTypes, objects, isGrant);
            globalStateMgr.getEditLog().logUpdateUserPrivilege(
                    userIdentity, collection, provider.getPluginId(), provider.getPluginVersion());
            invalidateUserInCache(userIdentity);
        } finally {
            userWriteUnlock();
        }
    }

    protected void grantToRole(
            ObjectType objectType,
            List<PrivilegeType> privilegeTypes,
            List<PEntryObject> objects,
            boolean isGrant,
            String roleName) throws PrivilegeException {
        roleWriteLock();
        try {
            long roleId = getRoleIdByNameNoLock(roleName);
            invalidateRolesInCacheRoleUnlocked(roleId);
            RolePrivilegeCollection collection = getRolePrivilegeCollectionUnlocked(roleId, true);
            collection.grant(objectType, privilegeTypes, objects, isGrant);

            Map<Long, RolePrivilegeCollection> rolePrivCollectionModified = new HashMap<>();
            rolePrivCollectionModified.put(roleId, collection);

            globalStateMgr.getEditLog().logUpdateRolePrivilege(
                    rolePrivCollectionModified, provider.getPluginId(), provider.getPluginVersion());
        } finally {
            roleWriteUnlock();
        }
    }

    public void revoke(RevokePrivilegeStmt stmt) throws DdlException {
        try {
            if (stmt.getRole() != null) {
                revokeFromRole(
                        stmt.getObjectType(),
                        stmt.getPrivilegeTypes(),
                        stmt.getObjectList(),
                        stmt.getRole());
            } else {
                revokeFromUser(
                        stmt.getObjectType(),
                        stmt.getPrivilegeTypes(),
                        stmt.getObjectList(),
                        stmt.getUserIdentity());
            }
        } catch (PrivilegeException e) {
            throw new DdlException(e.getMessage());
        }
    }

    protected void revokeFromUser(
            ObjectType objectType,
            List<PrivilegeType> privilegeTypes,
            List<PEntryObject> objects,
            UserIdentity userIdentity) throws PrivilegeException {
        userWriteLock();
        try {
            UserPrivilegeCollection collection = getUserPrivilegeCollectionUnlocked(userIdentity);
            collection.revoke(objectType, privilegeTypes, objects);
            globalStateMgr.getEditLog().logUpdateUserPrivilege(
                    userIdentity, collection, provider.getPluginId(), provider.getPluginVersion());
            invalidateUserInCache(userIdentity);
        } finally {
            userWriteUnlock();
        }
    }

    protected void revokeFromRole(
            ObjectType objectType,
            List<PrivilegeType> privilegeTypes,
            List<PEntryObject> objects,
            String roleName) throws PrivilegeException {
        roleWriteLock();
        try {
            long roleId = getRoleIdByNameNoLock(roleName);
            RolePrivilegeCollection collection = getRolePrivilegeCollectionUnlocked(roleId, true);
            collection.revoke(objectType, privilegeTypes, objects);
            invalidateRolesInCacheRoleUnlocked(roleId);

            Map<Long, RolePrivilegeCollection> rolePrivCollectionModified = new HashMap<>();
            rolePrivCollectionModified.put(roleId, collection);

            globalStateMgr.getEditLog().logUpdateRolePrivilege(
                    rolePrivCollectionModified, provider.getPluginId(), provider.getPluginVersion());
        } finally {
            roleWriteUnlock();
        }
    }

    public void grantRole(GrantRoleStmt stmt) throws DdlException {
        try {
            if (stmt.getUserIdentity() != null) {
                grantRoleToUser(stmt.getGranteeRole(), stmt.getUserIdentity());
            } else {
                grantRoleToRole(stmt.getGranteeRole(), stmt.getRole());
            }
        } catch (PrivilegeException e) {
            throw new DdlException("failed to grant role: " + e.getMessage(), e);
        }
    }

    protected void grantRoleToUser(List<String> parentRoleName, UserIdentity user) throws PrivilegeException {
        userWriteLock();
        try {
            UserPrivilegeCollection userPrivilegeCollection = getUserPrivilegeCollectionUnlocked(user);

            roleReadLock();
            try {
                for (String parentRole : parentRoleName) {
                    long roleId = getRoleIdByNameNoLock(parentRole);

                    // public cannot be revoked!
                    if (roleId == PrivilegeBuiltinConstants.PUBLIC_ROLE_ID) {
                        throw new PrivilegeException("Granting role PUBLIC has no effect.  " +
                                "Every user and role has role PUBLIC implicitly granted.");
                    }

                    // temporarily add parent role to user to verify predecessors
                    userPrivilegeCollection.grantRole(roleId);
                    boolean verifyDone = false;
                    try {
                        Set<Long> result = getAllPredecessorsUnlocked(userPrivilegeCollection);
                        if (result.size() > Config.privilege_max_total_roles_per_user) {
                            LOG.warn("too many predecessor roles {} for user {}", result, user);
                            throw new PrivilegeException(String.format(
                                    "%s has total %d predecessor roles > %d!",
                                    user, result.size(), Config.privilege_max_total_roles_per_user));
                        }
                        verifyDone = true;
                    } finally {
                        if (!verifyDone) {
                            userPrivilegeCollection.revokeRole(roleId);
                        }
                    }
                }
            } finally {
                roleReadUnlock();
            }

            globalStateMgr.getEditLog().logUpdateUserPrivilege(
                    user, userPrivilegeCollection, provider.getPluginId(), provider.getPluginVersion());
            invalidateUserInCache(user);
            LOG.info("grant role {} to user {}", Joiner.on(", ").join(parentRoleName), user);
        } finally {
            userWriteUnlock();
        }
    }

    protected void grantRoleToRole(List<String> parentRoleName, String roleName) throws PrivilegeException {
        roleWriteLock();
        try {
            long roleId = getRoleIdByNameNoLock(roleName);
            RolePrivilegeCollection collection = getRolePrivilegeCollectionUnlocked(roleId, true);

            Map<Long, RolePrivilegeCollection> rolePrivCollectionModified = new HashMap<>();
            rolePrivCollectionModified.put(roleId, collection);
            for (String parentRole : parentRoleName) {
                long parentRoleId = getRoleIdByNameNoLock(parentRole);

                if (parentRoleId == PrivilegeBuiltinConstants.PUBLIC_ROLE_ID) {
                    throw new PrivilegeException("Granting role PUBLIC has no effect.  " +
                            "Every user and role has role PUBLIC implicitly granted.");
                }

                RolePrivilegeCollection parentCollection = getRolePrivilegeCollectionUnlocked(parentRoleId, true);

                // to avoid circle, verify roleName is not predecessor role of parentRoleName
                Set<Long> parentRolePredecessors = getAllPredecessorsUnlocked(parentRoleId);
                if (parentRolePredecessors.contains(roleId)) {
                    throw new PrivilegeException(String.format("role %s[%d] is already a predecessor role of %s[%d]",
                            roleName, roleId, parentRole, parentRoleId));
                }

                // temporarily add sub role to parent role to verify inheritance depth
                boolean verifyDone = false;
                parentCollection.addSubRole(roleId);
                try {
                    // verify role inheritance depth
                    parentRolePredecessors = getAllPredecessorsUnlocked(parentRoleId);
                    parentRolePredecessors.add(parentRoleId);
                    for (long i : parentRolePredecessors) {
                        long cnt = getMaxRoleInheritanceDepthInner(0, i);
                        if (cnt > Config.privilege_max_role_depth) {
                            String name = getRolePrivilegeCollectionUnlocked(i, true).getName();
                            throw new PrivilegeException(String.format(
                                    "role inheritance depth for %s[%d] is %d > %d",
                                    name, i, cnt, Config.privilege_max_role_depth));
                        }
                    }

                    verifyDone = true;
                } finally {
                    if (!verifyDone) {
                        parentCollection.removeSubRole(roleId);
                    }
                }

                collection.addParentRole(parentRoleId);
                rolePrivCollectionModified.put(parentRoleId, parentCollection);
            }

            invalidateRolesInCacheRoleUnlocked(roleId);

            // write journal to update privilege collections of both role & parent role
            RolePrivilegeCollectionInfo info = new RolePrivilegeCollectionInfo(
                    rolePrivCollectionModified, provider.getPluginId(), provider.getPluginVersion());
            globalStateMgr.getEditLog().logUpdateRolePrivilege(info);
            LOG.info("grant role {}[{}] to role {}[{}]", parentRoleName,
                    Joiner.on(", ").join(parentRoleName), roleName, roleId);
        } finally {
            roleWriteUnlock();
        }
    }

    public void revokeRole(RevokeRoleStmt stmt) throws DdlException {
        try {
            if (stmt.getUserIdentity() != null) {
                revokeRoleFromUser(stmt.getGranteeRole(), stmt.getUserIdentity());
            } else {
                revokeRoleFromRole(stmt.getGranteeRole(), stmt.getRole());
            }
        } catch (PrivilegeException e) {
            throw new DdlException("failed to revoke role: " + e.getMessage(), e);
        }
    }

    protected void revokeRoleFromUser(List<String> roleNameList, UserIdentity user) throws PrivilegeException {
        userWriteLock();
        try {
            UserPrivilegeCollection collection = getUserPrivilegeCollectionUnlocked(user);
            roleReadLock();
            try {
                for (String roleName : roleNameList) {
                    long roleId = getRoleIdByNameNoLock(roleName);
                    // public cannot be revoked!
                    if (roleId == PrivilegeBuiltinConstants.PUBLIC_ROLE_ID) {
                        throw new PrivilegeException("Revoking role PUBLIC has no effect.  " +
                                "Every user and role has role PUBLIC implicitly granted.");
                    }
                    collection.revokeRole(roleId);
                }
            } finally {
                roleReadUnlock();
            }
            globalStateMgr.getEditLog().logUpdateUserPrivilege(
                    user, collection, provider.getPluginId(), provider.getPluginVersion());
            invalidateUserInCache(user);
            LOG.info("revoke role {} from user {}", roleNameList.toString(), user);
        } finally {
            userWriteUnlock();
        }
    }

    protected void revokeRoleFromRole(List<String> parentRoleNameList, String roleName) throws PrivilegeException {
        roleWriteLock();
        try {
            long roleId = getRoleIdByNameNoLock(roleName);
            RolePrivilegeCollection collection = getRolePrivilegeCollectionUnlocked(roleId, true);

            for (String parentRoleName : parentRoleNameList) {
                long parentRoleId = getRoleIdByNameNoLock(parentRoleName);

                if (parentRoleId == PrivilegeBuiltinConstants.PUBLIC_ROLE_ID) {
                    throw new PrivilegeException("Revoking role PUBLIC has no effect.  " +
                            "Every user and role has role PUBLIC implicitly granted.");
                }

                RolePrivilegeCollection parentCollection =
                        getRolePrivilegeCollectionUnlocked(parentRoleId, true);
                parentCollection.removeSubRole(roleId);
                collection.removeParentRole(parentRoleId);
            }

            Map<Long, RolePrivilegeCollection> rolePrivCollectionModified = new HashMap<>();
            rolePrivCollectionModified.put(roleId, collection);

            List<Long> parentRoleIdList = new ArrayList<>();
            for (String parentRoleName : parentRoleNameList) {
                long parentRoleId = getRoleIdByNameNoLock(parentRoleName);
                RolePrivilegeCollection parentCollection =
                        getRolePrivilegeCollectionUnlocked(parentRoleId, true);
                parentRoleIdList.add(parentRoleId);
                rolePrivCollectionModified.put(parentRoleId, parentCollection);
            }

            // write journal to update privilege collections of both role & parent role
            RolePrivilegeCollectionInfo info = new RolePrivilegeCollectionInfo(
                    rolePrivCollectionModified, provider.getPluginId(), provider.getPluginVersion());
            globalStateMgr.getEditLog().logUpdateRolePrivilege(info);
            invalidateRolesInCacheRoleUnlocked(roleId);
            LOG.info("revoke role {}[{}] from role {}[{}]",
                    parentRoleNameList.toString(), parentRoleIdList.toString(), roleName, roleId);
        } finally {
            roleWriteUnlock();
        }
    }

    public void validateGrant(ObjectType objectType, List<PrivilegeType> privilegeTypes, List<PEntryObject> objects)
            throws PrivilegeException {
        provider.validateGrant(objectType, privilegeTypes, objects);
    }


    public static Set<Long> getOwnedRolesByUser(UserIdentity userIdentity) throws PrivilegeException {
        PrivilegeManager manager = GlobalStateMgr.getCurrentState().getPrivilegeManager();
        try {
            manager.userReadLock();
            UserPrivilegeCollection userCollection = manager.getUserPrivilegeCollectionUnlocked(userIdentity);
            return userCollection.getAllRoles();
        } finally {
            manager.userReadUnlock();
        }
    }

    protected boolean checkAction(
            PrivilegeCollection collection, ObjectType objectType, PrivilegeType privilegeType, List<String> objectNames)
            throws PrivilegeException {
        if (objectNames == null) {
            return provider.check(objectType, privilegeType, null, collection);
        } else {
            PEntryObject object = provider.generateObject(
                    objectType, objectNames, globalStateMgr);
            return provider.check(objectType, privilegeType, object, collection);
        }
    }

    public boolean canExecuteAs(ConnectContext context, UserIdentity impersonateUser) {
        try {
            PrivilegeCollection collection = mergePrivilegeCollection(context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds());
            PEntryObject object = provider.generateUserObject(ObjectType.USER, impersonateUser, globalStateMgr);
            return provider.check(ObjectType.USER, PrivilegeType.IMPERSONATE, object, collection);
        } catch (PrivilegeException e) {
            LOG.warn("caught exception in canExecuteAs() user[{}]", impersonateUser, e);
            return false;
        }
    }

    public boolean allowGrant(ConnectContext context, ObjectType type, List<PrivilegeType> wants, List<PEntryObject> objects) {
        try {
            PrivilegeCollection collection = mergePrivilegeCollection(context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds());
            // check for GRANT or WITH GRANT OPTION in the specific type
            return checkAction(collection, ObjectType.SYSTEM, PrivilegeType.GRANT, null)
                    || provider.allowGrant(type, wants, objects, collection);
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when allowGrant", e);
            return false;
        }
    }

    public void replayUpdateUserPrivilegeCollection(
            UserIdentity user,
            UserPrivilegeCollection privilegeCollection,
            short pluginId,
            short pluginVersion) throws PrivilegeException {
        userWriteLock();
        try {
            provider.upgradePrivilegeCollection(privilegeCollection, pluginId, pluginVersion);
            userToPrivilegeCollection.put(user, privilegeCollection);
            invalidateUserInCache(user);
            LOG.info("replayed update user {}", user);
        } finally {
            userWriteUnlock();
        }
    }

    /**
     * init all builtin privilege when a user is created, called by AuthenticationManager
     */
    public UserPrivilegeCollection onCreateUser(UserIdentity user, List<String> defaultRoleName) throws PrivilegeException {
        userWriteLock();
        try {
            UserPrivilegeCollection privilegeCollection = new UserPrivilegeCollection();

            if (!defaultRoleName.isEmpty()) {
                Set<Long> roleIds = new HashSet<>();
                for (String role : defaultRoleName) {
                    Long roleId = getRoleIdByNameNoLock(role);
                    privilegeCollection.grantRole(roleId);
                    roleIds.add(roleId);
                }
                privilegeCollection.setDefaultRoleIds(roleIds);
            }

            userToPrivilegeCollection.put(user, privilegeCollection);
            LOG.info("user privilege for {} is created, role {} is granted", user, PrivilegeBuiltinConstants.PUBLIC_ROLE_NAME);
            return privilegeCollection;
        } finally {
            userWriteUnlock();
        }
    }

    /**
     * drop user privilege collection when a user is dropped, called by AuthenticationManager
     */
    public void onDropUser(UserIdentity user) {
        userWriteLock();
        try {
            userToPrivilegeCollection.remove(user);
            invalidateUserInCache(user);
        } finally {
            userWriteUnlock();
        }
    }

    public short getProviderPluginId() {
        return provider.getPluginId();
    }

    public short getProviderPluginVersion() {
        return provider.getPluginVersion();
    }

    /**
     * read from cache
     */
    protected PrivilegeCollection mergePrivilegeCollection(UserIdentity userIdentity, Set<Long> roleIds)
            throws PrivilegeException {
        try {
            return ctxToMergedPrivilegeCollections.get(new Pair<>(userIdentity, roleIds));
        } catch (ExecutionException e) {
            String errMsg = String.format("failed merge privilege collection on %s with roles %s", userIdentity, roleIds);
            PrivilegeException exception = new PrivilegeException(errMsg);
            exception.initCause(e);
            throw exception;
        }
    }

    /**
     * used for cache to do the actual merge job
     */
    protected PrivilegeCollection loadPrivilegeCollection(UserIdentity userIdentity, Set<Long> roleIds)
            throws PrivilegeException {
        PrivilegeCollection collection = new PrivilegeCollection();
        userReadLock();
        try {
            UserPrivilegeCollection userPrivilegeCollection = getUserPrivilegeCollectionUnlocked(userIdentity);
            collection.merge(userPrivilegeCollection);
            roleReadLock();
            try {
                // 1. get all parent roles by default, but can be specified with `SET ROLE` statement
                if (roleIds == null) {
                    roleIds = new HashSet<>(userPrivilegeCollection.getAllRoles());
                }

                // 2. get all predecessors base on step 1
                // The main purpose of the secondary verification of UserPrivilegeCollection here is.
                // Because the user's permissions may be revoke while the session is not disconnected,
                // the role list stored in the session cannot be changed at this time
                // (because the current session and the session initiated by the revoke operation may not be the same),
                // but for the user The operation will cause the cache to invalid, so in the next load process after
                // the cache fails, we need to determine whether the user still has access to this role.
                Set<Long> validRoleIds = new HashSet<>(userPrivilegeCollection.getAllRoles());
                validRoleIds.retainAll(roleIds);
                validRoleIds = getAllPredecessorsUnlocked(validRoleIds);

                // 3. merge privilege collections of all predecessors
                for (long roleId : validRoleIds) {
                    // Because the drop role is an asynchronous behavior, the parentRole may not exist.
                    // Here, for the role that does not exist, choose to ignore it directly
                    RolePrivilegeCollection rolePrivilegeCollection = getRolePrivilegeCollectionUnlocked(roleId, false);
                    if (rolePrivilegeCollection != null) {
                        collection.merge(rolePrivilegeCollection);
                    }
                }

                RolePrivilegeCollection rolePrivilegeCollection =
                        getRolePrivilegeCollectionUnlocked(PrivilegeBuiltinConstants.PUBLIC_ROLE_ID, false);
                if (rolePrivilegeCollection != null) {
                    collection.merge(rolePrivilegeCollection);
                }

            } finally {
                roleReadUnlock();
            }
        } finally {
            userReadUnlock();
        }
        return collection;
    }

    /**
     * if the privileges of a role are changed, call this function to invalidate cache
     * requires role lock
     */
    protected void invalidateRolesInCacheRoleUnlocked(long roleId) throws PrivilegeException {
        Set<Long> badRoles = getAllDescendantsUnlocked(roleId);
        List<Pair<UserIdentity, Set<Long>>> badKeys = new ArrayList<>();
        for (Pair<UserIdentity, Set<Long>> pair : ctxToMergedPrivilegeCollections.asMap().keySet()) {
            Set<Long> roleIds = pair.second;
            if (roleIds == null) {
                roleIds = getRoleIdsByUser(pair.first);
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

    /**
     * if the privileges of a user are changed, call this function to invalidate cache
     * require not extra lock.
     */
    protected void invalidateUserInCache(UserIdentity userIdentity) {
        List<Pair<UserIdentity, Set<Long>>> badKeys = new ArrayList<>();
        for (Pair<UserIdentity, Set<Long>> pair : ctxToMergedPrivilegeCollections.asMap().keySet()) {
            if (pair.first.equals(userIdentity)) {
                badKeys.add(pair);
            }
        }
        for (Pair<UserIdentity, Set<Long>> pair : badKeys) {
            ctxToMergedPrivilegeCollections.invalidate(pair);
        }
    }

    public UserPrivilegeCollection getUserPrivilegeCollectionUnlocked(UserIdentity userIdentity)
            throws PrivilegeException {
        UserPrivilegeCollection userCollection = userToPrivilegeCollection.get(userIdentity);
        if (userCollection == null) {
            throw new PrivilegeException("cannot find user " + (userIdentity == null ? "null" :
                    userIdentity.toString()));
        }
        return userCollection;
    }

    public List<String> getAllUsers() {
        userReadLock();
        try {
            List<String> users = Lists.newArrayList();
            Set<UserIdentity> userIdentities = userToPrivilegeCollection.keySet();
            for (UserIdentity userIdentity : userIdentities) {
                users.add(userIdentity.toString());
            }
            return users;
        } finally {
            userReadUnlock();
        }
    }

    // return null if not exists
    protected UserPrivilegeCollection getUserPrivilegeCollectionUnlockedAllowNull(UserIdentity userIdentity) {
        return userToPrivilegeCollection.get(userIdentity);
    }

    public RolePrivilegeCollection getRolePrivilegeCollectionUnlocked(long roleId, boolean exceptionIfNotExists)
            throws PrivilegeException {
        RolePrivilegeCollection collection = roleIdToPrivilegeCollection.get(roleId);
        if (collection == null) {
            if (exceptionIfNotExists) {
                throw new PrivilegeException("cannot find role" + roleId);
            } else {
                return null;
            }
        }
        return collection;
    }

    public List<String> getGranteeRoleDetailsForRole(String roleName) {
        roleReadLock();
        try {
            Long roleId = getRoleIdByNameAllowNull(roleName);
            if (roleId == null) {
                throw new SemanticException("cannot find role " + roleName);
            }

            RolePrivilegeCollection rolePrivilegeCollection =
                    getRolePrivilegeCollectionUnlocked(roleId, true);

            List<String> parentRoleNameList = new ArrayList<>();
            for (Long parentRoleId : rolePrivilegeCollection.getParentRoleIds()) {
                // Because the drop role is an asynchronous behavior, the parentRole may not exist.
                // Here, for the role that does not exist, choose to ignore it directly
                RolePrivilegeCollection parentRolePriv =
                        getRolePrivilegeCollectionUnlocked(parentRoleId, false);
                if (parentRolePriv != null) {
                    parentRoleNameList.add(parentRolePriv.getName());
                }
            }

            if (!parentRoleNameList.isEmpty()) {
                return Lists.newArrayList(roleName, null,
                        AstToSQLBuilder.toSQL(new GrantRoleStmt(parentRoleNameList, roleName)));
            }
            return null;
        } catch (PrivilegeException e) {
            throw new SemanticException(e.getMessage());
        } finally {
            roleReadUnlock();
        }
    }

    public Map<ObjectType, List<PrivilegeCollection.PrivilegeEntry>> getTypeToPrivilegeEntryListByRole(String roleName) {
        roleReadLock();
        try {
            Long roleId = getRoleIdByNameAllowNull(roleName);
            if (roleId == null) {
                throw new SemanticException("cannot find role " + roleName);
            }

            RolePrivilegeCollection rolePrivilegeCollection =
                    getRolePrivilegeCollectionUnlocked(roleId, true);
            return rolePrivilegeCollection.getTypeToPrivilegeEntryList();
        } catch (PrivilegeException e) {
            throw new SemanticException(e.getMessage());
        } finally {
            roleReadUnlock();
        }
    }

    public List<String> getGranteeRoleDetailsForUser(UserIdentity userIdentity) {
        userReadLock();
        try {
            UserPrivilegeCollection userPrivilegeCollection = getUserPrivilegeCollectionUnlocked(userIdentity);
            Set<Long> allRoles = userPrivilegeCollection.getAllRoles();

            roleReadLock();
            try {
                List<String> parentRoleNameList = new ArrayList<>();
                for (Long roleId : allRoles) {
                    // Because the drop role is an asynchronous behavior, the parentRole may not exist.
                    // Here, for the role that does not exist, choose to ignore it directly
                    RolePrivilegeCollection parentRolePriv =
                            getRolePrivilegeCollectionUnlocked(roleId, false);
                    if (parentRolePriv != null) {
                        parentRoleNameList.add(parentRolePriv.getName());
                    }
                }

                if (!parentRoleNameList.isEmpty()) {
                    return Lists.newArrayList(userIdentity.toString(), null,
                            AstToSQLBuilder.toSQL(new GrantRoleStmt(parentRoleNameList, userIdentity)));
                }
                return null;
            } finally {
                roleReadUnlock();
            }
        } catch (PrivilegeException e) {
            throw new SemanticException(e.getMessage());
        } finally {
            userReadUnlock();
        }
    }

    public Map<ObjectType, List<PrivilegeCollection.PrivilegeEntry>> getTypeToPrivilegeEntryListByUser(
            UserIdentity userIdentity) {
        userReadLock();
        try {
            UserPrivilegeCollection userPrivilegeCollection = getUserPrivilegeCollectionUnlocked(userIdentity);
            return userPrivilegeCollection.getTypeToPrivilegeEntryList();
        } catch (PrivilegeException e) {
            throw new SemanticException(e.getMessage());
        } finally {
            userReadUnlock();
        }
    }

    public List<String> getAllRoles() {
        roleReadLock();
        try {
            List<String> roles = new ArrayList<>();
            for (RolePrivilegeCollection rolePrivilegeCollection : roleIdToPrivilegeCollection.values()) {
                roles.add(rolePrivilegeCollection.getName());
            }
            return roles;
        } finally {
            roleReadUnlock();
        }
    }

    public List<PrivilegeType> analyzeActionSet(ObjectType objectType, ActionSet actionSet) {
        List<PrivilegeType> privilegeTypes = provider.getAvailablePrivType(objectType);
        List<PrivilegeType> actions = new ArrayList<>();
        for (PrivilegeType actionName : privilegeTypes) {
            if (actionSet.contains(actionName)) {
                actions.add(actionName);
            }
        }
        return actions;
    }

    public boolean isAvailablePirvType(ObjectType objectType, PrivilegeType privilegeType) {
        return provider.isAvailablePrivType(objectType, privilegeType);
    }

    public List<PrivilegeType> getAvailablePrivType(ObjectType objectType) {
        return provider.getAvailablePrivType(objectType);
    }

    public void createRole(CreateRoleStmt stmt) {
        roleWriteLock();
        try {
            Map<String, Long> roleNameToBeCreated = new HashMap<>();
            Map<Long, RolePrivilegeCollection> rolePrivCollectionModified = new HashMap<>();
            for (String roleName : stmt.getRoles()) {
                if (roleNameToId.containsKey(roleName)) {
                    // Existence verification has been performed in the Analyzer stage. If it exists here,
                    // it may be that other threads have performed the same operation, and return directly here
                    LOG.info("Operation CREATE ROLE failed for " + roleName + " : role " + roleName + " already exists");
                    return;
                }


                long roleId = globalStateMgr.getNextId();
                RolePrivilegeCollection collection = new RolePrivilegeCollection(
                        roleName, RolePrivilegeCollection.RoleFlags.REMOVABLE, RolePrivilegeCollection.RoleFlags.MUTABLE);
                rolePrivCollectionModified.put(roleId, collection);

                roleNameToBeCreated.put(roleName, roleId);
            }

            roleIdToPrivilegeCollection.putAll(rolePrivCollectionModified);
            roleNameToId.putAll(roleNameToBeCreated);

            globalStateMgr.getEditLog().logUpdateRolePrivilege(
                    rolePrivCollectionModified, provider.getPluginId(), provider.getPluginVersion());
            LOG.info("created role {}[{}]", stmt.getRoles().toString(), roleNameToBeCreated.values());
        } finally {
            roleWriteUnlock();
        }
    }

    public void replayUpdateRolePrivilegeCollection(
            RolePrivilegeCollectionInfo info) throws PrivilegeException {
        roleWriteLock();
        try {
            for (Map.Entry<Long, RolePrivilegeCollection> entry : info.getRolePrivCollectionModified().entrySet()) {
                long roleId = entry.getKey();
                invalidateRolesInCacheRoleUnlocked(roleId);
                RolePrivilegeCollection privilegeCollection = entry.getValue();
                provider.upgradePrivilegeCollection(privilegeCollection, info.getPluginId(), info.getPluginVersion());
                roleIdToPrivilegeCollection.put(roleId, privilegeCollection);
                if (!roleNameToId.containsKey(privilegeCollection.getName())) {
                    roleNameToId.put(privilegeCollection.getName(), roleId);
                }
                LOG.info("replayed update role {}", roleId);
            }
        } finally {
            roleWriteUnlock();
        }
    }

    public void dropRole(DropRoleStmt stmt) throws DdlException {
        roleWriteLock();
        try {
            List<String> roleNameToBeDropped = new ArrayList<>();
            Map<Long, RolePrivilegeCollection> rolePrivCollectionModified = new HashMap<>();
            for (String roleName : stmt.getRoles()) {
                if (!roleNameToId.containsKey(roleName)) {
                    // Existence verification has been performed in the Analyzer stage. If it not exists here,
                    // it may be that other threads have performed the same operation, and return directly here
                    LOG.info("Operation DROP ROLE failed for " + roleName + " : role " + roleName + " not exists");

                    return;
                }

                long roleId = getRoleIdByNameNoLock(roleName);
                RolePrivilegeCollection collection = roleIdToPrivilegeCollection.get(roleId);
                if (!collection.isRemovable()) {
                    throw new DdlException("role " + roleName + " cannot be dropped!");
                }

                roleNameToBeDropped.add(roleName);
                rolePrivCollectionModified.put(roleId, collection);
                invalidateRolesInCacheRoleUnlocked(roleId);
            }

            roleIdToPrivilegeCollection.keySet().removeAll(rolePrivCollectionModified.keySet());
            roleNameToBeDropped.forEach(roleNameToId.keySet()::remove);

            globalStateMgr.getEditLog().logDropRole(
                    rolePrivCollectionModified, provider.getPluginId(), provider.getPluginVersion());
            LOG.info("dropped role {}[{}]", stmt.getRoles().toString(), rolePrivCollectionModified.keySet().toString());
        } catch (PrivilegeException e) {
            throw new DdlException("failed to drop role: " + e.getMessage(), e);
        } finally {
            roleWriteUnlock();
        }
    }

    public void replayDropRole(
            RolePrivilegeCollectionInfo info) throws PrivilegeException {
        roleWriteLock();
        try {
            for (Map.Entry<Long, RolePrivilegeCollection> entry : info.getRolePrivCollectionModified().entrySet()) {
                long roleId = entry.getKey();
                invalidateRolesInCacheRoleUnlocked(roleId);
                RolePrivilegeCollection privilegeCollection = entry.getValue();
                // Actually privilege collection is useless here, but we still record it for further usage
                provider.upgradePrivilegeCollection(privilegeCollection, info.getPluginId(), info.getPluginVersion());
                roleIdToPrivilegeCollection.remove(roleId);
                roleNameToId.remove(privilegeCollection.getName());
                LOG.info("replayed drop role {}", roleId);
            }
        } finally {
            roleWriteUnlock();
        }
    }

    public boolean checkRoleExists(String name) {
        roleReadLock();
        try {
            return roleNameToId.containsKey(name);
        } finally {
            roleReadUnlock();
        }
    }

    // used in executing `set role` statement
    public Set<Long> getRoleIdsByUser(UserIdentity user) throws PrivilegeException {
        userReadLock();
        try {
            Set<Long> ret = new HashSet<>();
            roleReadLock();
            try {
                for (long roleId : getUserPrivilegeCollectionUnlocked(user).getAllRoles()) {
                    // role may be removed
                    if (getRolePrivilegeCollectionUnlocked(roleId, false) != null) {
                        ret.add(roleId);
                    }
                }
                return ret;
            } finally {
                roleReadUnlock();
            }
        } finally {
            userReadUnlock();
        }
    }

    public Set<Long> getDefaultRoleIdsByUser(UserIdentity user) throws PrivilegeException {
        userReadLock();
        try {
            Set<Long> ret = new HashSet<>();
            roleReadLock();
            try {
                for (long roleId : getUserPrivilegeCollectionUnlocked(user).getDefaultRoleIds()) {
                    // role may be removed
                    if (getRolePrivilegeCollectionUnlocked(roleId, false) != null) {
                        ret.add(roleId);
                    }
                }
                return ret;
            } finally {
                roleReadUnlock();
            }
        } finally {
            userReadUnlock();
        }
    }

    public void setUserDefaultRole(Set<Long> roleName, UserIdentity user) throws PrivilegeException {
        userWriteLock();
        try {
            UserPrivilegeCollection collection = getUserPrivilegeCollectionUnlocked(user);

            roleReadLock();
            try {
                collection.setDefaultRoleIds(roleName);
            } finally {
                roleReadUnlock();
            }

            globalStateMgr.getEditLog().logUpdateUserPrivilege(
                    user, collection, provider.getPluginId(), provider.getPluginVersion());
            LOG.info("grant role {} to user {}", roleName, user);
        } finally {
            userWriteUnlock();
        }
    }

    public List<String> getRoleNamesByUser(UserIdentity user) throws PrivilegeException {
        try {
            userReadLock();
            List<String> roleNameList = Lists.newArrayList();
            try {
                roleReadLock();
                for (long roleId : getUserPrivilegeCollectionUnlocked(user).getAllRoles()) {
                    RolePrivilegeCollection rolePrivilegeCollection =
                            getRolePrivilegeCollectionUnlocked(roleId, false);
                    // role may be removed
                    if (rolePrivilegeCollection != null) {
                        roleNameList.add(rolePrivilegeCollection.getName());
                    }
                }
                return roleNameList;
            } finally {
                roleReadUnlock();
            }
        } finally {
            userReadUnlock();
        }
    }

    // used in executing `set role` statement
    public Long getRoleIdByNameAllowNull(String name) {
        roleReadLock();
        try {
            return roleNameToId.get(name);
        } finally {
            roleReadUnlock();
        }
    }

    protected Long getRoleIdByNameNoLock(String name) throws PrivilegeException {
        Long roleId = roleNameToId.get(name);
        if (roleId == null) {
            throw new PrivilegeException(String.format("Role %s doesn't exist!", name));
        }
        return roleId;
    }

    public PEntryObject generateObject(ObjectType objectType, List<String> objectTokenList) throws PrivilegeException {
        if (objectTokenList == null) {
            return null;
        }
        return this.provider.generateObject(objectType, objectTokenList, globalStateMgr);
    }

    public PEntryObject generateUserObject(ObjectType objectType, UserIdentity user) throws PrivilegeException {
        return this.provider.generateUserObject(objectType, user, globalStateMgr);
    }

    /**
     * remove invalid object periodically
     * <p>
     * lock order should always be:
     * AuthenticationManager.lock -> PrivilegeManager.userLock -> PrivilegeManager.roleLock
     */
    public void removeInvalidObject() {
        userWriteLock();
        try {
            // 1. remove invalidate object of users
            for (Map.Entry<UserIdentity, UserPrivilegeCollection> userPrivEntry : userToPrivilegeCollection.entrySet()) {
                userPrivEntry.getValue().removeInvalidObject(globalStateMgr);
            }

            // 2. remove invalidate roles of users
            roleReadLock();
            try {
                for (Map.Entry<UserIdentity, UserPrivilegeCollection> userPrivEntry : userToPrivilegeCollection.entrySet()) {
                    removeInvalidRolesUnlocked(userPrivEntry.getValue().getAllRoles());
                    removeInvalidRolesUnlocked(userPrivEntry.getValue().getDefaultRoleIds());
                }
            } finally {
                roleReadUnlock();
            }
        } finally {
            userWriteUnlock();
        }

        // 3. remove invalidate object of roles
        // we have to add user lock first because it may contain user privilege
        userReadLock();
        try {
            roleWriteLock();
            try {
                for (Map.Entry<Long, RolePrivilegeCollection> rolePrivEntry : roleIdToPrivilegeCollection.entrySet()) {
                    rolePrivEntry.getValue().removeInvalidObject(globalStateMgr);
                }
            } finally {
                roleWriteUnlock();
            }
        } finally {
            userReadUnlock();
        }

        // 4. remove invalidate parent roles & sub roles
        roleWriteLock();
        try {
            for (Map.Entry<Long, RolePrivilegeCollection> rolePrivEntry : roleIdToPrivilegeCollection.entrySet()) {
                RolePrivilegeCollection collection = rolePrivEntry.getValue();
                removeInvalidRolesUnlocked(collection.getParentRoleIds());
                removeInvalidRolesUnlocked(collection.getSubRoleIds());
            }
        } finally {
            roleWriteUnlock();
        }
    }

    private void removeInvalidRolesUnlocked(Set<Long> roleIds) {
        roleIds.removeIf(aLong -> !roleIdToPrivilegeCollection.containsKey(aLong));
    }

    /**
     * get max role inheritance depth
     * e.g. grant role_a to role role_b; grant role_b to role role_c;
     * then the inheritance graph would be role_a -> role_b -> role_c
     * the role inheritance depth for role_a would be 2, for role_b would be 1, for role_c would be 0
     */
    protected long getMaxRoleInheritanceDepthInner(long currentDepth, long roleId) throws PrivilegeException {
        RolePrivilegeCollection collection = getRolePrivilegeCollectionUnlocked(roleId, false);
        if (collection == null) {  // this role has been dropped
            return currentDepth - 1;
        }
        Set<Long> subRoleIds = collection.getSubRoleIds();
        if (subRoleIds.isEmpty()) {
            return currentDepth;
        } else {
            long maxDepth = -1;
            for (long subRoleId : subRoleIds) {
                // return the max depth
                maxDepth = Math.max(maxDepth, getMaxRoleInheritanceDepthInner(currentDepth + 1, subRoleId));
            }
            return maxDepth;
        }
    }

    /**
     * get all descendants roles(sub roles and their subs etc.)
     * e.g. grant role_a to role role_b; grant role_b to role role_c;
     * then the inheritance graph would be role_a -> role_b -> role_c
     * then all descendants roles of role_a would be [role_b, role_c]
     */
    protected Set<Long> getAllDescendantsUnlocked(long roleId) throws PrivilegeException {
        Set<Long> set = new HashSet<>();
        set.add(roleId);
        getAllDescendantsUnlockedInner(roleId, set);
        return set;
    }

    protected void getAllDescendantsUnlockedInner(long roleId, Set<Long> resultSet) throws PrivilegeException {
        RolePrivilegeCollection collection = getRolePrivilegeCollectionUnlocked(roleId, false);
        // this role has been dropped, but we still count it as descendants
        if (collection == null) {
            return;
        }
        for (Long subId : collection.getSubRoleIds()) {
            if (!resultSet.contains(subId)) {
                resultSet.add(subId);
                // recursively collect all predecessors
                getAllDescendantsUnlockedInner(subId, resultSet);
            }
        }
    }

    /**
     * get all predecessors roles (parent roles and their parents etc.)
     * e.g. grant role_a to role role_b; grant role_b to role role_c;
     * then the inheritance graph would be role_a -> role_b -> role_c
     * then all parent roles of role_c would be [role_a, role_b]
     */
    protected Set<Long> getAllPredecessorsUnlocked(UserPrivilegeCollection collection) throws PrivilegeException {
        return getAllPredecessorsUnlocked(collection.getAllRoles());
    }

    protected Set<Long> getAllPredecessorsUnlocked(long roleId) throws PrivilegeException {
        Set<Long> set = new HashSet<>();
        set.add(roleId);
        return getAllPredecessorsUnlocked(set);
    }

    protected Set<Long> getAllPredecessorsUnlocked(Set<Long> initialRoleIds) throws PrivilegeException {
        Set<Long> result = new HashSet<>(initialRoleIds);
        for (long roleId : initialRoleIds) {
            getAllPredecessorsInner(roleId, result);
        }
        return result;
    }

    protected void getAllPredecessorsInner(long roleId, Set<Long> resultSet) throws PrivilegeException {
        RolePrivilegeCollection collection = getRolePrivilegeCollectionUnlocked(roleId, false);
        if (collection == null) { // this role has been dropped
            resultSet.remove(roleId);
            return;
        }
        for (Long parentId : collection.getParentRoleIds()) {
            if (!resultSet.contains(parentId)) {
                resultSet.add(parentId);
                // recursively collect all predecessors
                getAllPredecessorsInner(parentId, resultSet);
            }
        }
    }

    /**
     * Use new image format by SRMetaBlockWriter/SRMetaBlockReader
     * +------------------+
     * |     header       |
     * +------------------+
     * |                  |
     * | PrivilegeManager |
     * |                  |
     * +------------------+
     * |      numUser     |
     * +------------------+
     * |      User        |
     * |    Privilege     |
     * |   Collection 1   |
     * +------------------+
     * |      User        |
     * |    Privilege     |
     * |   Collection 2   |
     * +------------------+
     * |       ...        |
     * +------------------+
     * |      numRole     |
     * +------------------+
     * |      Role        |
     * |    Privilege     |
     * |   Collection 1   |
     * +------------------+
     * |      Role        |
     * |    Privilege     |
     * |   Collection 1   |
     * +------------------+
     * |       ...        |
     * +------------------+
     * |      footer      |
     * +------------------+
     */
    public void save(DataOutputStream dos) throws IOException {
        try {
            // 1 json for myself,1 json for number of users, 2 json for each user(kv)
            // 1 json for number of roles, 2 json for each role(kv)
            final int cnt = 1 + 1 + userToPrivilegeCollection.size() * 2
                    + 1 + roleIdToPrivilegeCollection.size() * 2;
            SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, PrivilegeManager.class.getName(), cnt);
            // 1 json for myself
            writer.writeJson(this);
            // 1 json for num user
            writer.writeJson(userToPrivilegeCollection.size());
            Iterator<Map.Entry<UserIdentity, UserPrivilegeCollection>> iterator =
                    userToPrivilegeCollection.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<UserIdentity, UserPrivilegeCollection> entry = iterator.next();
                writer.writeJson(entry.getKey());
                writer.writeJson(entry.getValue());
            }
            // 1 json for num roles
            writer.writeJson(roleIdToPrivilegeCollection.size());
            Iterator<Map.Entry<Long, RolePrivilegeCollection>> roleIter =
                    roleIdToPrivilegeCollection.entrySet().iterator();
            while (roleIter.hasNext()) {
                Map.Entry<Long, RolePrivilegeCollection> entry = roleIter.next();
                writer.writeJson(entry.getKey());
                writer.writeJson(entry.getValue());
            }
            writer.close();
        } catch (SRMetaBlockException e) {
            throw new IOException("failed to save AuthenticationManager!", e);
        }
    }

    public static PrivilegeManager load(
            DataInputStream dis, GlobalStateMgr globalStateMgr, AuthorizationProvider provider)
            throws IOException, DdlException {
        try {
            SRMetaBlockReader reader = new SRMetaBlockReader(dis, PrivilegeManager.class.getName());
            PrivilegeManager ret = null;

            try {
                // 1 json for myself
                ret = (PrivilegeManager) reader.readJson(PrivilegeManager.class);
                ret.globalStateMgr = globalStateMgr;
                if (provider == null) {
                    ret.provider = new DefaultAuthorizationProvider();
                } else {
                    ret.provider = provider;
                }
                ret.initBuiltinRolesAndUsers();
                // 1 json for num user
                int numUser = (int) reader.readJson(int.class);
                LOG.info("loading {} users", numUser);
                for (int i = 0; i != numUser; ++i) {
                    // 2 json for each user(kv)
                    UserIdentity userIdentity = (UserIdentity) reader.readJson(UserIdentity.class);
                    UserPrivilegeCollection collection =
                            (UserPrivilegeCollection) reader.readJson(UserPrivilegeCollection.class);

                    if (userIdentity.equals(UserIdentity.ROOT)) {
                        UserPrivilegeCollection rootUserPrivCollection =
                                ret.getUserPrivilegeCollectionUnlocked(UserIdentity.ROOT);
                        collection.grantRoles(rootUserPrivCollection.getAllRoles());
                        collection.setDefaultRoleIds(rootUserPrivCollection.getDefaultRoleIds());
                        collection.typeToPrivilegeEntryList = rootUserPrivCollection.typeToPrivilegeEntryList;
                    }

                    // upgrade meta to current version
                    ret.provider.upgradePrivilegeCollection(collection, ret.pluginId, ret.pluginVersion);
                    ret.userToPrivilegeCollection.put(userIdentity, collection);
                }
                // 1 json for num roles
                int numRole = (int) reader.readJson(int.class);
                LOG.info("loading {} roles", numRole);
                for (int i = 0; i != numRole; ++i) {
                    // 2 json for each role(kv)
                    Long roleId = (Long) reader.readJson(Long.class);
                    RolePrivilegeCollection collection =
                            (RolePrivilegeCollection) reader.readJson(RolePrivilegeCollection.class);

                    // Use hard-code PrivilegeCollection in the memory as the built-in role permission.
                    // The reason why need to replay from the image here
                    // is because the associated information of the role-id is stored in the image.
                    if (PrivilegeBuiltinConstants.IMMUTABLE_BUILT_IN_ROLE_IDS.contains(roleId)) {
                        RolePrivilegeCollection builtInRolePrivilegeCollection =
                                ret.roleIdToPrivilegeCollection.get(roleId);
                        collection.typeToPrivilegeEntryList = builtInRolePrivilegeCollection.typeToPrivilegeEntryList;
                    }
                    // upgrade meta to current version
                    ret.provider.upgradePrivilegeCollection(collection, ret.pluginId, ret.pluginVersion);
                    ret.roleIdToPrivilegeCollection.put(roleId, collection);
                }
            } catch (SRMetaBlockEOFException eofException) {
                LOG.warn("got EOF exception, ignore, ", eofException);
            } finally {
                reader.close();
            }

            assert ret != null; // can't be NULL
            LOG.info("loaded {} users, {} roles",
                    ret.userToPrivilegeCollection.size(), ret.roleIdToPrivilegeCollection.size());
            // mark data is loaded
            ret.isLoaded = true;
            return ret;
        } catch (SRMetaBlockException | PrivilegeException e) {
            throw new DdlException("failed to load PrivilegeManager!", e);
        }
    }

    public boolean isLoaded() {
        return isLoaded;
    }

    public void setLoaded(boolean loaded) {
        isLoaded = loaded;
    }

    /**
     * these public interfaces are for AuthUpgrader to upgrade from 2.x
     */
    public void upgradeUserInitPrivilegeUnlock(UserIdentity userIdentity, UserPrivilegeCollection collection) {
        userToPrivilegeCollection.put(userIdentity, collection);
        LOG.info("upgrade user {}", userIdentity);
    }

    public void upgradeRoleInitPrivilegeUnlock(long roleId, RolePrivilegeCollection collection) {
        roleIdToPrivilegeCollection.put(roleId, collection);
        roleNameToId.put(collection.getName(), roleId);
        LOG.info("upgrade role {}[{}]", collection.getName(), roleId);
    }
}
