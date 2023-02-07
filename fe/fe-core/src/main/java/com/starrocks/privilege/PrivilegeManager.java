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
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.UserIdentity;
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
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.sql.ast.RevokeRoleStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private static final String ALL_ACTIONS = "ALL";
    // builtin roles and users
    private static final String ROOT_ROLE_NAME = "root";
    public static final long ROOT_ROLE_ID = -1;
    public static final long DB_ADMIN_ROLE_ID = -2;
    public static final long CLUSTER_ADMIN_ROLE_ID = -3;
    public static final long USER_ADMIN_ROLE_ID = -4;
    public static final long PUBLIC_ROLE_ID = -5;

    public static final Set<String> BUILT_IN_ROLE_NAMES =
            new HashSet<>(Arrays.asList("root", "db_admin", "user_admin", "cluster_admin", "public"));

    public static final Set<Long> IMMUTABLE_BUILT_IN_ROLE_IDS = new HashSet<>(Arrays.asList(
            ROOT_ROLE_ID, DB_ADMIN_ROLE_ID, CLUSTER_ADMIN_ROLE_ID, USER_ADMIN_ROLE_ID));

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
                        public PrivilegeCollection load(Pair<UserIdentity, Set<Long>> userIdentitySetPair)
                                throws Exception {
                            return loadPrivilegeCollection(userIdentitySetPair.first, userIdentitySetPair.second);
                        }
                    });

    private final ReentrantReadWriteLock userLock;

    private String publicRoleName = null; // ut may be null

    // set by load() to distinguish brand-new environment with upgraded environment
    private boolean isLoaded = false;

    // only when deserialized
    protected PrivilegeManager() {
        roleNameToId = new HashMap<>();
        userToPrivilegeCollection = new HashMap<>();
        roleIdToPrivilegeCollection = new HashMap<>();
        userLock = new ReentrantReadWriteLock();
        roleLock = new ReentrantReadWriteLock();
    }

    protected Map<Long, RolePrivilegeCollection> roleIdToPrivilegeCollection;
    private final ReentrantReadWriteLock roleLock;

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
            RolePrivilegeCollection rolePrivilegeCollection = initBuiltinRoleUnlocked(ROOT_ROLE_ID, ROOT_ROLE_NAME);
            // GRANT ALL ON ALL
            for (ObjectType objectType : provider.getAllPrivObjectTypes()) {
                initPrivilegeCollectionAllObjects(rolePrivilegeCollection, objectType,
                        new ArrayList<>(objectType.getActionMap().keySet()));
            }
            rolePrivilegeCollection.disableMutable();  // not mutable

            // 2. builtin db_admin role
            rolePrivilegeCollection = initBuiltinRoleUnlocked(DB_ADMIN_ROLE_ID, "db_admin");
            ObjectType systemTypes = ObjectType.SYSTEM;
            // ALL system but GRANT AND NODE
            List<String> actionWithoutNodeGrant = systemTypes.getActionMap().keySet().stream().filter(
                    x -> !x.equals("GRANT") && !x.equals("NODE")).collect(Collectors.toList());
            initPrivilegeCollections(rolePrivilegeCollection, systemTypes.name(), actionWithoutNodeGrant, null,
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
                initPrivilegeCollectionAllObjects(rolePrivilegeCollection, t,
                        new ArrayList<>(t.getActionMap().keySet()));
            }
            rolePrivilegeCollection.disableMutable(); // not mutable

            // 3. cluster_admin
            rolePrivilegeCollection = initBuiltinRoleUnlocked(CLUSTER_ADMIN_ROLE_ID, "cluster_admin");
            // GRANT NODE ON SYSTEM
            initPrivilegeCollections(
                    rolePrivilegeCollection,
                    systemTypes.name(),
                    Collections.singletonList(PrivilegeType.NODE.name()),
                    null,
                    false);
            rolePrivilegeCollection.disableMutable(); // not mutable

            // 4. user_admin
            rolePrivilegeCollection = initBuiltinRoleUnlocked(USER_ADMIN_ROLE_ID, "user_admin");
            // GRANT GRANT ON SYSTEM
            initPrivilegeCollections(
                    rolePrivilegeCollection,
                    systemTypes.name(),
                    Collections.singletonList(PrivilegeType.GRANT.name()),
                    null,
                    false);
            ObjectType t = ObjectType.USER;
            initPrivilegeCollectionAllObjects(rolePrivilegeCollection, t,
                    new ArrayList<>(t.getActionMap().keySet()));
            rolePrivilegeCollection.disableMutable(); // not mutable

            // 5. public
            publicRoleName = "public";
            rolePrivilegeCollection = initBuiltinRoleUnlocked(PUBLIC_ROLE_ID, publicRoleName);
            // GRANT SELECT ON ALL TABLES IN information_schema
            List<PEntryObject> object = Collections.singletonList(new TablePEntryObject(
                    SystemId.INFORMATION_SCHEMA_DB_ID, TablePEntryObject.ALL_TABLES_ID));
            short tableTypeId = (short) ObjectType.TABLE.getId();
            ActionSet selectAction =
                    analyzeActionSet(tableTypeId, Collections.singletonList(PrivilegeType.SELECT.name()));
            rolePrivilegeCollection.grant(tableTypeId, selectAction, object, false);

            // 6. builtin user root
            UserPrivilegeCollection rootCollection = new UserPrivilegeCollection();
            RolePrivilegeCollection rootRolePrivCollection =
                    getRolePrivilegeCollectionUnlocked(getRoleIdByNameNoLock(ROOT_ROLE_NAME), true);
            rootCollection.merge(rootRolePrivCollection);
            userToPrivilegeCollection.put(UserIdentity.createAnalyzedUserIdentWithIp("root", "%"), rootCollection);
        } catch (PrivilegeException e) {
            // all initial privileges are supposed to be legal
            throw new RuntimeException("Fatal error when initializing built-in role and user", e);
        }
    }

    // called by initBuiltinRolesAndUsers()
    private void initPrivilegeCollections(
            PrivilegeCollection collection, String type, List<String> actionList, List<String> tokens, boolean isGrant)
            throws PrivilegeException {
        short typeId = analyzeType(type);
        ActionSet actionSet = analyzeActionSet(typeId, actionList);
        List<PEntryObject> object = null;
        if (tokens != null) {
            object = Arrays.asList(provider.generateObject(type, tokens, globalStateMgr));
        }
        collection.grant(typeId, actionSet, object, isGrant);
    }

    // called by initBuiltinRolesAndUsers()
    private void initPrivilegeCollectionAllObjects(
            PrivilegeCollection collection, ObjectType type, List<String> actionList) throws PrivilegeException {
        short typeId = analyzeType(type.name());
        ActionSet actionSet = analyzeActionSet(typeId, actionList);
        List<PEntryObject> objects = new ArrayList<>();
        switch (type) {
            case TABLE:
            case VIEW:
            case MATERIALIZED_VIEW:
            case FUNCTION:
                objects.add(provider.generateObject(
                        type.name(),
                        Lists.newArrayList("*", "*"),
                        globalStateMgr));
                collection.grant(typeId, actionSet, objects, false);
                break;

            case USER:
                objects.add(provider.generateUserObject(type.name(), null, globalStateMgr));
                collection.grant(typeId, actionSet, objects, false);
                break;

            case DATABASE:
            case RESOURCE:
            case CATALOG:
            case RESOURCE_GROUP:
            case GLOBAL_FUNCTION:
                objects.add(provider.generateObject(
                        type.name(),
                        Lists.newArrayList("*"),
                        globalStateMgr));
                collection.grant(typeId, actionSet, objects, false);
                break;

            case SYSTEM:
                collection.grant(typeId, actionSet, null, false);
                break;

            default:
                throw new PrivilegeException("unsupported type " + type);
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
                        stmt.getTypeId(),
                        stmt.getActionList(),
                        stmt.getObjectList(),
                        stmt.isWithGrantOption(),
                        stmt.getRole());
            } else {
                grantToUser(
                        stmt.getTypeId(),
                        stmt.getActionList(),
                        stmt.getObjectList(),
                        stmt.isWithGrantOption(),
                        stmt.getUserIdentity());
            }
        } catch (PrivilegeException e) {
            throw new DdlException("failed to grant: " + e.getMessage(), e);
        }
    }

    protected void grantToUser(
            short type,
            ActionSet actionSet,
            List<PEntryObject> objects,
            boolean isGrant,
            UserIdentity userIdentity) throws PrivilegeException {
        userWriteLock();
        try {
            UserPrivilegeCollection collection = getUserPrivilegeCollectionUnlocked(userIdentity);
            collection.grant(type, actionSet, objects, isGrant);
            globalStateMgr.getEditLog().logUpdateUserPrivilege(
                    userIdentity, collection, provider.getPluginId(), provider.getPluginVersion());
            invalidateUserInCache(userIdentity);
        } finally {
            userWriteUnlock();
        }
    }

    protected void grantToRole(
            short type,
            ActionSet actionSet,
            List<PEntryObject> objects,
            boolean isGrant,
            String roleName) throws PrivilegeException {
        roleWriteLock();
        try {
            long roleId = getRoleIdByNameNoLock(roleName);
            invalidateRolesInCacheRoleUnlocked(roleId);
            RolePrivilegeCollection collection = getRolePrivilegeCollectionUnlocked(roleId, true);
            collection.grant(type, actionSet, objects, isGrant);
            globalStateMgr.getEditLog().logUpdateRolePrivilege(
                    roleId, collection, provider.getPluginId(), provider.getPluginVersion());
        } finally {
            roleWriteUnlock();
        }
    }

    public void revoke(RevokePrivilegeStmt stmt) throws DdlException {
        try {
            if (stmt.getRole() != null) {
                revokeFromRole(
                        stmt.getTypeId(),
                        stmt.getActionList(),
                        stmt.getObjectList(),
                        stmt.isWithGrantOption(),
                        stmt.getRole());
            } else {
                revokeFromUser(
                        stmt.getTypeId(),
                        stmt.getActionList(),
                        stmt.getObjectList(),
                        stmt.isWithGrantOption(),
                        stmt.getUserIdentity());
            }
        } catch (PrivilegeException e) {
            throw new DdlException("failed to revoke: " + e.getMessage(), e);
        }
    }

    protected void revokeFromUser(
            short type,
            ActionSet actionSet,
            List<PEntryObject> objects,
            boolean isGrant,
            UserIdentity userIdentity) throws PrivilegeException {
        userWriteLock();
        try {
            UserPrivilegeCollection collection = getUserPrivilegeCollectionUnlocked(userIdentity);
            collection.revoke(type, actionSet, objects, isGrant);
            globalStateMgr.getEditLog().logUpdateUserPrivilege(
                    userIdentity, collection, provider.getPluginId(), provider.getPluginVersion());
            invalidateUserInCache(userIdentity);
        } finally {
            userWriteUnlock();
        }
    }

    protected void revokeFromRole(
            short type,
            ActionSet actionSet,
            List<PEntryObject> objects,
            boolean isGrant,
            String roleName) throws PrivilegeException {
        roleWriteLock();
        try {
            long roleId = getRoleIdByNameNoLock(roleName);
            invalidateRolesInCacheRoleUnlocked(roleId);
            RolePrivilegeCollection collection = getRolePrivilegeCollectionUnlocked(roleId, true);
            collection.revoke(type, actionSet, objects, isGrant);
            globalStateMgr.getEditLog().logUpdateRolePrivilege(
                    roleId, collection, provider.getPluginId(), provider.getPluginVersion());
        } finally {
            roleWriteUnlock();
        }
    }

    public void grantRole(GrantRoleStmt stmt) throws DdlException {
        try {
            if (stmt.getUserIdent() != null) {
                grantRoleToUser(stmt.getGranteeRole(), stmt.getUserIdent());
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

            // write journal to update privilege collections of both role & parent role
            RolePrivilegeCollectionInfo info = new RolePrivilegeCollectionInfo(
                    roleId, collection, provider.getPluginId(), provider.getPluginVersion());

            for (String parentRole : parentRoleName) {
                long parentRoleId = getRoleIdByNameNoLock(parentRole);
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
                info.add(parentRoleId, parentCollection);
            }

            invalidateRolesInCacheRoleUnlocked(roleId);
            globalStateMgr.getEditLog().logUpdateRolePrivilege(info);
            LOG.info("grant role {}[{}] to role {}[{}]", parentRoleName,
                    Joiner.on(", ").join(parentRoleName), roleName, roleId);
        } finally {
            roleWriteUnlock();
        }
    }

    public void revokeRole(RevokeRoleStmt stmt) throws DdlException {
        try {
            if (stmt.getUserIdent() != null) {
                revokeRoleFromUser(stmt.getGranteeRole(), stmt.getUserIdent());
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
                    if (roleId == PUBLIC_ROLE_ID) {
                        throw new PrivilegeException("role public cannot be dropped!");
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
            invalidateRolesInCacheRoleUnlocked(roleId);

            for (String parentRoleName : parentRoleNameList) {
                long parentRoleId = getRoleIdByNameNoLock(parentRoleName);
                RolePrivilegeCollection parentCollection =
                        getRolePrivilegeCollectionUnlocked(parentRoleId, true);
                parentCollection.removeSubRole(roleId);
                collection.removeParentRole(parentRoleId);
            }
            // write journal to update privilege collections of both role & parent role
            RolePrivilegeCollectionInfo info = new RolePrivilegeCollectionInfo(
                    roleId, collection, provider.getPluginId(), provider.getPluginVersion());

            List<Long> parentRoleIdList = new ArrayList<>();
            for (String parentRoleName : parentRoleNameList) {
                long parentRoleId = getRoleIdByNameNoLock(parentRoleName);
                RolePrivilegeCollection parentCollection =
                        getRolePrivilegeCollectionUnlocked(parentRoleId, true);
                info.add(parentRoleId, parentCollection);
                parentRoleIdList.add(parentRoleId);
            }
            globalStateMgr.getEditLog().logUpdateRolePrivilege(info);
            LOG.info("revoke role {}[{}] from role {}[{}]",
                    parentRoleNameList.toString(), parentRoleIdList.toString(), roleName, roleId);
        } finally {
            roleWriteUnlock();
        }
    }

    public void validateGrant(String type, List<String> actions, List<PEntryObject> objects) throws PrivilegeException {
        provider.validateGrant(type, actions, objects);
    }

    private static ConnectContext createTmpContext(UserIdentity currentUser) {
        ConnectContext tmpContext = new ConnectContext();
        tmpContext.setCurrentUserIdentity(currentUser);
        tmpContext.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        return tmpContext;
    }

    public static boolean checkSystemAction(
            ConnectContext context, PrivilegeType action) {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context);
            return manager.checkSystemAction(collection, action);
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on system", action, e);
            return false;
        }
    }

    public static boolean checkSystemAction(
            UserIdentity currentUser, PrivilegeType action) {
        return checkSystemAction(createTmpContext(currentUser), action);
    }

    public static boolean checkTableAction(
            ConnectContext context, String db, String table, PrivilegeType action) {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context);
            return manager.checkTableAction(collection, db, table, action);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on table {}.{}, message: {}",
                    action, db, table, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on table {}.{}", action, db, table, e);
            return false;
        }
    }

    public static boolean checkTableAction(
            UserIdentity currentUser, String db, String table, PrivilegeType privilegeType) {
        return checkTableAction(createTmpContext(currentUser), db, table, privilegeType);
    }

    public static boolean checkDbAction(ConnectContext context, String db, PrivilegeType action) {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context);
            return manager.checkDbAction(collection, db, action);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on database {}, message: {}",
                    action, db, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on db {}", action, db, e);
            return false;
        }
    }

    public static boolean checkResourceAction(ConnectContext context, String name,
                                              PrivilegeType action) {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context);
            return manager.checkResourceAction(collection, name, action);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on resource {}, message: {}",
                    action, name, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on resource {}", action, name, e);
            return false;
        }
    }

    public static boolean checkAnyActionOnResource(ConnectContext context, String name) {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context);
            // 1. check for any action on resource
            PEntryObject resourceObject = manager.provider.generateObject(
                    ObjectType.RESOURCE.name(), Arrays.asList(name), manager.globalStateMgr);
            short resourceTypeId = manager.analyzeType(ObjectType.RESOURCE.name());
            return manager.provider.searchAnyActionOnObject(resourceTypeId, resourceObject, collection);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking any action on resource {}, message: {}",
                    name, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking any action on resource {}", name, e);
            return false;
        }
    }

    public static boolean checkResourceGroupAction(ConnectContext context, String name,
                                                   PrivilegeType action) {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context);
            return manager.checkResourceGroupAction(collection, name, action);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on resource group {}, message: {}",
                    action, name, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on resource group {}", action, name, e);
            return false;
        }
    }

    public static boolean checkGlobalFunctionAction(ConnectContext context, String name,
                                                    PrivilegeType action) {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context);
            return manager.checkGlobalFunctionAction(collection, name, action);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on global function {}, message: {}",
                    action, name, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on global function {}", action, name, e);
            return false;
        }
    }

    public static boolean checkCatalogAction(ConnectContext context, String name,
                                             PrivilegeType action) {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context);
            return manager.checkCatalogAction(collection, name, action);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on catalog {}, message: {}",
                    action, name, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on catalog {}", action, name, e);
            return false;
        }
    }

    public static boolean checkAnyActionOnCatalog(ConnectContext context, String catalogName) {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context);
            // 1. check for any action on catalog
            PEntryObject catalogObject = manager.provider.generateObject(
                    ObjectType.CATALOG.name(), Arrays.asList(catalogName), manager.globalStateMgr);
            short catalogTypeId = manager.analyzeType(ObjectType.CATALOG.name());
            return manager.provider.searchAnyActionOnObject(catalogTypeId, catalogObject, collection);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking any action on catalog {}, message: {}",
                    catalogName, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking any action on catalog {}", catalogName, e);
            return false;
        }
    }

    public static boolean checkViewAction(
            ConnectContext context, String db, String view, PrivilegeType action) {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context);
            return manager.checkViewAction(collection, db, view, action);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on view {}.{}, message: {}",
                    action, db, view, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on view {}.{}", action, db, view, e);
            return false;
        }
    }

    public static boolean checkMaterializedViewAction(
            ConnectContext context, String db, String materializedView,
            PrivilegeType action) {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context);
            return manager.checkMaterializedViewAction(collection, db, materializedView, action);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on materialized view {}.{}, message: {}",
                    action, db, materializedView, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on materialized view {}.{}",
                    action, db, materializedView, e);
            return false;
        }
    }

    public static boolean checkFunctionAction(
            ConnectContext context, String db, String functionSig,
            PrivilegeType action) {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context);
            return manager.checkFunctionAction(collection, db, functionSig, action);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action[{}] on function {}.{}, message: {}",
                    action, db, functionSig, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action[{}] on function {}.{}",
                    action, db, functionSig, e);
            return false;
        }
    }

    public static boolean checkAnyActionOnMaterializedView(
            ConnectContext context, String db, String materializedView) {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context);
            PEntryObject materializedViewObject = manager.provider.generateObject(
                    ObjectType.MATERIALIZED_VIEW.name(), Arrays.asList(db, materializedView),
                    manager.globalStateMgr);
            short mvTypeId = manager.analyzeType(ObjectType.MATERIALIZED_VIEW.name());
            return manager.provider.searchAnyActionOnObject(mvTypeId, materializedViewObject, collection);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking any action on materialized view {}.{}, message: {}",
                    db, materializedView, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking any action on materialized view {}.{}",
                    db, materializedView, e);
            return false;
        }
    }

    public static boolean checkAnyActionOnMaterializedView(
            UserIdentity currentUser, String db, String materializedView) {
        return checkAnyActionOnMaterializedView(createTmpContext(currentUser), db, materializedView);
    }

    public static boolean checkAnyActionOnView(
            ConnectContext context, String db, String view) {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context);
            PEntryObject viewObject = manager.provider.generateObject(
                    ObjectType.VIEW.name(), Arrays.asList(db, view), manager.globalStateMgr);
            short viewId = manager.analyzeType(ObjectType.VIEW.name());
            return manager.provider.searchAnyActionOnObject(viewId, viewObject, collection);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking any action on view {}.{}, message: {}",
                    db, view, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking any action on view {}.{}", db, view, e);
            return false;
        }
    }

    public static boolean checkAnyActionOnView(
            UserIdentity currentUser, String db, String view) {
        return checkAnyActionOnView(createTmpContext(currentUser), db, view);
    }

    /**
     * show databases; use database
     */
    public static boolean checkAnyActionOnDb(ConnectContext context, String db) {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context);
            // 1. check for any action on db
            PEntryObject dbObject = manager.provider.generateObject(
                    ObjectType.DATABASE.name(), Arrays.asList(db), manager.globalStateMgr);
            short dbTypeId = manager.analyzeType(ObjectType.DATABASE.name());
            return manager.provider.searchAnyActionOnObject(dbTypeId, dbObject, collection);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking any action on database {}, message: {}",
                    db, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking any action on db {}", db, e);
            return false;
        }
    }

    /**
     * Check whether current user has any privilege action on the db or objects(table/view/mv) in the db.
     * Currently, it's used by `show databases` or `use database`.
     */
    public static boolean checkAnyActionOnOrInDb(ConnectContext context, String db) {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        try {
            // 1. check for any action on db
            if (checkAnyActionOnDb(context, db)) {
                return true;
            }
            // 2. check for any action on any table in this db
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context);
            PEntryObject allTableInDbObject = manager.provider.generateObject(
                    ObjectType.TABLE.name(),
                    Lists.newArrayList(db, "*"),
                    manager.globalStateMgr);
            short tableTypeId = manager.analyzeType(ObjectType.TABLE.name());
            if (manager.provider.searchAnyActionOnObject(tableTypeId, allTableInDbObject, collection)) {
                return true;
            }
            // 3. check for any action on any view in this db
            PEntryObject allViewInDbObject = manager.provider.generateObject(
                    ObjectType.VIEW.name(),
                    Lists.newArrayList(db, "*"),
                    manager.globalStateMgr);
            short viewTypeId = manager.analyzeType(ObjectType.VIEW.name());
            if (manager.provider.searchAnyActionOnObject(viewTypeId, allViewInDbObject, collection)) {
                return true;
            }
            // 4. check for any action on any mv in this db
            PEntryObject allMvInDbObject = manager.provider.generateObject(
                    ObjectType.MATERIALIZED_VIEW.name(),
                    Lists.newArrayList(db, "*"),
                    manager.globalStateMgr);
            short mvTypeId = manager.analyzeType(ObjectType.MATERIALIZED_VIEW.name());
            return manager.provider.searchAnyActionOnObject(mvTypeId, allMvInDbObject, collection);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking any action on or in database {}, message: {}",
                    db, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking any action on or in db {}", db, e);
            return false;
        }
    }

    public static boolean checkAnyActionOnOrInDb(UserIdentity currentUser, String db) {
        return checkAnyActionOnOrInDb(createTmpContext(currentUser), db);
    }

    /**
     * Check whether current user has specified privilege action on any object(table/view/mv) in the db.
     */
    public static boolean checkActionInDb(ConnectContext context, String db, String actionName) {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context);
            // 1. check for specified action on any table in this db

            if (ObjectType.TABLE.isAvailablePrivType(actionName)) {
                PEntryObject allTableInDbObject = manager.provider.generateObject(
                        ObjectType.TABLE.name(),
                        Lists.newArrayList(db, "*"),
                        manager.globalStateMgr);
                short tableTypeId = manager.analyzeType(ObjectType.TABLE.name());
                Action want = manager.provider.getAction(tableTypeId, actionName);
                if (manager.provider.searchActionOnObject(tableTypeId, allTableInDbObject, collection, want)) {
                    return true;
                }
            }

            // 2. check for specified action on any view in this db
            if (ObjectType.VIEW.isAvailablePrivType(actionName)) {
                PEntryObject allViewInDbObject = manager.provider.generateObject(
                        ObjectType.VIEW.name(),
                        Lists.newArrayList(db, "*"),
                        manager.globalStateMgr);
                short viewTypeId = manager.analyzeType(ObjectType.VIEW.name());
                Action want = manager.provider.getAction(viewTypeId, actionName);
                if (manager.provider.searchActionOnObject(viewTypeId, allViewInDbObject, collection, want)) {
                    return true;
                }
            }

            // 3. check for specified action on any mv in this db
            if (ObjectType.MATERIALIZED_VIEW.isAvailablePrivType(actionName)) {
                PEntryObject allMvInDbObject = manager.provider.generateObject(
                        ObjectType.MATERIALIZED_VIEW.name(),
                        Lists.newArrayList(db, "*"),
                        manager.globalStateMgr);
                short mvTypeId = manager.analyzeType(ObjectType.MATERIALIZED_VIEW.name());
                Action want = manager.provider.getAction(mvTypeId, actionName);
                if (manager.provider.searchActionOnObject(mvTypeId, allMvInDbObject, collection, want)) {
                    return true;
                }
            }
            return false;
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking action {} in database {}, message: {}",
                    actionName, db, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking action {} in db {}", actionName, db, e);
            return false;
        }
    }

    public static boolean checkAnyActionOnTable(ConnectContext context, String db, String table) {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context);
            PEntryObject tableObject = manager.provider.generateObject(
                    ObjectType.TABLE.name(), Arrays.asList(db, table), manager.globalStateMgr);
            short tableTypeId = manager.analyzeType(ObjectType.TABLE.name());
            return manager.provider.searchAnyActionOnObject(tableTypeId, tableObject, collection);
        } catch (PrivObjNotFoundException e) {
            LOG.info("Object not found when checking any action on table {}.{}, message: {}",
                    db, table, e.getMessage());
            return true;
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checking any action on table {}.{}", db, table, e);
            return false;
        }
    }

    public static boolean checkAnyActionOnTable(UserIdentity currentUser, String db, String table) {
        return checkAnyActionOnTable(createTmpContext(currentUser), db, table);
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
            PrivilegeCollection collection, ObjectType objectType, String actionName, List<String> objectNames)
            throws PrivilegeException {
        short objectTypeId = analyzeType(objectType.name());
        Action want = provider.getAction(objectTypeId, actionName);
        if (objectNames == null) {
            return provider.check(objectTypeId, want, null, collection);
        } else {
            PEntryObject object = provider.generateObject(
                    objectType.name(), objectNames, globalStateMgr);
            return provider.check(objectTypeId, want, object, collection);
        }
    }

    protected boolean checkSystemAction(PrivilegeCollection collection, PrivilegeType action)
            throws PrivilegeException {
        return checkAction(collection, ObjectType.SYSTEM, action.name(), null);
    }

    protected boolean checkTableAction(
            PrivilegeCollection collection, String db, String table, PrivilegeType privilegeType)
            throws PrivilegeException {
        return checkAction(collection, ObjectType.TABLE, privilegeType.name(), Arrays.asList(db, table));
    }

    protected boolean checkDbAction(PrivilegeCollection collection, String db, PrivilegeType action)
            throws PrivilegeException {
        return checkAction(collection, ObjectType.DATABASE, action.name(), Arrays.asList(db));
    }

    protected boolean checkResourceAction(PrivilegeCollection collection, String name,
                                          PrivilegeType action)
            throws PrivilegeException {
        return checkAction(collection, ObjectType.RESOURCE, action.name(), Arrays.asList(name));
    }

    protected boolean checkResourceGroupAction(PrivilegeCollection collection, String name,
                                               PrivilegeType action)
            throws PrivilegeException {
        return checkAction(collection, ObjectType.RESOURCE_GROUP, action.name(), Arrays.asList(name));
    }

    protected boolean checkGlobalFunctionAction(PrivilegeCollection collection, String name,
                                                PrivilegeType action)
            throws PrivilegeException {
        return checkAction(collection, ObjectType.GLOBAL_FUNCTION, action.name(), Arrays.asList(name));
    }

    protected boolean checkCatalogAction(PrivilegeCollection collection, String name,
                                         PrivilegeType action)
            throws PrivilegeException {
        return checkAction(collection, ObjectType.CATALOG, action.name(), Arrays.asList(name));
    }

    protected boolean checkViewAction(
            PrivilegeCollection collection, String db, String view, PrivilegeType action)
            throws PrivilegeException {
        return checkAction(collection, ObjectType.VIEW, action.name(), Arrays.asList(db, view));
    }

    protected boolean checkMaterializedViewAction(
            PrivilegeCollection collection, String db, String materializeView,
            PrivilegeType action)
            throws PrivilegeException {
        return checkAction(collection, ObjectType.MATERIALIZED_VIEW, action.name(),
                Arrays.asList(db, materializeView));
    }

    protected boolean checkFunctionAction(
            PrivilegeCollection collection, String db, String functionSig,
            PrivilegeType action)
            throws PrivilegeException {
        return checkAction(collection, ObjectType.FUNCTION, action.name(),
                Arrays.asList(db, functionSig));
    }

    public boolean canExecuteAs(ConnectContext context, UserIdentity impersonateUser) {
        try {
            PrivilegeCollection collection = mergePrivilegeCollection(context);
            String typeStr = ObjectType.USER.toString();
            short typeId = analyzeType(typeStr);
            PEntryObject object = provider.generateUserObject(typeStr, impersonateUser, globalStateMgr);
            Action want = provider.getAction(typeId, PrivilegeType.IMPERSONATE.toString());
            return provider.check(typeId, want, object, collection);
        } catch (PrivilegeException e) {
            LOG.warn("caught exception in canExecuteAs() user[{}]", impersonateUser, e);
            return false;
        }
    }

    public boolean allowGrant(ConnectContext context, short type, ActionSet wants, List<PEntryObject> objects) {
        try {
            PrivilegeCollection collection = mergePrivilegeCollection(context);
            // check for GRANT or WITH GRANT OPTION in the specific type
            return checkSystemAction(collection, PrivilegeType.GRANT)
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
    public UserPrivilegeCollection onCreateUser(UserIdentity user) throws PrivilegeException {
        userWriteLock();
        try {
            UserPrivilegeCollection privilegeCollection = new UserPrivilegeCollection();
            if (publicRoleName != null) {
                // grant public role
                privilegeCollection.grantRole(getRoleIdByNameNoLock(publicRoleName));
            }
            userToPrivilegeCollection.put(user, privilegeCollection);
            LOG.info("user privilege for {} is created, role {} is granted", user, publicRoleName);
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
    protected PrivilegeCollection mergePrivilegeCollection(ConnectContext context) throws PrivilegeException {
        try {
            return ctxToMergedPrivilegeCollections.get(
                    new Pair<>(context.getCurrentUserIdentity(), context.getCurrentRoleIds()));
        } catch (ExecutionException e) {
            String errMsg = String.format(
                    "failed merge privilege collection on %s with roles %s %s",
                    context.getCurrentUserIdentity(),
                    context.getCurrentRoleIds(),
                    context);
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
            UserPrivilegeCollection userCollection = getUserPrivilegeCollectionUnlocked(userIdentity);
            collection.merge(userCollection);
            roleReadLock();
            try {
                // 1. get all parent roles by default, but can be specified with `SET ROLE` statement
                if (roleIds == null) {
                    roleIds = userCollection.getAllRoles();
                }

                // 2. get all predecessors base on step 1
                roleIds = getAllPredecessorsUnlocked(roleIds);

                // 3. merge privilege collections of all predecessors
                for (long roleId : roleIds) {
                    RolePrivilegeCollection rolePrivilegeCollection = getRolePrivilegeCollectionUnlocked(roleId, false);
                    if (rolePrivilegeCollection != null) {
                        collection.merge(rolePrivilegeCollection);
                    }
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

    public Map<UserIdentity, UserPrivilegeCollection> getUserToPrivilegeCollection() {
        return userToPrivilegeCollection;
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

    public Map<Long, RolePrivilegeCollection> getRoleIdToPrivilegeCollection() {
        return roleIdToPrivilegeCollection;
    }

    public ActionSet analyzeActionSet(short typeId, List<String> actionNameList) throws PrivilegeException {
        List<Action> actions = new ArrayList<>();
        for (String actionName : actionNameList) {
            // grant ALL on xx
            if (actionName.equals(ALL_ACTIONS)) {
                return new ActionSet(provider.getAllActions(typeId));
            }
            // in consideration of legacy format such as SELECT_PRIV
            if (actionName.endsWith("_PRIV")) {
                actionName = actionName.substring(0, actionName.length() - 5);
            }
            Action action = provider.getAction(typeId, actionName);
            actions.add(action);
        }
        return new ActionSet(actions);
    }

    public String analyzeTypeInPlural(String plural) throws PrivilegeException {
        return provider.getTypeNameByPlural(plural);
    }

    public short analyzeType(String typeName) throws PrivilegeException {
        return (short) ObjectType.valueOf(typeName).getId();
    }

    public ObjectType getObjectType(short typeId) throws PrivilegeException {
        return provider.getObjectType(typeId);
    }

    public void createRole(CreateRoleStmt stmt) throws DdlException {
        roleWriteLock();
        try {
            String roleName = stmt.getQualifiedRole();
            if (roleNameToId.containsKey(roleName)) {
                throw new DdlException(String.format("Role %s already exists!", roleName));
            }
            RolePrivilegeCollection collection = new RolePrivilegeCollection(
                    roleName, RolePrivilegeCollection.RoleFlags.REMOVABLE, RolePrivilegeCollection.RoleFlags.MUTABLE);
            long roleId = globalStateMgr.getNextId();
            roleIdToPrivilegeCollection.put(roleId, collection);
            roleNameToId.put(roleName, roleId);
            globalStateMgr.getEditLog().logUpdateRolePrivilege(
                    roleId, collection, provider.getPluginId(), provider.getPluginVersion());
            LOG.info("created role {}[{}]", roleName, roleId);
        } finally {
            roleWriteUnlock();
        }
    }

    public void replayUpdateRolePrivilegeCollection(
            RolePrivilegeCollectionInfo info) throws PrivilegeException {
        roleWriteLock();
        try {
            for (Map.Entry<Long, RolePrivilegeCollection> entry : info.getRolePrivilegeCollectionMap().entrySet()) {
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
            String roleName = stmt.getQualifiedRole();
            long roleId = getRoleIdByNameNoLock(roleName);
            invalidateRolesInCacheRoleUnlocked(roleId);
            RolePrivilegeCollection collection = roleIdToPrivilegeCollection.get(roleId);
            if (!collection.isRemovable()) {
                throw new DdlException("role " + roleName + " cannot be dropped!");
            }
            roleIdToPrivilegeCollection.remove(roleId);
            roleNameToId.remove(roleName);
            globalStateMgr.getEditLog()
                    .logDropRole(roleId, collection, provider.getPluginId(), provider.getPluginVersion());
            LOG.info("dropped role {}[{}]", roleName, roleId);
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
            for (Map.Entry<Long, RolePrivilegeCollection> entry : info.getRolePrivilegeCollectionMap().entrySet()) {
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

    public PEntryObject analyzeObject(String typeName, List<String> objectTokenList) throws PrivilegeException {
        if (objectTokenList == null) {
            return null;
        }
        return this.provider.generateObject(typeName, objectTokenList, globalStateMgr);
    }

    public PEntryObject analyzeUserObject(String typeName, UserIdentity user) throws PrivilegeException {
        return this.provider.generateUserObject(typeName, user, globalStateMgr);
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
            Iterator<Map.Entry<UserIdentity, UserPrivilegeCollection>> mapIter =
                    userToPrivilegeCollection.entrySet().iterator();
            while (mapIter.hasNext()) {
                mapIter.next().getValue().removeInvalidObject(globalStateMgr);
            }

            // 2. remove invalidate roles of users
            roleReadLock();
            try {
                mapIter = userToPrivilegeCollection.entrySet().iterator();
                while (mapIter.hasNext()) {
                    removeInvalidRolesUnlocked(mapIter.next().getValue().getAllRoles());
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
                Iterator<Map.Entry<Long, RolePrivilegeCollection>> mapIter =
                        roleIdToPrivilegeCollection.entrySet().iterator();
                while (mapIter.hasNext()) {
                    mapIter.next().getValue().removeInvalidObject(globalStateMgr);
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
            Iterator<Map.Entry<Long, RolePrivilegeCollection>> roleIter =
                    roleIdToPrivilegeCollection.entrySet().iterator();
            while (roleIter.hasNext()) {
                RolePrivilegeCollection collection = roleIter.next().getValue();
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

    private void getAllPredecessorsInner(long roleId, Set<Long> resultSet) throws PrivilegeException {
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
                    if (IMMUTABLE_BUILT_IN_ROLE_IDS.contains(roleId)) {
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

    public void setLoaded() {
        isLoaded = true;
    }

    /**
     * these public interfaces are for AuthUpgrader to upgrade from 2.x
     */
    public void upgradeUserInitPrivilegeUnlock(UserIdentity userIdentity, UserPrivilegeCollection collection) {
        collection.grantRole(PUBLIC_ROLE_ID);
        userToPrivilegeCollection.put(userIdentity, collection);
        LOG.info("upgrade user {}", userIdentity);
    }

    public void upgradeRoleInitPrivilegeUnlock(long roleId, RolePrivilegeCollection collection) {
        roleIdToPrivilegeCollection.put(roleId, collection);
        roleNameToId.put(collection.getName(), roleId);
        LOG.info("upgrade role {}[{}]", collection.getName(), roleId);
    }

    public void upgradeUserRoleUnlock(UserIdentity user, long... roleIds) {
        UserPrivilegeCollection collection = userToPrivilegeCollection.get(user);
        for (long roleId : roleIds) {
            collection.grantRole(roleId);
        }
    }

    // This function only change data in parent roles
    // Child role will be updated as a whole by upgradeRoleInitPrivilegeUnlock
    public void upgradeParentRoleRelationUnlock(long parentRoleId, long subRoleId) {
        roleIdToPrivilegeCollection.get(parentRoleId).addSubRole(subRoleId);
    }
}
