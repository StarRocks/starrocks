// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PrivilegeManager {
    private static final Logger LOG = LogManager.getLogger(PrivilegeManager.class);
    private static final String ALL_ACTIONS = "ALL";

    @SerializedName(value = "t")
    private final Map<String, Short> typeStringToId;
    @SerializedName(value = "a")
    private final Map<Short, Map<String, Action>> typeToActionMap;
    @SerializedName(value = "r")
    private final Map<String, Long> roleNameToId;
    @SerializedName(value = "p")
    private final Map<String, String> pluralToType;

    protected AuthorizationProvider provider;

    private GlobalStateMgr globalStateMgr;

    protected Map<UserIdentity, UserPrivilegeCollection> userToPrivilegeCollection;

    private final ReentrantReadWriteLock userLock;

    // for quick lookup
    private short dbTypeId;
    private short tableTypeId;
    private short systemTypeId;

    // only when deserialized
    protected PrivilegeManager() {
        typeStringToId = new HashMap<>();
        typeToActionMap = new HashMap<>();
        roleNameToId = new HashMap<>();
        userToPrivilegeCollection = new HashMap<>();
        roleIdToPrivilegeCollection = new HashMap<>();
        pluralToType = new HashMap<>();
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
        typeStringToId = new HashMap<>();
        typeToActionMap = new HashMap<>();
        pluralToType = new HashMap<>();
        roleNameToId = new HashMap<>();
        userLock = new ReentrantReadWriteLock();
        roleLock = new ReentrantReadWriteLock();
        // init typeStringToId  && typeToActionMap
        initIdNameMaps();
        userToPrivilegeCollection = new HashMap<>();
        UserPrivilegeCollection rootPrivileges = new UserPrivilegeCollection();
        // TODO init default roles
        initPrivilegeCollections(rootPrivileges, "SYSTEM", Arrays.asList("GRANT", "ADMIN"), null, false);
        userToPrivilegeCollection.put(UserIdentity.ROOT, rootPrivileges);
        roleIdToPrivilegeCollection = new HashMap<>();
    }

    private void initIdNameMaps() {
        Map<String, List<String>> map = this.provider.getValidPrivilegeTypeToActions();
        short typeId = 0;
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            typeStringToId.put(entry.getKey(), typeId);
            PrivilegeTypes types = PrivilegeTypes.valueOf(entry.getKey());
            if (types.getPlural() != null) {
                pluralToType.put(types.getPlural(), entry.getKey());
            }
            Map<String, Action> actionMap = new HashMap<>();
            typeToActionMap.put(typeId, actionMap);
            typeId++;

            short actionId = 0;
            for (String actionName : map.get(entry.getKey())) {
                actionMap.put(actionName, new Action(actionId, actionName));
                actionId++;
            }
        }
        systemTypeId = typeStringToId.getOrDefault(PrivilegeTypes.SYSTEM.name(), (short) -1);
        dbTypeId = typeStringToId.getOrDefault(PrivilegeTypes.DATABASE.name(), (short) -1);
        tableTypeId = typeStringToId.getOrDefault(PrivilegeTypes.TABLE.name(), (short) -1);
    }

    private void initPrivilegeCollections(
            PrivilegeCollection collection, String type, List<String> actionList, List<String> tokens, boolean isGrant) {
        try {
            short typeId = 0;
            typeId = analyzeType(type);
            ActionSet actionSet = analyzeActionSet(type, typeId, actionList);
            List<PEntryObject> object = null;
            if (tokens != null) {
                object = Arrays.asList(provider.generateObject(type, tokens, globalStateMgr));
            }
            collection.grant(typeId, actionSet, object, isGrant);
        } catch (PrivilegeException e) {
            // all initial privileges are supposed to be legal
            throw new RuntimeException("should not happened!", e);
        }
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
            throw new DdlException("grant failed: " + stmt.getOrigStmt(), e);
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
            throw new DdlException("revoke failed: " + stmt.getOrigStmt(), e);
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

    protected void grantRoleToUser(String roleName, UserIdentity user) throws PrivilegeException {
        userWriteLock();
        try {
            UserPrivilegeCollection collection = getUserPrivilegeCollectionUnlocked(user);

            roleReadLock();
            try {
                long roleId = getRoleIdByNameNoLock(roleName);
                // temporarily add parent role to user to verify predecessors
                collection.grantRole(roleId);
                boolean verifyDone = false;
                try {
                    Set<Long> result = getAllPredecessorsUnlocked(collection);
                    if (result.size() >= Config.privilege_max_total_roles_per_user) {
                        LOG.warn("too many predecessor roles {} for user {}", result, user);
                        throw new PrivilegeException(String.format(
                                "%s has total %d predecessor roles >= %d!",
                                user, result.size(), Config.privilege_max_total_roles_per_user));
                    }
                    verifyDone = true;
                } finally {
                    if (!verifyDone) {
                        collection.revokeRole(roleId);
                    }
                }
            } finally {
                roleReadUnlock();
            }

            globalStateMgr.getEditLog().logUpdateUserPrivilege(
                    user, collection, provider.getPluginId(), provider.getPluginVersion());
            LOG.info("grant role {} to user {}", roleName, user);
        } finally {
            userWriteLock();
        }
    }

    protected void grantRoleToRole(String parentRoleName, String roleName) throws PrivilegeException {
        roleWriteLock();
        try {
            long parentRoleId = getRoleIdByNameNoLock(parentRoleName);
            RolePrivilegeCollection parentCollection = getRolePrivilegeCollectionUnlocked(parentRoleId, true);
            long roleId = getRoleIdByNameNoLock(roleName);
            RolePrivilegeCollection collection = getRolePrivilegeCollectionUnlocked(roleId, true);

            // to avoid circle, verify roleName is not predecessor role of parentRoleName
            Set<Long> parentRolePrecessors = getAllPredecessorsUnlocked(parentRoleId);
            if (parentRolePrecessors.contains(roleId)) {
                throw new PrivilegeException(String.format("role %s[%d] is already a predecessor role of %s[%d]",
                        roleName, roleId, parentRoleName, parentRoleId));
            }

            // temporarily add sub role to parent role to verify inheritance depth
            boolean verifyDone = false;
            parentCollection.addSubRole(roleId);
            try {
                // verify role inheritance depth
                parentRolePrecessors = getAllPredecessorsUnlocked(parentRoleId);
                parentRolePrecessors.add(parentRoleId);
                for (long i : parentRolePrecessors) {
                    long cnt = getMaxRoleInheritanceDepthInner(0, i);
                    if (cnt >= Config.privilege_max_role_depth) {
                        String name = getRolePrivilegeCollectionUnlocked(i, true).getName();
                        throw new PrivilegeException(String.format(
                                "role inheritance depth for %s[%d] is %d >= %d",
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

            // write journal to update privilege collections of both role & parent role
            RolePrivilegeCollectionInfo info = new RolePrivilegeCollectionInfo(
                    roleId, collection, provider.getPluginId(), provider.getPluginVersion());
            info.add(parentRoleId, parentCollection);
            globalStateMgr.getEditLog().logUpdateRolePrivilege(info);
            LOG.info("grant role {}[{}] to role {}[{}]", parentRoleName, parentRoleId, roleName, roleId);
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

    protected void revokeRoleFromUser(String roleName, UserIdentity user) throws PrivilegeException {
        userWriteLock();
        try {
            UserPrivilegeCollection collection = getUserPrivilegeCollectionUnlocked(user);
            roleReadLock();
            try {
                long roleId = getRoleIdByNameNoLock(roleName);
                collection.revokeRole(roleId);
            } finally {
                roleReadUnlock();
            }
            globalStateMgr.getEditLog().logUpdateUserPrivilege(
                    user, collection, provider.getPluginId(), provider.getPluginVersion());
            LOG.info("revoke role {} from user {}", roleName, user);
        } finally {
            userWriteLock();
        }
    }

    protected void revokeRoleFromRole(String parentRoleName, String roleName) throws PrivilegeException {
        roleWriteLock();
        try {
            long parentRoleId = getRoleIdByNameNoLock(parentRoleName);
            RolePrivilegeCollection parentCollection = getRolePrivilegeCollectionUnlocked(parentRoleId, true);
            long roleId = getRoleIdByNameNoLock(roleName);
            RolePrivilegeCollection collection = getRolePrivilegeCollectionUnlocked(roleId, true);

            parentCollection.removeSubRole(roleId);
            collection.removeParentRole(parentRoleId);

            // write journal to update privilege collections of both role & parent role
            RolePrivilegeCollectionInfo info = new RolePrivilegeCollectionInfo(
                    roleId, collection, provider.getPluginId(), provider.getPluginVersion());
            info.add(parentRoleId, parentCollection);
            globalStateMgr.getEditLog().logUpdateRolePrivilege(info);
            LOG.info("revoke role {}[{}] from role {}[{}]", parentRoleName, parentRoleId, roleName, roleId);
        } finally {
            roleWriteUnlock();
        }
    }

    public void validateGrant(String type, List<String> actions, List<PEntryObject> objects) throws PrivilegeException {
        provider.validateGrant(type, actions, objects);
    }

    public static boolean checkTableAction(
            ConnectContext context, String db, String table, PrivilegeTypes.TableActions action) {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context);
            return manager.checkSystemAction(collection, PrivilegeTypes.SystemActions.ADMIN)
                    || manager.checkTableAction(collection, db, table, action);
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when check action[{}] on table {}.{}", action, db, table, e);
            return false;
        }
    }

    public static boolean checkDbAction(ConnectContext context, String db, PrivilegeTypes.DbActions action) {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context);
            return manager.checkSystemAction(collection, PrivilegeTypes.SystemActions.ADMIN)
                    || manager.checkDbAction(collection, db, action);
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when check action[{}] on db {}", action, db, e);
            return false;
        }
    }

    /**
     * show databases; use database
     */
    public static boolean checkAnyActionInDb(ConnectContext context, String db) {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context);
            // 1. check for admin
            if (manager.checkSystemAction(collection, PrivilegeTypes.SystemActions.ADMIN)) {
                return true;
            }
            // 2. check for any action in db
            PEntryObject dbObject = manager.provider.generateObject(
                    PrivilegeTypes.DATABASE.name(), Arrays.asList(db), manager.globalStateMgr);
            if (manager.provider.checkAnyAction(manager.dbTypeId, dbObject, collection)) {
                return true;
            }
            // 3. check for any action in any table in this db
            PEntryObject allTableInDbObject = manager.provider.generateObject(
                    PrivilegeTypes.TABLE.name(),
                    Arrays.asList(PrivilegeTypes.TABLE.getPlural()),
                    PrivilegeTypes.DATABASE.name(),
                    db,
                    manager.globalStateMgr);
            return manager.provider.checkAnyAction(manager.tableTypeId, allTableInDbObject, collection);
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when check any on db {}", db, e);
            return false;
        }
    }

    /**
     * show tables
     */
    public static boolean checkAnyActionInTable(ConnectContext context, String db, String table) {
        PrivilegeManager manager = context.getGlobalStateMgr().getPrivilegeManager();
        try {
            PrivilegeCollection collection = manager.mergePrivilegeCollection(context);
            // 1. check for admin
            if (manager.checkSystemAction(collection, PrivilegeTypes.SystemActions.ADMIN)) {
                return true;
            }
            // 2. check for any action in table
            PEntryObject tableObject = manager.provider.generateObject(
                    PrivilegeTypes.TABLE.name(), Arrays.asList(db, table), manager.globalStateMgr);
            return manager.provider.checkAnyAction(manager.tableTypeId, tableObject, collection);
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when check any on db {}", db, e);
            return false;
        }
    }

    protected boolean checkSystemAction(PrivilegeCollection collection, PrivilegeTypes.SystemActions action) {
        Action want = typeToActionMap.get(systemTypeId).get(action.name());
        return provider.check(systemTypeId, want, null, collection);
    }

    protected boolean checkTableAction(
            PrivilegeCollection collection, String db, String table, PrivilegeTypes.TableActions action)
            throws PrivilegeException {
        Action want = typeToActionMap.get(tableTypeId).get(action.name());
        PEntryObject object = provider.generateObject(
                PrivilegeTypes.TABLE.name(), Arrays.asList(db, table), globalStateMgr);
        return provider.check(tableTypeId, want, object, collection);
    }

    protected boolean checkDbAction(PrivilegeCollection collection, String db, PrivilegeTypes.DbActions action)
            throws PrivilegeException {
        Action want = typeToActionMap.get(dbTypeId).get(action.name());
        PEntryObject object = provider.generateObject(
                PrivilegeTypes.DATABASE.name(), Arrays.asList(db), globalStateMgr);
        return provider.check(dbTypeId, want, object, collection);
    }

    public boolean canExecuteAs(ConnectContext context, UserIdentity impersonateUser) {
        try {
            PrivilegeCollection collection = mergePrivilegeCollection(context);
            String typeStr = PrivilegeTypes.USER.toString();
            short typeId = analyzeType(typeStr);
            PEntryObject object = provider.generateUserObject(typeStr, impersonateUser, globalStateMgr);
            Action want = typeToActionMap.get(typeId).get(PrivilegeTypes.UserActions.IMPERSONATE.toString());
            return provider.check(typeId, want, object, collection);
        } catch (PrivilegeException e) {
            LOG.warn("caught exception in canExecuteAs() user[{}]", impersonateUser, e);
            return false;
        }
    }

    public boolean allowGrant(ConnectContext context, short type, ActionSet wants, List<PEntryObject> objects) {
        try {
            PrivilegeCollection collection = mergePrivilegeCollection(context);
            // check for GRANT action on SYSTEM
            if (checkSystemAction(collection, PrivilegeTypes.SystemActions.ADMIN)) {
                return true;
            }
            // check for WITH GRANT OPTION in the specific type
            return provider.allowGrant(type, wants, objects, collection);
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
            LOG.info("replayed update user {}", user);
        } finally {
            userWriteUnlock();
        }
    }

    /**
     * init all default privilege when a user is created, called by AuthenticationManager
     */
    public UserPrivilegeCollection onCreateUser(UserIdentity user) {
        userWriteLock();
        try {
            // TODO default user privilege
            UserPrivilegeCollection privilegeCollection = new UserPrivilegeCollection();
            userToPrivilegeCollection.put(user, privilegeCollection);
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
        } finally {
            userWriteUnlock();
        }
    }

    public short getProviderPluginId() {
        return provider.getPluginId();
    }

    public short getProviderPluginVerson() {
        return provider.getPluginVersion();
    }

    protected PrivilegeCollection mergePrivilegeCollection(ConnectContext context) throws PrivilegeException {
        userReadLock();
        try {
            UserIdentity userIdentity = context.getCurrentUserIdentity();
            UserPrivilegeCollection userCollection = getUserPrivilegeCollectionUnlocked(userIdentity);
            PrivilegeCollection collection = new PrivilegeCollection();
            collection.merge(userCollection);
            roleReadLock();
            try {
                // 1. get all parent roles by default, but can be specified with `SET ROLE` statement
                Set<Long> roleIds = context.getCurrentRoleIds();
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
            return collection;
        } finally {
            userReadUnlock();
        }
    }

    protected UserPrivilegeCollection getUserPrivilegeCollectionUnlocked(UserIdentity userIdentity) throws PrivilegeException {
        UserPrivilegeCollection userCollection = userToPrivilegeCollection.get(userIdentity);
        if (userCollection == null) {
            throw new PrivilegeException("cannot find " + userIdentity.toString());
        }
        return userCollection;
    }

    // return null if not eixsts
    protected UserPrivilegeCollection getUserPrivilegeCollectionUnlockedAllowNull(UserIdentity userIdentity) {
        return userToPrivilegeCollection.get(userIdentity);
    }

    private RolePrivilegeCollection getRolePrivilegeCollectionUnlocked(long roleId, boolean exceptionIfNotExists)
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

    public ActionSet analyzeActionSet(String typeName, short typeId, List<String> actionNameList)
            throws PrivilegeException {
        Map<String, Action> actionMap = typeToActionMap.get(typeId);
        List<Action> actions = new ArrayList<>();
        for (String actionName : actionNameList) {
            // grant ALL on xx
            if (actionName.equals(ALL_ACTIONS)) {
                return new ActionSet(new ArrayList<>(actionMap.values()));
            }
            // in consideration of legacy format such as SELECT_PRIV
            if (actionName.endsWith("_PRIV")) {
                actionName = actionName.substring(0, actionName.length() - 5);
            }
            Action action = actionMap.get(actionName);
            if (action == null) {
                throw new PrivilegeException("invalid action " + actionName + " for " + typeName);
            }
            actions.add(action);
        }
        return new ActionSet(actions);
    }

    public String analyzeTypeInPlural(String plural) throws PrivilegeException {
        String type = pluralToType.get(plural);
        if (type == null) {
            throw new PrivilegeException("invalid plural privilege type " + plural);
        }
        return type;
    }

    public short analyzeType(String typeName) throws PrivilegeException {
        Short typeId = typeStringToId.get(typeName);
        if (typeId == null) {
            throw new PrivilegeException("cannot find type " + typeName + " in " + typeStringToId.keySet());
        }
        return typeId;
    }

    public void createRole(CreateRoleStmt stmt) throws DdlException {
        roleWriteLock();
        try {
            String roleName = stmt.getQualifiedRole();
            if (roleNameToId.containsKey(roleName)) {
                throw new DdlException(String.format("Role %s already exists!", roleName));
            }
            RolePrivilegeCollection collection = new RolePrivilegeCollection(roleName);
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
                RolePrivilegeCollection privilegeCollection = entry.getValue();
                provider.upgradePrivilegeCollection(privilegeCollection, info.getPluginId(), info.getPluginVersion());
                roleIdToPrivilegeCollection.put(roleId, privilegeCollection);
                if (!roleNameToId.containsKey(privilegeCollection.getName())) {
                    roleNameToId.put(privilegeCollection.getName(), roleId);
                }
                LOG.info("replayed update role {}",  roleId);
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
            RolePrivilegeCollection collection = roleIdToPrivilegeCollection.get(roleId);
            roleIdToPrivilegeCollection.remove(roleId);
            roleNameToId.remove(roleName);
            globalStateMgr.getEditLog().logDropRole(roleId, collection, provider.getPluginId(), provider.getPluginVersion());
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

    public PEntryObject analyzeObject(String type, List<String> allTypes, String restrictType, String restrictName)
            throws PrivilegeException {
        return this.provider.generateObject(type, allTypes, restrictType, restrictName, globalStateMgr);
    }

    /**
     * remove invalid object periodically
     *
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
        // we have to add user lock first because it may contains user privilege
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

        // 4. remove invalidate parent roles & subroles
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
        Iterator<Long> roleIdIter = roleIds.iterator();
        while (roleIdIter.hasNext()) {
            if (!roleIdToPrivilegeCollection.containsKey(roleIdIter.next())) {
                roleIdIter.remove();
            }
        }
    }

    /**
     * get max role inheritance depth
     * e.g grant role_a to role role_b; grant role_b to role role_c;
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
     * get all predecessors roles (parent roles and their parents etc.)
     * e.g grant role_a to role role_b; grant role_b to role role_c;
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
                // 1 json for num user
                int numUser = (int) reader.readJson(int.class);
                LOG.info("loading {} users", numUser);
                for (int i = 0; i != numUser; ++i) {
                    // 2 json for each user(kv)
                    UserIdentity userIdentity = (UserIdentity) reader.readJson(UserIdentity.class);
                    UserPrivilegeCollection collection =
                            (UserPrivilegeCollection) reader.readJson(UserPrivilegeCollection.class);
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
            return ret;
        } catch (SRMetaBlockException e) {
            throw new DdlException("failed to load PrivilegeManager!", e);
        }
    }
}
