// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.CreateRoleStmt;
import com.starrocks.analysis.DropRoleStmt;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.DdlException;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PrivilegeManager {
    private static final Logger LOG = LogManager.getLogger(PrivilegeManager.class);
    private static final String ALL_ACTIONS = "ALL";

    @SerializedName(value = "t")
    private final Map<String, Short> typeStringToId;
    @SerializedName(value = "a")
    private final Map<Short, Map<String, Action>> typeToActionMap;
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
        userToPrivilegeCollection = new HashMap<>();
        roleIdToPrivilegeCollection = new HashMap<>();
        pluralToType = new HashMap<>();
        userLock = new ReentrantReadWriteLock();
        roleLock = new ReentrantReadWriteLock();
    }

    private Map<Long, RolePrivilegeCollection> roleIdToPrivilegeCollection;
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
        if (stmt.getRole() != null) {
            throw new DdlException("role not supported!");  // support it later
        }
        try {
            grantToUser(
                    stmt.getTypeId(),
                    stmt.getActionList(),
                    stmt.getObjectList(),
                    stmt.isWithGrantOption(),
                    stmt.getUserIdentity());
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
            UserPrivilegeCollection collection = getUserPrivilegeCollection(userIdentity);
            collection.grant(type, actionSet, objects, isGrant);
            globalStateMgr.getEditLog().logUpdateUserPrivilege(
                    userIdentity, collection, provider.getPluginId(), provider.getPluginVersion());
        } finally {
            userWriteUnlock();
        }
    }

    public void revoke(RevokePrivilegeStmt stmt) throws DdlException {
        if (stmt.getRole() != null) {
            throw new DdlException("role not supported!");  // support it later
        }
        try {
            revokeFromUser(
                    stmt.getTypeId(),
                    stmt.getActionList(),
                    stmt.getObjectList(),
                    stmt.isWithGrantOption(),
                    stmt.getUserIdentity());
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
            UserPrivilegeCollection collection = getUserPrivilegeCollection(userIdentity);
            collection.revoke(type, actionSet, objects, isGrant);
            globalStateMgr.getEditLog().logUpdateUserPrivilege(
                    userIdentity, collection, provider.getPluginId(), provider.getPluginVersion());
        } finally {
            userWriteUnlock();
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
            LOG.info("check admin fails: {} {}",
                    context.getCurrentUserIdentity(), collection.typeToPrivilegeEntryList.get(manager.systemTypeId));
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
            if (!userToPrivilegeCollection.containsKey(userIdentity)) {
                throw new PrivilegeException("cannot find " + userIdentity.toString());
            }
            // TODO merge role privilege
            return userToPrivilegeCollection.get(userIdentity);
        } finally {
            userReadUnlock();
        }
    }

    private UserPrivilegeCollection getUserPrivilegeCollection(UserIdentity userIdentity) throws PrivilegeException {
        if (!userToPrivilegeCollection.containsKey(userIdentity)) {
            throw new PrivilegeException("cannot find " + userIdentity.toString());
        }
        return userToPrivilegeCollection.get(userIdentity);
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
            if (!actionMap.containsKey(actionName)) {
                throw new PrivilegeException("invalid action " + actionName + " for " + typeName);
            }
            actions.add(actionMap.get(actionName));
        }
        return new ActionSet(actions);
    }

    public String analyzeTypeInPlural(String plural) throws PrivilegeException {
        if (! pluralToType.containsKey(plural)) {
            throw new PrivilegeException("invalid plural privilege type " + plural);
        }
        return pluralToType.get(plural);
    }

    public short analyzeType(String typeName) throws PrivilegeException {
        if (!typeStringToId.containsKey(typeName)) {
            throw new PrivilegeException("cannot find type " + typeName + " in " + typeStringToId.keySet());
        }
        return typeStringToId.get(typeName);
    }

    public void createRole(CreateRoleStmt stmt) throws DdlException {
        roleWriteLock();
        try {
            String roleName = stmt.getQualifiedRole();
            Long roleId = getRoleIdByNameNoLock(roleName);
            if (roleId != null) {
                throw new DdlException(String.format("Role %s already exists! id = %d", roleName, roleId));
            }
            RolePrivilegeCollection collection = new RolePrivilegeCollection(roleName);
            long nextRoleId = globalStateMgr.getNextId();
            roleIdToPrivilegeCollection.put(nextRoleId, collection);
            globalStateMgr.getEditLog().logUpdateRolePrivilege(
                    nextRoleId, collection, provider.getPluginId(), provider.getPluginVersion());
            LOG.info("created role {}[{}]", roleName, roleId);
        } finally {
            roleWriteUnlock();
        }
    }

    public void replayUpdateRolePrivilegeCollection(
            long roleId,
            RolePrivilegeCollection privilegeCollection,
            short pluginId,
            short pluginVersion) throws PrivilegeException {
        roleWriteLock();
        try {
            provider.upgradePrivilegeCollection(privilegeCollection, pluginId, pluginVersion);
            roleIdToPrivilegeCollection.put(roleId, privilegeCollection);
            LOG.info("replayed update role {}{}",  roleId, privilegeCollection);
        } finally {
            roleWriteUnlock();
        }
    }

    public void dropRole(DropRoleStmt stmt) throws DdlException {
        roleWriteLock();
        try {
            String roleName = stmt.getQualifiedRole();
            Long roleId = getRoleIdByNameNoLock(roleName);
            if (roleId == null) {
                throw new DdlException(String.format("Role %s doesn't exist! id = %d", roleName, roleId));
            }
            RolePrivilegeCollection collection = roleIdToPrivilegeCollection.get(roleId);
            roleIdToPrivilegeCollection.remove(roleId);
            globalStateMgr.getEditLog().logDropRole(roleId, collection, provider.getPluginId(), provider.getPluginVersion());
            LOG.info("dropped role {}[{}]", roleName, roleId);
        } finally {
            roleWriteUnlock();
        }
    }

    public void replayDropRole(
            long roleId,
            RolePrivilegeCollection privilegeCollection,
            short pluginId,
            short pluginVersion) throws PrivilegeException {
        roleWriteLock();
        try {
            // Actually privilege collection is useless here, but we still record it for further usage
            provider.upgradePrivilegeCollection(privilegeCollection, pluginId, pluginVersion);
            roleIdToPrivilegeCollection.remove(roleId);
            LOG.info("replayed dropped role {}",  roleId);
        } finally {
            roleWriteUnlock();
        }
    }

    public boolean checkRoleExists(String name) {
        roleReadLock();
        try {
            return getRoleIdByNameNoLock(name) != null;
        } finally {
            roleReadUnlock();
        }
    }

    protected Long getRoleIdByNameNoLock(String name) {
        Iterator<Map.Entry<Long, RolePrivilegeCollection>> iterator = roleIdToPrivilegeCollection.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RolePrivilegeCollection> entry = iterator.next();
            if (entry.getValue().getName().equals(name)) {
                return entry.getKey();
            }
        }
        return null;
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

    public void removeInvalidObject() {
        Iterator<Map.Entry<UserIdentity, UserPrivilegeCollection>> mapIter =
                userToPrivilegeCollection.entrySet().iterator();
        while (mapIter.hasNext()) {
            Map.Entry<UserIdentity, UserPrivilegeCollection> entry = mapIter.next();
            UserIdentity user = entry.getKey();
            UserPrivilegeCollection collection = entry.getValue();
            if (! globalStateMgr.getAuthenticationManager().doesUserExist(user)) {
                String collectionStr = GsonUtils.GSON.toJson(collection);
                LOG.info("find invalid user {}, will remove privilegeCollection now {}",
                        entry, collectionStr);
                mapIter.remove();
            } else {
                collection.removeInvalidObject(globalStateMgr);
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