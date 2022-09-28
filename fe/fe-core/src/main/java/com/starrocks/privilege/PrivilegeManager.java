// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.DdlException;
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

    @SerializedName(value = "t")
    private final Map<String, Short> typeStringToId;
    @SerializedName(value = "a")
    private final Map<Short, Map<String, Action>> typeToActionMap;

    protected AuthorizationProvider provider;

    private GlobalStateMgr globalStateMgr;

    private Map<UserIdentity, UserPrivilegeCollection> userToPrivilegeCollection;

    private final ReentrantReadWriteLock userLock;

    public PrivilegeManager() {
        typeStringToId = new HashMap<>();
        typeToActionMap = new HashMap<>();
        userToPrivilegeCollection = new HashMap<>();
        userLock = new ReentrantReadWriteLock();
    }

    public PrivilegeManager(GlobalStateMgr globalStateMgr, AuthorizationProvider provider) {
        this.globalStateMgr = globalStateMgr;
        if (provider == null) {
            this.provider = new DefaultAuthorizationProvider();
        } else {
            this.provider = provider;
        }
        // init typeStringToId  && typeToActionMap
        Map<String, List<String>> map = this.provider.getValidPrivilegeTypeToActions();
        typeStringToId = new HashMap<>();
        typeToActionMap = new HashMap<>();
        short typeId = 0;
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            typeStringToId.put(entry.getKey(), typeId);
            Map<String, Action> actionMap = new HashMap<>();
            typeToActionMap.put(typeId, actionMap);
            typeId++;

            short actionId = 0;
            for (String actionName : map.get(entry.getKey())) {
                actionMap.put(actionName, new Action(actionId, actionName));
                actionId++;
            }
        }
        userToPrivilegeCollection = new HashMap<>();
        userLock = new ReentrantReadWriteLock();
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

    public void grant(GrantPrivilegeStmt stmt) throws DdlException {
        if (stmt.getRole() != null) {
            throw new DdlException("role not supported!");  // support it later
        }
        try {
            short typeId = checkType(stmt.getPrivType());
            ActionSet actionSet = checkActionSet(stmt.getPrivType(), typeId, stmt.getPrivList());
            PEntryObject object = provider.generateObject(
                    stmt.getPrivType(), stmt.getPrivilegeObjectNameTokenList(), globalStateMgr);
            List<PEntryObject> objects = Arrays.asList(object); // only support one object for now TBD
            grantToUser(typeId, actionSet, objects, stmt.isWithGrantOption(), stmt.getUserIdentity());
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
            short typeId = checkType(stmt.getPrivType());
            ActionSet actionSet = checkActionSet(stmt.getPrivType(), typeId, stmt.getPrivList());
            PEntryObject object = provider.generateObject(
                    stmt.getPrivType(), stmt.getPrivilegeObjectNameTokenList(), globalStateMgr);
            List<PEntryObject> objects = Arrays.asList(object); // only support one object for now TBD
            revokeFromUser(typeId, actionSet, objects, stmt.isWithGrantOption(), stmt.getUserIdentity());
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

    public boolean check(ConnectContext context, String typeName, String actionName, List<String> objectToken) {
        userReadLock();
        try {
            PEntryObject object = provider.generateObject(
                    typeName, objectToken, globalStateMgr);
            short typeId = checkType(typeName);
            Action want = typeToActionMap.get(typeId).get(actionName);
            return provider.check(typeId, want, object, mergePrivilegeCollection(context));
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when check type[{}] action[{}] object[{}]",
                    typeName, actionName, objectToken, e);
            return false;
        } finally {
            userReadUnlock();
        }
    }

    public boolean checkAnyObject(ConnectContext context, String typeName, String actionName) {
        userReadLock();
        try {
            short typeId = checkType(typeName);
            Action want = typeToActionMap.get(typeId).get(actionName);
            return provider.checkAnyObject(typeId, want, mergePrivilegeCollection(context));
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when checkAnyObject type[{}] action[{}]", typeName, actionName, e);
            return false;
        } finally {
            userReadUnlock();
        }
    }

    public boolean hasType(ConnectContext context, String typeName) {
        userReadLock();
        try {
            short typeId = checkType(typeName);
            return provider.hasType(typeId, mergePrivilegeCollection(context));
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when hasType type[{}]", typeName, e);
            return false;
        } finally {
            userReadUnlock();
        }
    }

    public boolean allowGrant(ConnectContext context, String typeName, String actionName, List<String> objectToken) {
        userReadLock();
        try {
            short typeId = checkType(typeName);
            Action want = typeToActionMap.get(typeId).get(actionName);
            PEntryObject object = provider.generateObject(
                    typeName, objectToken, globalStateMgr);
            return provider.allowGrant(typeId, want, object, mergePrivilegeCollection(context));
        } catch (PrivilegeException e) {
            LOG.warn("caught exception when allowGrant type[{}] action[{}] object[{}]",
                    typeName, actionName, objectToken, e);
            return false;
        } finally {
            userReadUnlock();
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

    private PrivilegeCollection mergePrivilegeCollection(ConnectContext context) throws PrivilegeException {
        UserIdentity userIdentity = context.getCurrentUserIdentity();
        if (!userToPrivilegeCollection.containsKey(userIdentity)) {
            throw new PrivilegeException("cannot find " + userIdentity.toString());
        }
        // TODO merge role privilege
        return userToPrivilegeCollection.get(userIdentity);
    }

    private UserPrivilegeCollection getUserPrivilegeCollection(UserIdentity userIdentity) throws PrivilegeException {
        if (!userToPrivilegeCollection.containsKey(userIdentity)) {
            throw new PrivilegeException("cannot find " + userIdentity.toString());
        }
        return userToPrivilegeCollection.get(userIdentity);
    }

    private ActionSet checkActionSet(String typeName, short typeId, List<String> actionNameList)
            throws PrivilegeException {
        Map<String, Action> actionMap = typeToActionMap.get(typeId);
        List<Action> actions = new ArrayList<>();
        for (String actionName : actionNameList) {
            if (!actionMap.containsKey(actionName)) {
                throw new PrivilegeException("invalid action " + actionName + " for " + typeName);
            }
            actions.add(actionMap.get(actionName));
        }
        return new ActionSet(actions);
    }

    private short checkType(String typeName) throws PrivilegeException {
        if (!typeStringToId.containsKey(typeName)) {
            throw new PrivilegeException("cannot find type " + typeName + " in " + typeStringToId.keySet());
        }
        return typeStringToId.get(typeName);
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
            final int cnt = 1 + 1 + userToPrivilegeCollection.size() * 2;
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
                ret.userToPrivilegeCollection = new HashMap<>();
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
            } catch (SRMetaBlockEOFException eofException) {
                LOG.warn("got EOF exception, ignore, ", eofException);
            } finally {
                reader.close();
            }

            assert ret != null; // can't be NULL
            LOG.info("loaded {} users", ret.userToPrivilegeCollection.size());
            return ret;
        } catch (SRMetaBlockException e) {
            throw new DdlException("failed to load PrivilegeManager!", e);
        }
    }

}