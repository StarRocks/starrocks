// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.authentication;


import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.DdlException;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.privilege.PrivilegeManager;
import com.starrocks.privilege.UserPrivilegeCollection;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class AuthenticationManager {
    private static final Logger LOG = LogManager.getLogger(AuthenticationManager.class);
    private static final String DEFAULT_PLUGIN = PlainPasswordAuthenticationProvider.PLUGIN_NAME;

    public static final String ROOT_USER = "root";

    // core data struction
    // user identity -> all the authentication infomation
    private Map<UserIdentity, UserAuthenticationInfo> userToAuthenticationInfo = new HashMap<>();
    // For legacy reason, user property are set by username instead of full user identity.
    private Map<String, UserProperty> userNameToProperty = new HashMap<>();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    public void init() throws AuthenticationException {
        // default plugin
        AuthenticationProviderFactory.installPlugin(
                PlainPasswordAuthenticationProvider.PLUGIN_NAME, new PlainPasswordAuthenticationProvider());

        // default user
        UserIdentity rootUser = new UserIdentity(ROOT_USER, UserAuthenticationInfo.ANY_HOST);
        rootUser.setIsAnalyzed();
        UserAuthenticationInfo info = new UserAuthenticationInfo();
        info.setOrigUserHost(ROOT_USER, UserAuthenticationInfo.ANY_HOST);
        info.setAuthPlugin(PlainPasswordAuthenticationProvider.PLUGIN_NAME);
        info.setPassword(new byte[0]);
        userToAuthenticationInfo.put(rootUser, info);
        userNameToProperty.put(rootUser.getQualifiedUser(), new UserProperty());
    }

    public boolean doesUserExist(UserIdentity userIdentity) {
        readLock();
        try {
            return userToAuthenticationInfo.containsKey(userIdentity);
        } finally {
            readUnlock();
        }
    }

    public long getMaxConn(String userName) {
        return userNameToProperty.get(userName).getMaxConn();
    }

    public String getDefaultPlugin() {
        return DEFAULT_PLUGIN;
    }

    public UserIdentity checkPassword(String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString) {
        Iterator<Map.Entry<UserIdentity, UserAuthenticationInfo>> it = userToAuthenticationInfo.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<UserIdentity, UserAuthenticationInfo> entry = it.next();
            UserAuthenticationInfo info = entry.getValue();
            if (info.match(remoteUser, remoteHost)) {
                try {
                    AuthenticationProvider provider = AuthenticationProviderFactory.create(info.getAuthPlugin());
                    provider.authenticate(remoteUser, remoteHost, remotePasswd, randomString, info);
                    return entry.getKey();
                } catch (AuthenticationException e) {
                    LOG.debug("failed to authentication, ", e);
                }
                return null;  // authentication failed
            }
        }
        LOG.debug("cannot find user {}@{}", remoteUser, remoteHost);
        return null; // cannot find user
    }

    public void createUser(CreateUserStmt stmt) throws DdlException {
        try {
            // validate authentication info by plugin
            String pluginName = stmt.getAuthPlugin();
            AuthenticationProvider provider = AuthenticationProviderFactory.create(pluginName);
            UserIdentity userIdentity = stmt.getUserIdent();
            UserAuthenticationInfo info = provider.validAuthenticationInfo(
                    userIdentity, stmt.getOriginalPassword(), stmt.getAuthString());
            info.setAuthPlugin(pluginName);
            info.setOrigUserHost(userIdentity.getQualifiedUser(), userIdentity.getHost());

            writeLock();
            try {
                updateUserNoLock(userIdentity, info, false);
                UserProperty userProperty = null;
                if (!userNameToProperty.containsKey(userIdentity.getQualifiedUser())) {
                    userProperty = new UserProperty();
                    userNameToProperty.put(userIdentity.getQualifiedUser(), userProperty);
                }
                GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
                PrivilegeManager privilegeManager = globalStateMgr.getPrivilegeManager();
                // init user privilege
                UserPrivilegeCollection collection = privilegeManager.onCreateUser(userIdentity);
                short pluginId = privilegeManager.getProviderPluginId();
                short pluginVersion = privilegeManager.getProviderPluginVerson();
                globalStateMgr.getEditLog().logCreateUser(
                        userIdentity, info, userProperty, collection, pluginId, pluginVersion);
            } finally {
                writeUnlock();
            }
        } catch (AuthenticationException e) {
            DdlException exception = new DdlException("failed to create user" + stmt.getUserIdent().toString());
            exception.initCause(e);
            throw exception;
        }
    }

    public void replayCreateUser(
            UserIdentity userIdentity,
            UserAuthenticationInfo info,
            UserProperty userProperty,
            UserPrivilegeCollection privilegeCollection,
            short pluginId,
            short pluginVersion)
            throws AuthenticationException, PrivilegeException {
        writeLock();
        try {
            info.analyse();
            updateUserNoLock(userIdentity, info, false);
            if (userProperty != null) {
                userNameToProperty.put(userIdentity.getQualifiedUser(), userProperty);
            }

            GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
            globalStateMgr.getPrivilegeManager().replayUpdateUserPrivilegeCollection(
                    userIdentity, privilegeCollection, pluginId, pluginVersion);
        } finally {
            writeUnlock();
        }
    }

    private void updateUserNoLock(
            UserIdentity userIdentity, UserAuthenticationInfo info, boolean shouldExists) throws AuthenticationException {
        if (userToAuthenticationInfo.containsKey(userIdentity)) {
            if (! shouldExists) {
                throw new AuthenticationException("failed to find user " + userIdentity.getQualifiedUser());
            }
        } else {
            if (shouldExists) {
                throw new AuthenticationException("user " + userIdentity.getQualifiedUser() + " already exists");
            }
        }
        userToAuthenticationInfo.put(userIdentity, info);
    }
}
