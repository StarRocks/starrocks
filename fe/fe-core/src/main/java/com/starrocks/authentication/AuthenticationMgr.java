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


package com.starrocks.authentication;

import com.google.gson.annotations.SerializedName;
import com.starrocks.StarRocksFE;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.MapEntryConsumer;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.privilege.UserPrivilegeCollectionV2;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AuthenticationMgr {
    private static final Logger LOG = LogManager.getLogger(AuthenticationMgr.class);
    public static final String ROOT_USER = "root";
    public static final long DEFAULT_MAX_CONNECTION_FOR_EXTERNAL_USER = 100;

    // core data structure
    // user identity -> all the authentication information
    // will be manually serialized one by one
    protected Map<UserIdentity, UserAuthenticationInfo> userToAuthenticationInfo;

    private static final Map<String, Long> FAIL_TIME = new ConcurrentHashMap<>();
    private static final Map<String, Integer> FAIL_COUNT = new ConcurrentHashMap<>();
    private static final Map<String, Long> LOCKED_TIMES = new ConcurrentHashMap<>();

    private static class UserAuthInfoTreeMap extends TreeMap<UserIdentity, UserAuthenticationInfo> {
        public UserAuthInfoTreeMap() {
            super((o1, o2) -> {
                // make sure that ip > domain > %
                int compareHostScore = scoreUserIdentityHost(o1).compareTo(scoreUserIdentityHost(o2));
                if (compareHostScore != 0) {
                    return compareHostScore;
                }
                // host type is the same, compare host
                int compareByHost = o1.getHost().compareTo(o2.getHost());
                if (compareByHost != 0) {
                    return compareByHost;
                }
                // compare user name
                return o1.getUser().compareTo(o2.getUser());
            });
        }

        /**
         * If someone log in from 10.1.1.1 with name "test_user", the matching UserIdentity
         * can be sorted in the below order,
         * 1. test_user@10.1.1.1
         * 2. test_user@["hostname"], in which "hostname" can be resolved to 10.1.1.1.
         * If multiple hostnames match the login ip, just return one randomly.
         * 3. test_user@%, as a fallback.
         */
        private static Integer scoreUserIdentityHost(UserIdentity userIdentity) {
            // ip(1) > hostname(2) > %(3)
            if (userIdentity.isDomain()) {
                return 2;
            }
            if (userIdentity.getHost().equals(UserAuthenticationInfo.ANY_HOST)) {
                return 3;
            }
            return 1;
        }
    }

    // For legacy reason, user property are set by username instead of full user identity.
    @SerializedName(value = "m")
    private Map<String, UserProperty> userNameToProperty = new HashMap<>();

    // resolve hostname to ip
    private Map<String, Set<String>> hostnameToIpSet = new HashMap<>();
    private final ReentrantReadWriteLock hostnameToIpLock = new ReentrantReadWriteLock();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    // set by load() to distinguish brand-new environment with upgraded environment
    private boolean isLoaded = false;

    public AuthenticationMgr() {
        // default plugin
        AuthenticationProviderFactory.installPlugin(
                PlainPasswordAuthenticationProvider.PLUGIN_NAME, new PlainPasswordAuthenticationProvider());
        AuthenticationProviderFactory.installPlugin(
                LDAPAuthProviderForNative.PLUGIN_NAME, new LDAPAuthProviderForNative());
        AuthenticationProviderFactory.installPlugin(
                KerberosAuthenticationProvider.PLUGIN_NAME, new KerberosAuthenticationProvider());

        // default user
        userToAuthenticationInfo = new UserAuthInfoTreeMap();
        UserAuthenticationInfo info = new UserAuthenticationInfo();
        try {
            info.setOrigUserHost(ROOT_USER, UserAuthenticationInfo.ANY_HOST);
        } catch (AuthenticationException e) {
            throw new RuntimeException("should not happened!", e);
        }
        info.setAuthPlugin(PlainPasswordAuthenticationProvider.PLUGIN_NAME);
        info.setPassword(MysqlPassword.EMPTY_PASSWORD);
        userToAuthenticationInfo.put(UserIdentity.ROOT, info);
        userNameToProperty.put(UserIdentity.ROOT.getUser(), new UserProperty());
    }

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

    public boolean doesUserExist(UserIdentity userIdentity) {
        readLock();
        try {
            return userToAuthenticationInfo.containsKey(userIdentity);
        } finally {
            readUnlock();
        }
    }

    /**
     * Get max connection number of the user, if the user is ephemeral, i.e. the user is saved in SR,
     * but some external system, like LDAP, return default max connection number
     * @param currUserIdentity user identity of current connection
     * @return max connection number of the user
     */
    public long getMaxConn(UserIdentity currUserIdentity) {
        if (currUserIdentity.isEphemeral()) {
            return DEFAULT_MAX_CONNECTION_FOR_EXTERNAL_USER;
        } else {
            String userName = currUserIdentity.getUser();
            return getMaxConn(userName);
        }
    }

    /**
     * Get max connection number based on plain username, the user should be an internal user,
     * if the user doesn't exist in SR, it will throw an exception.
     * @param userName plain username saved in SR
     * @return max connection number of the user
     */
    public long getMaxConn(String userName) {
        UserProperty userProperty = userNameToProperty.get(userName);
        if (userProperty == null) {
            throw new SemanticException("Unknown user: " + userName);
        } else {
            return userNameToProperty.get(userName).getMaxConn();
        }
    }

    private boolean match(String remoteUser, String remoteHost, boolean isDomain, UserAuthenticationInfo info) {
        // quickly filter unmatched entries by username
        if (!info.matchUser(remoteUser)) {
            return false;
        }
        if (isDomain) {
            // check for resolved ips
            this.hostnameToIpLock.readLock().lock();
            try {
                Set<String> ipSet = hostnameToIpSet.get(info.getOrigHost());
                if (ipSet == null) {
                    return false;
                }
                return ipSet.contains(remoteHost);
            } finally {
                this.hostnameToIpLock.readLock().unlock();
            }
        } else {
            return info.matchHost(remoteHost);
        }
    }

    public Map.Entry<UserIdentity, UserAuthenticationInfo> getBestMatchedUserIdentity(
            String remoteUser, String remoteHost) {
        try {
            readLock();
            return userToAuthenticationInfo.entrySet().stream()
                    .filter(entry -> match(remoteUser, remoteHost, entry.getKey().isDomain(), entry.getValue()))
                    .findFirst().orElse(null);
        } finally {
            readUnlock();
        }
    }

    private UserIdentity checkPasswordForNative(
            String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString) {
        Map.Entry<UserIdentity, UserAuthenticationInfo> matchedUserIdentity =
                getBestMatchedUserIdentity(remoteUser, remoteHost);
        if (matchedUserIdentity == null) {
            LOG.debug("cannot find user {}@{}", remoteUser, remoteHost);
        } else {
            try {
                AuthenticationProvider provider =
                        AuthenticationProviderFactory.create(matchedUserIdentity.getValue().getAuthPlugin());
                provider.authenticate(remoteUser, remoteHost, remotePasswd, randomString,
                        matchedUserIdentity.getValue());
                return matchedUserIdentity.getKey();
            } catch (AuthenticationException e) {
                LOG.debug("failed to authenticate for native, user: {}@{}, error: {}",
                        remoteUser, remoteHost, e.getMessage());
            }
        }

        return null;
    }

    public UserIdentity checkPassword(String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString) {
        UserIdentity authenticatedUser = null;
        String userAndHost = remoteUser.concat("@").concat(remoteHost);
        for (Map.Entry<String, Long> attempt : FAIL_TIME.entrySet()) {
            if (((System.currentTimeMillis() - attempt.getValue()) / 1000) > Config.password_lock_interval_seconds) {
                if (!isUserLocked(attempt.getKey())) {
                    clearFailedAttemptRecords(attempt.getKey());
                } else {
                    if (getRemainingLockedTime(attempt.getKey()) <= 0) {
                        clearFailedAttemptRecords(attempt.getKey());
                    }
                }
            }
        }
        if (!allowLoginAttempt(userAndHost)) {
            return null;
        }
        authenticatedUser = checkPasswordForNative(remoteUser, remoteHost, remotePasswd, randomString);
        if (Config.max_failed_login_attempts >= 0) {
            if (authenticatedUser == null) {
                recordFailedAttempt(userAndHost);
            } else {
                clearFailedAttemptRecords(userAndHost);
            }
        }
        return authenticatedUser;
    }

    public Long getRemainingLockedTime(String userAndHost) {
        return Config.password_lock_interval_seconds - ((System.currentTimeMillis() - LOCKED_TIMES.get(userAndHost)) / 1000);
    }

    private boolean allowLoginAttempt(String userAndHost) {
        if (!FAIL_TIME.containsKey(userAndHost)) {
            return true;
        }
        return !isUserLocked(userAndHost);
    }

    private void recordFailedAttempt(String userAndHost) {
        if (!FAIL_TIME.containsKey(userAndHost)) {
            FAIL_TIME.put(userAndHost, System.currentTimeMillis());
        }
        FAIL_COUNT.put(userAndHost,
                FAIL_COUNT.getOrDefault(userAndHost, 0) + 1);
        if (!LOCKED_TIMES.containsKey(userAndHost)
                && FAIL_COUNT.get(userAndHost) >= Config.max_failed_login_attempts) {
            LOCKED_TIMES.put(userAndHost, System.currentTimeMillis());
        }
    }

    private void clearFailedAttemptRecords(String userAndHost) {
        if (FAIL_TIME.containsKey(userAndHost)) {
            FAIL_TIME.remove(userAndHost);
            FAIL_COUNT.remove(userAndHost);
            LOCKED_TIMES.remove(userAndHost);
        }
    }

    public boolean isUserLocked(String userAndHost) {
        return LOCKED_TIMES.containsKey(userAndHost);
    }

    public UserIdentity checkPlainPassword(String remoteUser, String remoteHost, String remotePasswd) {
        return checkPassword(remoteUser, remoteHost,
                remotePasswd.getBytes(StandardCharsets.UTF_8), null);
    }

    public void createUser(CreateUserStmt stmt) throws DdlException {
        UserIdentity userIdentity = stmt.getUserIdentity();
        UserAuthenticationInfo info = stmt.getAuthenticationInfo();
        writeLock();
        try {
            if (userToAuthenticationInfo.containsKey(userIdentity)) {
                // Existence verification has been performed in the Analyzer stage. If it exists here,
                // it may be that other threads have performed the same operation, and return directly here
                LOG.info("Operation CREATE USER failed for " + stmt.getUserIdentity()
                        + " : user " + stmt.getUserIdentity() + " already exists");
                return;
            }

            UserProperty userProperty = null;
            String userName = userIdentity.getUser();
            if (userNameToProperty.containsKey(userName)) {
                userProperty = userNameToProperty.get(userName);
            } else {
                userProperty = new UserProperty();
            }

            if (stmt.getProperties() != null) {
                // If we create the user with properties, we need to call userProperty.update to check and update userProperty.
                // If there are failures, update method will throw an exception
                userProperty.update(userIdentity, UserProperty.changeToPairList(stmt.getProperties()));
            }

            // If all checks are passed, we can add the user to the userToAuthenticationInfo and userNameToProperty
            userToAuthenticationInfo.put(userIdentity, info);
            userNameToProperty.put(userName, userProperty);

            GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
            AuthorizationMgr authorizationManager = globalStateMgr.getAuthorizationMgr();
            // init user privilege
            UserPrivilegeCollectionV2 collection =
                    authorizationManager.onCreateUser(userIdentity, stmt.getDefaultRoles());

            short pluginId = authorizationManager.getProviderPluginId();
            short pluginVersion = authorizationManager.getProviderPluginVersion();
            globalStateMgr.getEditLog().logCreateUser(
                    userIdentity, info, userProperty, collection, pluginId, pluginVersion);

        } catch (PrivilegeException e) {
            throw new DdlException("failed to create user " + userIdentity + " : " + e.getMessage(), e);
        } finally {
            writeUnlock();
        }
    }

    // This method is used to update user information, including authentication information and user properties
    // Note: if properties is null, we should keep the original properties
    public void alterUser(UserIdentity userIdentity, UserAuthenticationInfo userAuthenticationInfo,
                          Map<String, String> properties) throws DdlException {
        writeLock();
        try {
            if (!userToAuthenticationInfo.containsKey(userIdentity)) {
                // Existence verification has been performed in the Analyzer stage. If it not exists here,
                // it may be that other threads have performed the same operation, and return directly here
                LOG.info("Operation ALTER USER failed for " + userIdentity + " : user " + userIdentity + " not exists");
                return;
            }

            updateUserNoLock(userIdentity, userAuthenticationInfo, true);
            if (properties != null && properties.size() > 0) {
                UserProperty userProperty = userNameToProperty.get(userIdentity.getUser());
                userProperty.update(userIdentity, UserProperty.changeToPairList(properties));
            }
            GlobalStateMgr.getCurrentState().getEditLog().logAlterUser(userIdentity, userAuthenticationInfo, properties);
        } catch (AuthenticationException e) {
            throw new DdlException("failed to alter user " + userIdentity, e);
        } finally {
            writeUnlock();
        }
    }

    private void updateUserPropertyNoLock(String user, List<Pair<String, String>> properties, boolean isReplay)
            throws DdlException {
        UserProperty userProperty = userNameToProperty.getOrDefault(user, null);
        if (userProperty == null) {
            throw new DdlException("user '" + user + "' doesn't exist");
        }
        if (isReplay) {
            userProperty.updateForReplayJournal(properties);
        } else {
            userProperty.update(user, properties);
        }
    }

    public void updateUserProperty(String user, List<Pair<String, String>> properties) throws DdlException {
        try {
            writeLock();
            updateUserPropertyNoLock(user, properties, false);
            UserPropertyInfo propertyInfo = new UserPropertyInfo(user, properties);
            GlobalStateMgr.getCurrentState().getEditLog().logUpdateUserPropertyV2(propertyInfo);
            LOG.info("finished to update user '{}' with properties: {}", user, properties);
        } finally {
            writeUnlock();
        }
    }

    public void replayUpdateUserProperty(UserPropertyInfo info) throws DdlException {
        try {
            writeLock();
            updateUserPropertyNoLock(info.getUser(), info.getProperties(), true);
        } finally {
            writeUnlock();
        }
    }

    public void replayAlterUser(UserIdentity userIdentity, UserAuthenticationInfo info,
                                Map<String, String> properties) throws AuthenticationException {
        writeLock();
        try {
            updateUserNoLock(userIdentity, info, true);
            // updateForReplayJournal will catch all exceptions when replaying user properties
            UserProperty userProperty = userNameToProperty.get(userIdentity.getUser());
            userProperty.updateForReplayJournal(UserProperty.changeToPairList(properties));
        } finally {
            writeUnlock();
        }
    }

    public void dropUser(DropUserStmt stmt) throws DdlException {
        UserIdentity userIdentity = stmt.getUserIdentity();
        writeLock();
        try {
            dropUserNoLock(userIdentity);
            // drop user privilege as well
            GlobalStateMgr.getCurrentState().getAuthorizationMgr().onDropUser(userIdentity);
            GlobalStateMgr.getCurrentState().getEditLog().logDropUser(userIdentity);
        } finally {
            writeUnlock();
        }
    }

    public void replayDropUser(UserIdentity userIdentity) {
        writeLock();
        try {
            dropUserNoLock(userIdentity);
            // drop user privilege as well
            GlobalStateMgr.getCurrentState().getAuthorizationMgr().onDropUser(userIdentity);
        } finally {
            writeUnlock();
        }
    }

    private void dropUserNoLock(UserIdentity userIdentity) {
        // 1. remove from userToAuthenticationInfo
        if (!userToAuthenticationInfo.containsKey(userIdentity)) {
            LOG.info("Operation DROP USER failed for " + userIdentity + " : user " + userIdentity + " not exists");
            return;
        }
        userToAuthenticationInfo.remove(userIdentity);
        LOG.info("user {} is dropped", userIdentity);
        // 2. remove from userNameToProperty
        String userName = userIdentity.getUser();
        if (!hasUserNameNoLock(userName)) {
            LOG.info("user property for {} is dropped: {}", userName, userNameToProperty.get(userName));
            userNameToProperty.remove(userName);
        }
    }

    public void replayCreateUser(
            UserIdentity userIdentity,
            UserAuthenticationInfo info,
            UserProperty userProperty,
            UserPrivilegeCollectionV2 privilegeCollection,
            short pluginId,
            short pluginVersion)
            throws AuthenticationException, PrivilegeException {
        writeLock();
        try {
            info.analyze();
            updateUserNoLock(userIdentity, info, false);
            if (userProperty != null) {
                userNameToProperty.put(userIdentity.getUser(), userProperty);
            }

            GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
            globalStateMgr.getAuthorizationMgr().replayUpdateUserPrivilegeCollection(
                    userIdentity, privilegeCollection, pluginId, pluginVersion);
        } finally {
            writeUnlock();
        }
    }

    private void updateUserNoLock(UserIdentity userIdentity, UserAuthenticationInfo info, boolean shouldExists)
            throws AuthenticationException {
        if (userToAuthenticationInfo.containsKey(userIdentity)) {
            if (!shouldExists) {
                throw new AuthenticationException("user " + userIdentity.getUser() + " already exists");
            }
        } else {
            if (shouldExists) {
                throw new AuthenticationException("failed to find user " + userIdentity.getUser());
            }
        }
        userToAuthenticationInfo.put(userIdentity, info);
    }

    private boolean hasUserNameNoLock(String userName) {
        for (UserIdentity userIdentity : userToAuthenticationInfo.keySet()) {
            if (userIdentity.getUser().equals(userName)) {
                return true;
            }
        }
        return false;
    }

    public Set<String> getAllHostnames() {
        readLock();
        try {
            Set<String> ret = new HashSet<>();
            for (UserIdentity userIdentity : userToAuthenticationInfo.keySet()) {
                if (userIdentity.isDomain()) {
                    ret.add(userIdentity.getHost());
                }
            }
            return ret;
        } finally {
            readUnlock();
        }
    }

    /**
     * called by DomainResolver to periodically update hostname -> ip set
     */
    public void setHostnameToIpSet(Map<String, Set<String>> hostnameToIpSet) {
        this.hostnameToIpLock.writeLock().lock();
        try {
            this.hostnameToIpSet = hostnameToIpSet;
        } finally {
            this.hostnameToIpLock.writeLock().unlock();
        }
    }

    public boolean isLoaded() {
        return isLoaded;
    }

    public void setLoaded(boolean loaded) {
        isLoaded = loaded;
    }

    public UserAuthenticationInfo getUserAuthenticationInfoByUserIdentity(UserIdentity userIdentity) {
        return userToAuthenticationInfo.get(userIdentity);
    }

    public Map<UserIdentity, UserAuthenticationInfo> getUserToAuthenticationInfo() {
        return userToAuthenticationInfo;
    }

    private Class<?> authClazz = null;
    public static final String KRB5_AUTH_CLASS_NAME = "com.starrocks.plugins.auth.KerberosAuthentication";
    public static final String KRB5_AUTH_JAR_PATH = StarRocksFE.STARROCKS_HOME_DIR + "/lib/starrocks-kerberos.jar";

    public boolean isSupportKerberosAuth() {
        if (!Config.enable_authentication_kerberos) {
            LOG.error("enable_authentication_kerberos need to be set to true");
            return false;
        }

        if (Config.authentication_kerberos_service_principal.isEmpty()) {
            LOG.error("authentication_kerberos_service_principal must be set in config");
            return false;
        }

        if (Config.authentication_kerberos_service_key_tab.isEmpty()) {
            LOG.error("authentication_kerberos_service_key_tab must be set in config");
            return false;
        }

        if (authClazz == null) {
            try {
                File jarFile = new File(KRB5_AUTH_JAR_PATH);
                if (!jarFile.exists()) {
                    LOG.error("Can not found jar file at {}", KRB5_AUTH_JAR_PATH);
                    return false;
                } else {
                    ClassLoader loader = URLClassLoader.newInstance(
                            new URL[] {
                                    jarFile.toURL()
                            },
                            getClass().getClassLoader()
                    );
                    authClazz = Class.forName(AuthenticationMgr.KRB5_AUTH_CLASS_NAME, true, loader);
                }
            } catch (Exception e) {
                LOG.error("Failed to load {}", AuthenticationMgr.KRB5_AUTH_CLASS_NAME, e);
                return false;
            }
        }

        return true;
    }

    public Class<?> getAuthClazz() {
        return authClazz;
    }

    public void saveV2(ImageWriter imageWriter) throws IOException {
        try {
            // 1 json for myself,1 json for number of users, 2 json for each user(kv)
            final int cnt = 1 + 1 + userToAuthenticationInfo.size() * 2;
            SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.AUTHENTICATION_MGR, cnt);
            // 1 json for myself
            writer.writeJson(this);
            // 1 json for num user
            writer.writeInt(userToAuthenticationInfo.size());
            for (Map.Entry<UserIdentity, UserAuthenticationInfo> entry : userToAuthenticationInfo.entrySet()) {
                // 2 json for each user(kv)
                writer.writeJson(entry.getKey());
                writer.writeJson(entry.getValue());
            }
            LOG.info("saved {} users", userToAuthenticationInfo.size());
            writer.close();
        } catch (SRMetaBlockException e) {
            IOException exception = new IOException("failed to save AuthenticationManager!");
            exception.initCause(e);
            throw exception;
        }
    }

    public void loadV2(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        // 1 json for myself
        AuthenticationMgr ret = reader.readJson(AuthenticationMgr.class);
        ret.userToAuthenticationInfo = new UserAuthInfoTreeMap();

        LOG.info("loading users");
        reader.readMap(UserIdentity.class, UserAuthenticationInfo.class,
                (MapEntryConsumer<UserIdentity, UserAuthenticationInfo>) (userIdentity, userAuthenticationInfo) -> {
                    try {
                        userAuthenticationInfo.analyze();
                    } catch (AuthenticationException e) {
                        throw new IOException(e);
                    }

                    ret.userToAuthenticationInfo.put(userIdentity, userAuthenticationInfo);
                });

        LOG.info("loaded {} users", ret.userToAuthenticationInfo.size());

        // mark data is loaded
        this.isLoaded = true;
        this.userNameToProperty = ret.userNameToProperty;
        this.userToAuthenticationInfo = ret.userToAuthenticationInfo;
    }

    public UserProperty getUserProperty(String userName) {
        UserProperty userProperty = userNameToProperty.get(userName);
        if (userProperty == null) {
            throw new SemanticException("Unknown user: " + userName);
        }
        return userProperty;
    }

    public UserIdentity getUserIdentityByName(String userName) {
        Map<UserIdentity, UserAuthenticationInfo> userToAuthInfo = getUserToAuthenticationInfo();
        Map.Entry<UserIdentity, UserAuthenticationInfo> matchedUserIdentity = userToAuthInfo.entrySet().stream()
                .filter(entry -> (entry.getKey().getUser().equals(userName)))
                .findFirst().orElse(null);
        if (matchedUserIdentity == null) {
            throw new SemanticException("Unknown user: " + userName);
        }

        return matchedUserIdentity.getKey();
    }
}
