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

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.authorization.UserPrivilegeCollectionV2;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.persist.AlterUserInfo;
import com.starrocks.persist.CreateUserInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.GroupProviderLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.SecurityIntegrationPersistInfo;
import com.starrocks.persist.metablock.MapEntryConsumer;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.ast.group.CreateGroupProviderStmt;
import com.starrocks.sql.ast.group.DropGroupProviderStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
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
    public static final long DEFAULT_MAX_CONNECTION_FOR_EXTERNAL_USER = 1000;

    // core data structure
    // user identity -> all the authentication information
    // will be manually serialized one by one
    protected Map<UserIdentity, UserAuthenticationInfo> userToAuthenticationInfo;

    // For legacy reason, user property are set by username instead of full user identity.
    @SerializedName(value = "m")
    private Map<String, UserProperty> userNameToProperty = new HashMap<>();

    @SerializedName("gp")
    protected Map<String, GroupProvider> nameToGroupProviderMap = new ConcurrentHashMap<>();

    @SerializedName("sim")
    protected Map<String, SecurityIntegration> nameToSecurityIntegrationMap = new ConcurrentHashMap<>();

    // resolve hostname to ip
    private Map<String, Set<String>> hostnameToIpSet = new HashMap<>();
    private final ReentrantReadWriteLock hostnameToIpLock = new ReentrantReadWriteLock();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    // set by load() to distinguish brand-new environment with upgraded environment
    private boolean isLoaded = false;

    public AuthenticationMgr() {
        // default user
        userToAuthenticationInfo = new UserAuthInfoTreeMap();
        UserAuthenticationInfo info = new UserAuthenticationInfo(UserRef.ROOT, null);
        userToAuthenticationInfo.put(UserIdentity.ROOT, info);
        userNameToProperty.put(UserIdentity.ROOT.getUser(), new UserProperty());
    }

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
     * Get max connection number based on plain username, the user should be an internal user,
     * if the user doesn't exist in SR, it will throw an exception.
     *
     * @param userName plain username saved in SR
     * @return max connection number of the user
     */
    public long getMaxConn(String userName) {
        UserProperty userProperty = userNameToProperty.get(userName);
        if (userProperty == null) {
            return DEFAULT_MAX_CONNECTION_FOR_EXTERNAL_USER;
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

    public void createUser(CreateUserStmt stmt) throws DdlException {
        UserRef user = stmt.getUser();
        UserIdentity userIdentity = new UserIdentity(user.getUser(), user.getHost(), user.isDomain());
        UserAuthenticationInfo info = new UserAuthenticationInfo(user, stmt.getAuthOption());
        writeLock();
        try {
            if (userToAuthenticationInfo.containsKey(userIdentity)) {
                // Existence verification has been performed in the Analyzer stage. If it exists here,
                // it may be that other threads have performed the same operation, and return directly here
                LOG.info("Operation CREATE USER failed for " + stmt.getUser()
                        + " : user " + stmt.getUser() + " already exists");
                return;
            }

            UserProperty userProperty;
            String userName = userIdentity.getUser();
            if (userNameToProperty.containsKey(userName)) {
                userProperty = userNameToProperty.get(userName);
            } else {
                userProperty = new UserProperty();
            }

            if (stmt.getProperties() != null) {
                userProperty.update(UserProperty.changeToPairList(stmt.getProperties()));
            }

            GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
            AuthorizationMgr authorizationManager = globalStateMgr.getAuthorizationMgr();
            // init user privilege
            UserPrivilegeCollectionV2 collection =
                    authorizationManager.onCreateUser(userIdentity, stmt.getDefaultRoles());

            short pluginId = authorizationManager.getProviderPluginId();
            short pluginVersion = authorizationManager.getProviderPluginVersion();
            final UserProperty finalUserProperty = userProperty;
            globalStateMgr.getEditLog().logCreateUser(
                    new CreateUserInfo(userIdentity, info, userProperty, collection, pluginId, pluginVersion),
                    wal -> {
                        userToAuthenticationInfo.put(userIdentity, info);
                        userNameToProperty.put(userName, finalUserProperty);
                        authorizationManager.setUserPrivilegeCollection(userIdentity, collection);
                    });
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

            UserProperty.UpdateInfo updateInfo = null;
            if (properties != null && !properties.isEmpty()) {
                UserProperty userProperty = userNameToProperty.get(userIdentity.getUser());
                updateInfo = userProperty.checkUpdate(UserProperty.changeToPairList(properties));
            }
            final UserProperty.UpdateInfo finalUpdateInfo = updateInfo;
            GlobalStateMgr.getCurrentState().getEditLog().logAlterUser(
                    new AlterUserInfo(userIdentity, userAuthenticationInfo, properties),
                    wal -> {
                        // update user authentication info
                        userToAuthenticationInfo.put(userIdentity, userAuthenticationInfo);
                        if (finalUpdateInfo != null) {
                            UserProperty userProperty = userNameToProperty.get(userIdentity.getUser());
                            userProperty.update(finalUpdateInfo);
                        }
                    });
        } finally {
            writeUnlock();
        }
    }

    public void updateUserProperty(String user, List<Pair<String, String>> properties) throws DdlException {
        try {
            writeLock();
            UserProperty userProperty = userNameToProperty.getOrDefault(user, null);
            if (userProperty == null) {
                throw new DdlException("user '" + user + "' doesn't exist");
            }
            UserProperty.UpdateInfo result = userProperty.checkUpdate(properties);
            GlobalStateMgr.getCurrentState().getEditLog().logUpdateUserPropertyV2(
                    new UserPropertyInfo(user, properties), wal -> userProperty.update(result));
            LOG.info("finished to update user '{}' with properties: {}", user, properties);
        } finally {
            writeUnlock();
        }
    }

    public void replayUpdateUserProperty(UserPropertyInfo info) {
        try {
            writeLock();
            UserProperty userProperty = userNameToProperty.getOrDefault(info.getUser(), null);
            if (userProperty == null) {
                return;
            }

            userProperty.updateForReplayJournal(info.getProperties());
        } finally {
            writeUnlock();
        }
    }

    public void replayAlterUser(UserIdentity userIdentity, UserAuthenticationInfo info,
                                Map<String, String> properties) {
        writeLock();
        try {
            userToAuthenticationInfo.put(userIdentity, info);
            // updateForReplayJournal will catch all exceptions when replaying user properties
            UserProperty userProperty = userNameToProperty.get(userIdentity.getUser());
            userProperty.updateForReplayJournal(UserProperty.changeToPairList(properties));
        } finally {
            writeUnlock();
        }
    }

    public void dropUser(DropUserStmt stmt) {
        UserRef user = stmt.getUser();
        writeLock();
        try {
            UserIdentity userIdentity = new UserIdentity(user.getUser(), user.getHost(), user.isDomain());
            GlobalStateMgr.getCurrentState().getEditLog().logDropUser(userIdentity, wal -> {
                dropUserNoLock(userIdentity);
                // drop user privilege as well
                GlobalStateMgr.getCurrentState().getAuthorizationMgr().onDropUser(userIdentity);
            });
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
            LOG.info("Operation DROP USER failed for {} : user {} not exists", userIdentity, userIdentity);
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
            userToAuthenticationInfo.put(userIdentity, info);
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
                    ret.userToAuthenticationInfo.put(userIdentity, userAuthenticationInfo);
                });

        LOG.info("loaded {} users", ret.userToAuthenticationInfo.size());

        // mark data is loaded
        this.isLoaded = true;
        this.userNameToProperty = ret.userNameToProperty;
        this.userToAuthenticationInfo = ret.userToAuthenticationInfo;

        this.nameToSecurityIntegrationMap = ret.nameToSecurityIntegrationMap;
        this.nameToGroupProviderMap = ret.nameToGroupProviderMap;

        for (Map.Entry<String, GroupProvider> entry : nameToGroupProviderMap.entrySet()) {
            try {
                entry.getValue().init();
            } catch (Exception e) {
                LOG.error("failed to init group provider", e);
            }
        }
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

    //=========================================== Security Integration ==================================================

    public void createSecurityIntegration(String name,
                                          Map<String, String> propertyMap) throws DdlException {
        if (nameToSecurityIntegrationMap.containsKey(name)) {
            throw new DdlException("security integration '" + name + "' already exists");
        }
        SecurityIntegration securityIntegration = SecurityIntegrationFactory.createSecurityIntegration(name, propertyMap);
        EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
        editLog.logCreateSecurityIntegration(new SecurityIntegrationPersistInfo(name, propertyMap), wal -> {
            nameToSecurityIntegrationMap.put(name, securityIntegration);
        });
        LOG.info("finished to create security integration '{}'", securityIntegration.toString());
    }

    public void replayCreateSecurityIntegration(String name, Map<String, String> propertyMap) {
        SecurityIntegration securityIntegration = SecurityIntegrationFactory.createSecurityIntegration(name, propertyMap);
        nameToSecurityIntegrationMap.put(name, securityIntegration);
    }

    public void alterSecurityIntegration(String name, Map<String, String> alterProps) throws DdlException {
        SecurityIntegration securityIntegration = nameToSecurityIntegrationMap.get(name);
        if (securityIntegration == null) {
            throw new DdlException("security integration '" + name + "' not found");
        } else {
            // COW
            Map<String, String> newProps = Maps.newHashMap(securityIntegration.getPropertyMap());
            // update props
            newProps.putAll(alterProps);
            SecurityIntegration newSecurityIntegration = SecurityIntegrationFactory.createSecurityIntegration(name, newProps);
            EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
            editLog.logAlterSecurityIntegration(new SecurityIntegrationPersistInfo(name, alterProps), wal -> {
                // update map
                nameToSecurityIntegrationMap.put(name, newSecurityIntegration);
            });
            LOG.info("finished to alter security integration '{}' with updated properties {}", name, alterProps);
        }
    }



    public void dropSecurityIntegration(String name) throws DdlException {
        if (!nameToSecurityIntegrationMap.containsKey(name)) {
            throw new DdlException("security integration '" + name + "' not found");
        }

        EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
        editLog.logDropSecurityIntegration(new SecurityIntegrationPersistInfo(name, null), wal -> {
            nameToSecurityIntegrationMap.remove(name);
        });
        LOG.info("finished to drop security integration '{}'", name);
    }

    public SecurityIntegration getSecurityIntegration(String name) {
        return nameToSecurityIntegrationMap.get(name);
    }

    public Set<SecurityIntegration> getAllSecurityIntegrations() {
        return new HashSet<>(nameToSecurityIntegrationMap.values());
    }

    public void replayAlterSecurityIntegration(String name, Map<String, String> alterProps) {
        SecurityIntegration securityIntegration = nameToSecurityIntegrationMap.get(name);
        if (securityIntegration != null) {
            // COW
            Map<String, String> newProps = Maps.newHashMap(securityIntegration.getPropertyMap());
            // update props
            newProps.putAll(alterProps);
            SecurityIntegration newSecurityIntegration =
                    SecurityIntegrationFactory.createSecurityIntegration(name, newProps);
            // update map
            nameToSecurityIntegrationMap.put(name, newSecurityIntegration);
            LOG.info("finished to replay alter security integration '{}' with updated properties {}",
                    name, alterProps);
        }
    }

    public void replayDropSecurityIntegration(String name) throws DdlException {
        nameToSecurityIntegrationMap.remove(name);
    }

    // ---------------------------------------- Group Provider Statement --------------------------------------

    public void createGroupProviderStatement(CreateGroupProviderStmt stmt, ConnectContext context) throws DdlException {
        // Check if group provider already exists
        if (this.nameToGroupProviderMap.containsKey(stmt.getName())) {
            if (stmt.isIfNotExists()) {
                // If IF NOT EXISTS is specified, silently return without error
                return;
            } else {
                throw new DdlException("Group provider '" + stmt.getName() + "' already exists");
            }
        }

        GroupProvider groupProvider = GroupProviderFactory.createGroupProvider(stmt.getName(), stmt.getPropertyMap());
        groupProvider.init();

        GlobalStateMgr.getCurrentState().getEditLog().logCreateGroupProvider(
                new GroupProviderLog(stmt.getName(), stmt.getPropertyMap()),
                wal -> nameToGroupProviderMap.put(stmt.getName(), groupProvider));
    }

    public void replayCreateGroupProvider(String name, Map<String, String> properties) {
        GroupProvider groupProvider = GroupProviderFactory.createGroupProvider(name, properties);
        try {
            groupProvider.init();
            this.nameToGroupProviderMap.put(name, groupProvider);
        } catch (DdlException e) {
            LOG.error("Failed to create group provider '{}'", name, e);
        }
    }

    public void dropGroupProviderStatement(DropGroupProviderStmt stmt, ConnectContext context) throws DdlException {
        GroupProvider groupProvider = this.nameToGroupProviderMap.get(stmt.getName());
        if (groupProvider == null) {
            if (stmt.isIfExists()) {
                // If IF EXISTS is specified, silently return without error
                return;
            } else {
                throw new DdlException("Group provider '" + stmt.getName() + "' does not exist");
            }
        }

        groupProvider.destroy();

        GlobalStateMgr.getCurrentState().getEditLog().logDropGroupProvider(
                new GroupProviderLog(stmt.getName(), null),
                wal -> nameToGroupProviderMap.remove(stmt.getName()));
    }

    public void replayDropGroupProvider(String name) {
        GroupProvider groupProvider = this.nameToGroupProviderMap.remove(name);
        groupProvider.destroy();
    }

    public List<GroupProvider> getAllGroupProviders() {
        return new ArrayList<>(nameToGroupProviderMap.values());
    }

    public GroupProvider getGroupProvider(String name) {
        return nameToGroupProviderMap.get(name);
    }
}
