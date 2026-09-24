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
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.Pair;
import com.starrocks.common.util.PrintableMap;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

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

    /**
     * Serializes group provider DDL (CREATE / DROP / ALTER) on the leader. ALTER reads the current
     * properties, merges the delta and writes one journal record; without this, two concurrent ALTERs
     * would each merge onto the same snapshot, so one delta would be lost in this FE's memory while the
     * journal - and therefore every follower, and this FE itself after a restart - still carries both.
     * The same window lets an ALTER put a provider back after a concurrent DROP removed it.
     *
     * Held for the read-merge and again for the swap, but deliberately NOT across
     * prepareForActivation(): that blocks on LDAP I/O for as long as ldap_conn_timeout allows, and a DDL
     * running on the leader's handler thread cannot be cancelled (StmtExecutor.cancel() only reaches a
     * Coordinator), so an ALTER against an unreachable directory would otherwise park every other group
     * provider statement behind it. What the lock guarantees instead is checked explicitly: the swap only
     * happens if the provider is still the instance the merge was based on, and ALTER redoes the merge if
     * it is not. Also deliberately not the class-wide {@code lock}, which guards user metadata on the
     * login path. Journal replay does not take it - it is single threaded and never runs next to local DDL.
     */
    private final Object groupProviderDdlLock = new Object();

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
        return match(remoteUser, remoteHost, isDomain, info, false);
    }

    private boolean match(String remoteUser, String remoteHost, boolean isDomain, UserAuthenticationInfo info,
                          boolean ignoreUserCase) {
        // quickly filter unmatched entries by username
        if (!(ignoreUserCase ? info.matchUserCaseInsensitive(remoteUser) : info.matchUser(remoteUser))) {
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

    /**
     * Resolve a login to the stored user it should authenticate against.
     * <p>
     * An ambiguous case-insensitive match yields null here. Call
     * {@link #getBestMatchedUserIdentityForLogin(String, String)} on the authentication path, where
     * ambiguity must be refused rather than silently resolved.
     */
    public Map.Entry<UserIdentity, UserAuthenticationInfo> getBestMatchedUserIdentity(
            String remoteUser, String remoteHost) {
        List<Map.Entry<UserIdentity, UserAuthenticationInfo>> matched = matchUserIdentities(remoteUser, remoteHost);
        if (matched.size() > 1) {
            LOG.warn("user '{}'@'{}' matches several LDAP users when ignoring case: {}. Refusing to pick one.",
                    remoteUser, remoteHost, describeUserNames(matched));
            return null;
        }
        return matched.isEmpty() ? null : matched.get(0);
    }

    /**
     * Same lookup as {@link #getBestMatchedUserIdentity(String, String)}, but an ambiguous
     * case-insensitive match fails the login instead of resolving to one of the candidates. Which
     * candidate "wins" would be decided by the entry ordering, i.e. by an implicit character order,
     * and the candidates can carry completely different DNs and privileges.
     */
    public Map.Entry<UserIdentity, UserAuthenticationInfo> getBestMatchedUserIdentityForLogin(
            String remoteUser, String remoteHost) throws AuthenticationException {
        List<Map.Entry<UserIdentity, UserAuthenticationInfo>> matched = matchUserIdentities(remoteUser, remoteHost);
        if (matched.size() > 1) {
            // The conflicting names go to the log, not to the client: this runs before any password
            // has been checked, so echoing them back would disclose which accounts exist, and with
            // what host patterns, to anyone who can open a connection.
            LOG.warn("user '{}'@'{}' matches several LDAP users when ignoring case: {}. Refusing the login.",
                    remoteUser, remoteHost, describeUserNames(matched));
            throw new AuthenticationException(ErrorCode.ERR_AMBIGUOUS_LDAP_USER, remoteUser);
        }
        return matched.isEmpty() ? null : matched.get(0);
    }

    /**
     * @return the matching entries, best host first. At most one entry unless the case-insensitive
     *         second pass found candidates that differ in more than just their host.
     */
    private List<Map.Entry<UserIdentity, UserAuthenticationInfo>> matchUserIdentities(
            String remoteUser, String remoteHost) {
        try {
            readLock();
            // The entries are ordered ip > domain > '%', so the first exact hit is the best match.
            Map.Entry<UserIdentity, UserAuthenticationInfo> exact = userToAuthenticationInfo.entrySet().stream()
                    .filter(entry -> match(remoteUser, remoteHost, entry.getKey().isDomain(), entry.getValue()))
                    .findFirst().orElse(null);
            if (!Config.authentication_ldap_case_insensitive) {
                return exact == null ? List.of() : List.of(exact);
            }
            if (exact != null && !exact.getValue().isLdapAuthPlugin()) {
                // A native-password, JWT or OAuth2 user is its own account; another user whose name
                // differs only in case is unrelated to it and must not make this login ambiguous.
                return List.of(exact);
            }

            // Look for LDAP users whose name differs from the typed one only in case. This runs even
            // when an exact entry matched, because under this flag those entries all denote one
            // directory account: the same password opens every one of them, so leaving the exact
            // spelling as a way to select which of their privilege sets applies would defeat the
            // ambiguity check. Restricted to AUTHENTICATION_LDAP_SIMPLE users: relaxing it for
            // native passwords would let an attacker probe 'Root', 'ROOT', ... for a user
            // deliberately created as 'root'.
            List<Map.Entry<UserIdentity, UserAuthenticationInfo>> ldapMatches =
                    userToAuthenticationInfo.entrySet().stream()
                            .filter(entry -> entry.getValue().isLdapAuthPlugin())
                            .filter(entry -> match(remoteUser, remoteHost, entry.getKey().isDomain(),
                                    entry.getValue(), true))
                            .collect(Collectors.toList());
            // Several entries carrying the same user name only means several host patterns matched,
            // which the entry ordering already resolves. Entries carrying different user names are a
            // genuine ambiguity and are all returned for the caller to reject.
            boolean ambiguous = ldapMatches.stream().map(entry -> entry.getKey().getUser()).distinct().count() > 1;
            if (ambiguous) {
                return ldapMatches;
            }
            if (exact != null) {
                return List.of(exact);
            }
            return ldapMatches.isEmpty() ? List.of() : List.of(ldapMatches.get(0));
        } finally {
            readUnlock();
        }
    }

    private static String describeUserNames(List<Map.Entry<UserIdentity, UserAuthenticationInfo>> entries) {
        return entries.stream().map(entry -> entry.getKey().toString()).distinct()
                .collect(Collectors.joining(", "));
    }

    /**
     * Stored LDAP users whose name equals {@code user} ignoring case but is not identical to it.
     * Used to keep such a pair from being created in the first place, because a login that matches
     * both of them can only be refused.
     */
    public List<UserIdentity> getLdapUsersCollidingByCase(String user) {
        try {
            readLock();
            // Use the same predicate the login uses rather than equalsIgnoreCase, so the guard
            // cannot reject a pair that the login would in fact tell apart. The two fold case
            // differently outside ASCII.
            return userToAuthenticationInfo.entrySet().stream()
                    .filter(entry -> entry.getValue().isLdapAuthPlugin())
                    .filter(entry -> !entry.getKey().getUser().equals(user)
                            && entry.getValue().matchUserCaseInsensitive(user))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
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
        // Copy rather than adopt: Gson builds the field from its declared type (Map), so an image
        // that carries a "gp" entry - and saveV2 always writes one, even when empty - hands back a
        // LinkedTreeMap, not the ConcurrentHashMap the field was initialized with. Readers of this
        // map (getGroupProvider, getAllGroupProviders, and the login path through them) are
        // deliberately lock-free, which only holds for a genuinely concurrent map.
        this.nameToGroupProviderMap = new ConcurrentHashMap<>(ret.nameToGroupProviderMap);

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
            LOG.info("finished to alter security integration '{}' with updated properties {}", name,
                    maskedProps(alterProps));
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
                    name, maskedProps(alterProps));
        }
    }

    public void replayDropSecurityIntegration(String name) throws DdlException {
        nameToSecurityIntegrationMap.remove(name);
    }

    // ---------------------------------------- Group Provider Statement --------------------------------------

    public void createGroupProviderStatement(CreateGroupProviderStmt stmt, ConnectContext context) throws DdlException {
        String name = stmt.getName();
        // Reported before any I/O, so a duplicate name does not first wait out a directory round trip.
        if (groupProviderAlreadyExists(name, stmt.isIfNotExists())) {
            return;
        }

        // init() runs outside the DDL lock: FileGroupProvider.init() reads group_file_url, which for an
        // http(s) value is URL.openStream() with no timeout at all, and the LDAP one starts a schedule.
        // Holding the lock across that would park every other group provider statement behind it -
        // including the DROP an operator would reach for to get out of it. The name is re-checked below,
        // so a concurrent statement that took it in the meantime is still reported instead of overwritten.
        GroupProvider groupProvider = GroupProviderFactory.createGroupProvider(name, stmt.getPropertyMap());
        groupProvider.init();

        AtomicBoolean published = new AtomicBoolean(false);
        try {
            synchronized (groupProviderDdlLock) {
                if (groupProviderAlreadyExists(name, stmt.isIfNotExists())) {
                    return;
                }

                GlobalStateMgr.getCurrentState().getEditLog().logCreateGroupProvider(
                        new GroupProviderLog(name, stmt.getPropertyMap()),
                        wal -> {
                            nameToGroupProviderMap.put(name, groupProvider);
                            published.set(true);
                        });
            }
        } finally {
            if (!published.get()) {
                // Never made it into the map: tear down whatever init() started instead of leaking it.
                groupProvider.destroy();
            }
        }
    }

    /** True when the name is taken and IF NOT EXISTS makes that acceptable; throws when it is not. */
    private boolean groupProviderAlreadyExists(String name, boolean ifNotExists) throws DdlException {
        if (!nameToGroupProviderMap.containsKey(name)) {
            return false;
        }
        if (ifNotExists) {
            return true;
        }
        throw new DdlException("Group provider '" + name + "' already exists");
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
        synchronized (groupProviderDdlLock) {
            GroupProvider groupProvider = this.nameToGroupProviderMap.get(stmt.getName());
            if (groupProvider == null) {
                if (stmt.isIfExists()) {
                    // If IF EXISTS is specified, silently return without error
                    return;
                } else {
                    throw new DdlException("Group provider '" + stmt.getName() + "' does not exist");
                }
            }

            // Destroy only after the journal write, from inside the applier - the ordering ALTER uses two
            // methods below. Destroying first meant that a failed write left the provider in the map with
            // its refresh schedule already cancelled: a zombie serving a cache that can never refresh again
            // until the FE restarts.
            GlobalStateMgr.getCurrentState().getEditLog().logDropGroupProvider(
                    new GroupProviderLog(stmt.getName(), null),
                    wal -> {
                        GroupProvider removed = nameToGroupProviderMap.remove(stmt.getName());
                        if (removed != null) {
                            removed.destroy();
                        }
                    });
        }
    }

    public void replayDropGroupProvider(String name) {
        GroupProvider groupProvider = this.nameToGroupProviderMap.remove(name);
        if (groupProvider == null) {
            // Not necessarily a bug on this node: replayCreateGroupProvider drops a provider whose init()
            // failed locally (a FileGroupProvider whose group_file_url does not exist here, say), so the
            // name may legitimately be absent. An NPE here would kill the replayer thread and take the FE
            // down over a provider that is already gone.
            LOG.info("group provider '{}' is not present on this node, nothing to drop on replay", name);
            return;
        }
        groupProvider.destroy();
    }

    /**
     * Number of times ALTER redoes merge + validation when another statement changed the provider while
     * this one was talking to the directory. Two is already unusual (group provider DDL is rare and
     * operator-driven); the bound only exists so a pathological retry storm ends with an error the user
     * can act on instead of a statement that never returns.
     */
    private static final int ALTER_GROUP_PROVIDER_MAX_ATTEMPTS = 3;

    public void alterGroupProvider(String name, Map<String, String> alterProps) throws DdlException {
        for (int attempt = 1; ; attempt++) {
            // Phase 1, under the lock: read the current properties and build the new provider from them.
            GroupProvider existing;
            Map<String, String> mergedProps;
            GroupProvider newProvider;
            synchronized (groupProviderDdlLock) {
                existing = nameToGroupProviderMap.get(name);
                if (existing == null) {
                    throw new DdlException("Group Provider '" + name + "' not found");
                }

                Map<String, String> delta;
                try {
                    // Rejects a property this provider type would never read, and normalizes the case of
                    // one that it does. Both matter for the same reason: getters read the map with an
                    // exact get(), so an unnoticed spelling difference would leave the old value in place
                    // while the statement reported success.
                    delta = existing.canonicalizeAlterProperties(alterProps);
                } catch (SemanticException e) {
                    throw new DdlException(e.getMessage(), e);
                }

                // COW: merge the delta onto a copy of the existing properties; the old provider is untouched.
                mergedProps = Maps.newHashMap(existing.getProperties());
                mergedProps.putAll(delta);

                newProvider = GroupProviderFactory.createGroupProvider(name, mergedProps);
                try {
                    newProvider.checkProperty();
                } catch (SemanticException e) {
                    // checkProperty() reports bad input by throwing an unchecked SemanticException, which
                    // would travel past DDLStmtExecutor's StarRocksException handling and surface as
                    // "Maybe our bug or wrong input parameters" with a full stack trace. CREATE reports the
                    // same mistake cleanly because its analyzer pre-validates; convert here so ALTER does too.
                    throw new DdlException(e.getMessage(), e);
                }
            }

            // Phase 2, outside the lock: synchronously validate the new configuration against the directory
            // and warm up its cache. This blocks on network I/O for as long as ldap_conn_timeout allows, so
            // holding the lock here would make an ALTER against an unreachable host park every other group
            // provider statement - including the DROP an operator would reach for to get out of it.
            // On failure this throws and nothing has been touched: the old provider keeps serving.
            newProvider.prepareForActivation();

            // Phase 3, under the lock again: the merge above is only valid if nothing else changed the
            // provider in the meantime, so re-check the base we merged onto before making it official.
            synchronized (groupProviderDdlLock) {
                if (nameToGroupProviderMap.get(name) != existing) {
                    // Another ALTER (or a DROP followed by a CREATE) landed while we were validating. Our
                    // merged map is built on properties that are no longer current, so publishing it would
                    // silently drop that statement's delta. Nothing has been started yet - destroy() only
                    // has to tolerate a provider that never ran init().
                    newProvider.destroy();
                    if (attempt >= ALTER_GROUP_PROVIDER_MAX_ATTEMPTS) {
                        throw new DdlException("Group Provider '" + name + "' is being modified concurrently, "
                                + "gave up after " + attempt + " attempts; please retry");
                    }
                    LOG.info("group provider '{}' changed while ALTER was validating, retrying (attempt {})",
                            name, attempt);
                    // Redo phase 1 on the properties that are current now; this leaves the synchronized
                    // block, so the statement that overtook us is not blocked while we retry.
                    continue;
                }

                // Start the new provider's runtime (e.g. the LDAP refresh schedule). It is not in the map yet, so
                // getGroup() will not see it until the swap below; the old provider keeps serving in the meantime.
                newProvider.init();

                // The record carries the *merged* map, not the delta. A delta would be replayed by merging
                // onto each node's own copy, so one node whose replay failed once would keep merging every
                // later delta onto a stale base and diverge silently - and for a per-node property such as
                // FileGroupProvider's group_file_url, replay really can fail on one node only. Persisting the
                // whole map makes replay idempotent and independent of local state.
                AtomicBoolean swapped = new AtomicBoolean(false);
                try {
                    GlobalStateMgr.getCurrentState().getEditLog().logAlterGroupProvider(
                            new GroupProviderLog(name, mergedProps),
                            wal -> {
                                GroupProvider old = nameToGroupProviderMap.put(name, newProvider);
                                // Latched here, after the swap: logEditGated rethrows whatever the applier
                                // throws, so a failure in old.destroy() below must not make the `finally`
                                // tear down the provider that is already serving every login.
                                swapped.set(true);
                                if (old != null) {
                                    old.destroy();
                                }
                            });
                } finally {
                    if (!swapped.get()) {
                        // The swap never happened, so the new provider is not in the map. Tear down the
                        // runtime it just started and keep the old provider in place.
                        newProvider.destroy();
                    }
                }
                LOG.info("finished to alter group provider '{}' with delta {}", name, maskedProps(alterProps));
                return;
            }
        }
    }

    /**
     * @param properties the provider's <b>complete</b> property map, as journaled by
     *                   {@link #alterGroupProvider}. Replay deliberately does not merge onto this
     *                   node's own copy: a node whose earlier replay failed would then keep merging
     *                   later records onto a stale base and end up with a configuration that is
     *                   wrong rather than merely outdated, with nothing to heal it.
     */
    public void replayAlterGroupProvider(String name, Map<String, String> properties) {
        GroupProvider newProvider = GroupProviderFactory.createGroupProvider(name, Maps.newHashMap(properties));
        GroupProvider previous = nameToGroupProviderMap.get(name);
        if (previous != null) {
            // Replay must not block on network I/O, so unlike the leader this node cannot warm the new
            // instance up before publishing it. Without this the follower would answer every lookup with
            // an empty group set until its first background refresh completes - and indefinitely if it
            // cannot reach the directory - which is exactly the outage ALTER exists to avoid.
            newProvider.inheritCacheFrom(previous);
        }
        try {
            newProvider.init();
        } catch (DdlException e) {
            LOG.error("Failed to replay alter group provider '{}', keeping the old provider", name, e);
            return;
        }
        GroupProvider old = nameToGroupProviderMap.put(name, newProvider);
        if (old != null) {
            old.destroy();
        }
        LOG.info("finished to replay alter group provider '{}' with properties {}", name, maskedProps(properties));
    }

    /**
     * Wraps a property map for logging so that credentials (LDAP bind password, trust store password, ...)
     * are printed as *** instead of ending up in fe.log in plain text.
     */
    private static PrintableMap<String, String> maskedProps(Map<String, String> properties) {
        return new PrintableMap<>(properties, "=", true, false, true);
    }

    public List<GroupProvider> getAllGroupProviders() {
        return new ArrayList<>(nameToGroupProviderMap.values());
    }

    public GroupProvider getGroupProvider(String name) {
        return nameToGroupProviderMap.get(name);
    }
}
