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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/mysql/privilege/Auth.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.mysql.privilege;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.StarRocksFE;
import com.starrocks.analysis.ResourcePattern;
import com.starrocks.analysis.TablePattern;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.authentication.UserPropertyInfo;
import com.starrocks.catalog.AuthorizationInfo;
import com.starrocks.catalog.InfoSchemaDb;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.Pair;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.ImpersonatePrivInfo;
import com.starrocks.persist.PrivInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.sql.ast.RevokeRoleStmt;
import com.starrocks.sql.ast.SetPassVar;
import com.starrocks.sql.ast.SetUserPropertyStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Deprecated
public class Auth implements Writable {
    private static final Logger LOG = LogManager.getLogger(Auth.class);

    // root user's role is operator.
    // each starrocks system has only one root user.
    private static final String ROOT_USER = "root";
    public static final String ADMIN_USER = "admin";

    public static final String KRB5_AUTH_CLASS_NAME = "com.starrocks.plugins.auth.KerberosAuthentication";
    public static final String KRB5_AUTH_JAR_PATH = StarRocksFE.STARROCKS_HOME_DIR + "/lib/starrocks-kerberos.jar";

    private UserPrivTable userPrivTable = new UserPrivTable();
    private DbPrivTable dbPrivTable = new DbPrivTable();
    private TablePrivTable tablePrivTable = new TablePrivTable();
    private ResourcePrivTable resourcePrivTable = new ResourcePrivTable();
    private ImpersonateUserPrivTable impersonateUserPrivTable = new ImpersonateUserPrivTable();

    protected RoleManager roleManager = new RoleManager();
    private UserPropertyMgr propertyMgr = new UserPropertyMgr();

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Class<?> authClazz = null;

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

    public enum PrivLevel {
        GLOBAL, DATABASE, TABLE, RESOURCE
    }

    public Auth() {
        initUser();
    }

    // Reserve this method in the future, only to upgrade from old auth to new RBAC framework
    public UserPrivTable getUserPrivTable() {
        return userPrivTable;
    }

    protected DbPrivTable getDbPrivTable() {
        return dbPrivTable;
    }

    protected TablePrivTable getTablePrivTable() {
        return tablePrivTable;
    }

    protected ResourcePrivTable getResourcePrivTable() {
        return resourcePrivTable;
    }

    protected ImpersonateUserPrivTable getImpersonateUserPrivTable() {
        return impersonateUserPrivTable;
    }

    protected UserPropertyMgr getPropertyMgr() {
        return propertyMgr;
    }

    /**
     * check if role exist, this function can be used in analyze phrase to validate role
     */
    @Deprecated
    public boolean doesRoleExist(String roleName) {
        readLock();
        try {
            return roleManager.getRole(roleName) != null;
        } finally {
            readUnlock();
        }
    }

    private GlobalPrivEntry grantGlobalPrivs(UserIdentity userIdentity, boolean errOnExist, boolean errOnNonExist,
                                             PrivBitSet privs) throws DdlException {
        if (errOnExist && errOnNonExist) {
            throw new DdlException("Can only specified errOnExist or errOnNonExist");
        }
        GlobalPrivEntry entry;
        try {
            // password set here will not overwrite the password of existing entry, no need to worry.
            entry = GlobalPrivEntry.create(userIdentity.getHost(), userIdentity.getQualifiedUser(),
                    userIdentity.isDomain(), new Password(new byte[0]) /* no use */, privs);
            entry.setSetByDomainResolver(false);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        userPrivTable.addEntry(entry, errOnExist, errOnNonExist);
        return entry;
    }

    private void revokeGlobalPrivs(UserIdentity userIdentity, PrivBitSet privs, boolean errOnNonExist)
            throws DdlException {
        GlobalPrivEntry entry;
        try {
            entry = GlobalPrivEntry.create(userIdentity.getHost(), userIdentity.getQualifiedUser(),
                    userIdentity.isDomain(), new Password(new byte[0]) /* no use */, privs);
            entry.setSetByDomainResolver(false);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        userPrivTable.revoke(entry, errOnNonExist,
                false /* not delete entry if priv is empty, because global priv entry has password */);
    }

    private void grantDbPrivs(UserIdentity userIdentity, String db, boolean errOnExist, boolean errOnNonExist,
                              PrivBitSet privs) throws DdlException {
        DbPrivEntry entry;
        try {
            entry = DbPrivEntry.create(userIdentity.getHost(), db, userIdentity.getQualifiedUser(),
                    userIdentity.isDomain(), privs);
            entry.setSetByDomainResolver(false);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        dbPrivTable.addEntry(entry, errOnExist, errOnNonExist);
    }

    private void revokeDbPrivs(UserIdentity userIdentity, String db, PrivBitSet privs, boolean errOnNonExist)
            throws DdlException {
        DbPrivEntry entry;
        try {
            entry = DbPrivEntry.create(userIdentity.getHost(), db, userIdentity.getQualifiedUser(),
                    userIdentity.isDomain(), privs);
            entry.setSetByDomainResolver(false);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        dbPrivTable.revoke(entry, errOnNonExist, true /* delete entry when empty */);
    }

    @Deprecated
    @VisibleForTesting
    public List<String> getRoleNamesByUser(UserIdentity userIdentity) {
        readLock();
        try {
            return roleManager.getRoleNamesByUser(userIdentity);
        } finally {
            readUnlock();
        }
    }

    /**
     * this method merely return the first role name for compatibility
     * TODO fix this after refactoring the whole user privilege framework
     **/
    @Deprecated
    public String getRoleName(UserIdentity userIdentity) {
        List<String> roleNames = getRoleNamesByUser(userIdentity);
        if (roleNames.isEmpty()) {
            return null;
        }
        return roleNames.get(0);
    }

    private void grantTblPrivs(UserIdentity userIdentity, String db, String tbl, boolean errOnExist,
                               boolean errOnNonExist, PrivBitSet privs) throws DdlException {
        TablePrivEntry entry;
        try {
            entry = TablePrivEntry.create(userIdentity.getHost(), db, userIdentity.getQualifiedUser(), tbl,
                    userIdentity.isDomain(), privs);
            entry.setSetByDomainResolver(false);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        tablePrivTable.addEntry(entry, errOnExist, errOnNonExist);
    }

    private void revokeTblPrivs(UserIdentity userIdentity, String db, String tbl, PrivBitSet privs,
                                boolean errOnNonExist) throws DdlException {
        TablePrivEntry entry;
        try {
            entry = TablePrivEntry.create(userIdentity.getHost(), db, userIdentity.getQualifiedUser(), tbl,
                    userIdentity.isDomain(), privs);
            entry.setSetByDomainResolver(false);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        tablePrivTable.revoke(entry, errOnNonExist, true /* delete entry when empty */);
    }

    private void grantResourcePrivs(UserIdentity userIdentity, String resourceName, boolean errOnExist,
                                    boolean errOnNonExist, PrivBitSet privs) throws DdlException {
        ResourcePrivEntry entry;
        try {
            entry = ResourcePrivEntry.create(userIdentity.getHost(), resourceName, userIdentity.getQualifiedUser(),
                    userIdentity.isDomain(), privs);
            entry.setSetByDomainResolver(false);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        resourcePrivTable.addEntry(entry, errOnExist, errOnNonExist);
    }

    private void revokeResourcePrivs(UserIdentity userIdentity, String resourceName, PrivBitSet privs,
                                     boolean errOnNonExist) throws DdlException {
        ResourcePrivEntry entry;
        try {
            entry = ResourcePrivEntry.create(userIdentity.getHost(), resourceName, userIdentity.getQualifiedUser(),
                    userIdentity.isDomain(), privs);
            entry.setSetByDomainResolver(false);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        resourcePrivTable.revoke(entry, errOnNonExist, true /* delete entry when empty */);
    }

    /*
     * check password, if matched, save the userIdentity in matched entry.
     * the following auth checking should use userIdentity saved in currentUser.
     */
    @Deprecated
    public boolean checkPassword(String remoteUser, String remoteHost, byte[] remotePasswd, byte[] randomString,
                                 List<UserIdentity> currentUser) {
        if (!Config.enable_auth_check) {
            return true;
        }
        // TODO: Got no better ways to handle the case that user forgot the password, but to remove this backdoor temporarily.
        // if ((remoteUser.equals(ROOT_USER) || remoteUser.equals(ADMIN_USER)) && remoteHost.equals("127.0.0.1")) {
        //     // root and admin user is allowed to login from 127.0.0.1, in case user forget password.
        //     if (remoteUser.equals(ROOT_USER)) {
        //         currentUser.add(UserIdentity.ROOT);
        //     } else {
        //         currentUser.add(UserIdentity.ADMIN);
        //     }
        //     return true;
        // }
        readLock();
        try {
            return userPrivTable.checkPassword(remoteUser, remoteHost, remotePasswd, randomString, currentUser);
        } finally {
            readUnlock();
        }
    }

    @Deprecated
    public boolean checkPlainPassword(String remoteUser, String remoteHost, String remotePasswd,
                                      List<UserIdentity> currentUser) {
        if (!Config.enable_auth_check) {
            return true;
        }
        readLock();
        try {
            return userPrivTable.checkPlainPassword(remoteUser, remoteHost, remotePasswd, currentUser);
        } finally {
            readUnlock();
        }
    }

    @Deprecated
    public boolean checkGlobalPriv(ConnectContext ctx, PrivPredicate wanted) {
        return checkGlobalPriv(ctx.getCurrentUserIdentity(), wanted);
    }

    @Deprecated
    public boolean checkGlobalPriv(UserIdentity currentUser, PrivPredicate wanted) {
        if (!Config.enable_auth_check) {
            return true;
        }
        PrivBitSet savedPrivs = PrivBitSet.of();
        if (checkGlobalInternal(currentUser, wanted, savedPrivs)) {
            return true;
        }

        LOG.debug("failed to get wanted privs: {}, ganted: {}", wanted, savedPrivs);
        return false;
    }

    @Deprecated
    public boolean checkDbPriv(ConnectContext ctx, String qualifiedDb, PrivPredicate wanted) {
        return checkDbPriv(ctx.getCurrentUserIdentity(), qualifiedDb, wanted);
    }

    /*
     * Check if 'user'@'host' on 'db' has 'wanted' priv.
     * If the given db is null, which means it will no check if database name is matched.
     */
    @Deprecated
    public boolean checkDbPriv(UserIdentity currentUser, String db, PrivPredicate wanted) {
        if (!Config.enable_auth_check) {
            return true;
        }
        if (wanted.getPrivs().containsNodePriv()) {
            LOG.debug("should not check NODE priv in Database level. user: {}, db: {}",
                    currentUser, db);
            return false;
        }

        PrivBitSet savedPrivs = PrivBitSet.of();
        if (checkGlobalInternal(currentUser, wanted, savedPrivs)
                || checkDbInternal(currentUser, db, wanted, savedPrivs)) {
            return true;
        }

        // if user has any privs of table in this db, and the wanted priv is SHOW, return true
        if (db != null && wanted == PrivPredicate.SHOW && checkTblWithDb(currentUser, db)) {
            return true;
        }

        LOG.debug("failed to get wanted privs: {}, ganted: {}", wanted, savedPrivs);
        return false;
    }

    /*
     * User may not have privs on a database, but have privs of tables in this database.
     * So we have to check if user has any privs of tables in this database.
     * if so, the database should be visible to this user.
     */
    private boolean checkTblWithDb(UserIdentity currentUser, String db) {
        readLock();
        try {
            return tablePrivTable.hasPrivsOfDb(currentUser, db);
        } finally {
            readUnlock();
        }
    }

    @Deprecated
    public boolean checkTblPriv(ConnectContext ctx, String qualifiedDb, String tbl, PrivPredicate wanted) {
        return checkTblPriv(ctx.getCurrentUserIdentity(), qualifiedDb, tbl, wanted);
    }

    @Deprecated
    public boolean checkTblPriv(UserIdentity currentUser, String db, String tbl, PrivPredicate wanted) {
        if (!Config.enable_auth_check) {
            return true;
        }
        if (wanted.getPrivs().containsNodePriv()) {
            LOG.debug("should check NODE priv in GLOBAL level. user: {}, db: {}, tbl: {}", currentUser, db, tbl);
            return false;
        }

        PrivBitSet savedPrivs = PrivBitSet.of();
        if (checkGlobalInternal(currentUser, wanted, savedPrivs)
                || checkDbInternal(currentUser, db, wanted, savedPrivs)
                || checkTblInternal(currentUser, db, tbl, wanted, savedPrivs)) {
            return true;
        }

        LOG.debug("failed to get wanted privs: {}, ganted: {}", wanted, savedPrivs);
        return false;
    }

    @Deprecated
    public boolean checkResourcePriv(ConnectContext ctx, String resourceName, PrivPredicate wanted) {
        return checkResourcePriv(ctx.getCurrentUserIdentity(), resourceName, wanted);
    }

    @Deprecated
    public boolean checkResourcePriv(UserIdentity currentUser, String resourceName, PrivPredicate wanted) {
        if (!Config.enable_auth_check) {
            return true;
        }

        PrivBitSet savedPrivs = PrivBitSet.of();
        if (checkGlobalInternal(currentUser, wanted, savedPrivs)
                || checkResourceInternal(currentUser, resourceName, wanted, savedPrivs)) {
            return true;
        }

        LOG.debug("failed to get wanted privs: {}, granted: {}", wanted, savedPrivs);
        return false;
    }

    @Deprecated
    public boolean checkPrivByAuthInfo(ConnectContext ctx, AuthorizationInfo authInfo, PrivPredicate wanted) {
        if (authInfo == null) {
            return false;
        }
        if (authInfo.getDbName() == null) {
            return false;
        }
        if (authInfo.getTableNameList() == null || authInfo.getTableNameList().isEmpty()) {
            return checkDbPriv(ctx, authInfo.getDbName(), wanted);
        }
        for (String tblName : authInfo.getTableNameList()) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(ConnectContext.get(), authInfo.getDbName(),
                    tblName, wanted)) {
                return false;
            }
        }
        return true;
    }

    /**
     * check if `currentUser` has impersonation privilege to execute sql as `toUser`
     */
    @Deprecated
    public boolean canImpersonate(UserIdentity currentUser, UserIdentity toUser) {
        readLock();
        try {
            return impersonateUserPrivTable.canImpersonate(currentUser, toUser);
        } finally {
            readUnlock();
        }
    }

    /*
     * Check if current user has certain privilege.
     * This method will check the given privilege levels
     */
    @Deprecated
    public boolean checkHasPriv(ConnectContext ctx, PrivPredicate priv, PrivLevel... levels) {
        if (!Config.enable_auth_check) {
            return true;
        }
        // currentUser referred to the account that determines user's access privileges.
        return checkHasPrivInternal(ctx.getCurrentUserIdentity(), priv, levels);
    }

    private boolean checkHasPrivInternal(UserIdentity currentUser, PrivPredicate priv, PrivLevel... levels) {
        readLock();
        try {
            for (PrivLevel privLevel : levels) {
                switch (privLevel) {
                    case GLOBAL:
                        if (userPrivTable.hasPriv(currentUser, priv)) {
                            return true;
                        }
                        break;
                    case DATABASE:
                        if (dbPrivTable.hasPriv(currentUser, priv)) {
                            return true;
                        }
                        break;
                    case TABLE:
                        if (tablePrivTable.hasPriv(currentUser, priv)) {
                            return true;
                        }
                        break;
                    default:
                        break;
                }
            }
        } finally {
            readUnlock();
        }
        return false;
    }

    private boolean checkGlobalInternal(UserIdentity currentUser, PrivPredicate wanted, PrivBitSet savedPrivs) {
        readLock();
        try {
            userPrivTable.getPrivs(currentUser, savedPrivs);
            if (Privilege.satisfy(savedPrivs, wanted)) {
                return true;
            }
            return false;
        } finally {
            readUnlock();
        }
    }

    private boolean checkDbInternal(UserIdentity currentUser, String db, PrivPredicate wanted,
                                    PrivBitSet savedPrivs) {
        readLock();
        try {
            dbPrivTable.getPrivs(currentUser, db, savedPrivs);
            if (Privilege.satisfy(savedPrivs, wanted)) {
                return true;
            }
        } finally {
            readUnlock();
        }
        return false;
    }

    private boolean checkTblInternal(UserIdentity currentUser, String db, String tbl,
                                     PrivPredicate wanted, PrivBitSet savedPrivs) {
        readLock();
        try {
            tablePrivTable.getPrivs(currentUser, db, tbl, savedPrivs);
            if (Privilege.satisfy(savedPrivs, wanted)) {
                return true;
            }
            return false;
        } finally {
            readUnlock();
        }
    }

    private boolean checkResourceInternal(UserIdentity currentUser, String resourceName,
                                          PrivPredicate wanted, PrivBitSet savedPrivs) {
        readLock();
        try {
            resourcePrivTable.getPrivs(currentUser, resourceName, savedPrivs);
            if (Privilege.satisfy(savedPrivs, wanted)) {
                return true;
            }
            return false;
        } finally {
            readUnlock();
        }
    }

    // for test only
    @Deprecated
    public void clear() {
        userPrivTable.clear();
        dbPrivTable.clear();
        tablePrivTable.clear();
        resourcePrivTable.clear();
    }

    // create user
    @Deprecated
    public void createUser(CreateUserStmt stmt) throws DdlException {
        AuthPlugin authPlugin = null;
        if (!Strings.isNullOrEmpty(stmt.getAuthPlugin())) {
            authPlugin = AuthPlugin.valueOf(stmt.getAuthPlugin());
        }
        createUserInternal(stmt.getUserIdent(), stmt.getQualifiedRole(),
                new Password(stmt.getPassword(), authPlugin, stmt.getUserForAuthPlugin()), false, stmt.isIfNotExist());
    }

    // alter user
    @Deprecated
    public void alterUser(AlterUserStmt stmt) throws DdlException {
        AuthPlugin authPlugin = null;
        if (!Strings.isNullOrEmpty(stmt.getAuthPlugin())) {
            authPlugin = AuthPlugin.valueOf(stmt.getAuthPlugin());
        }
        // alter user only support change password till now
        setPasswordInternal(stmt.getUserIdent(),
                new Password(stmt.getPassword(), authPlugin, stmt.getUserForAuthPlugin()), null, true, false, false);
    }

    @Deprecated
    public void replayCreateUser(PrivInfo privInfo) {
        try {
            createUserInternal(privInfo.getUserIdent(), privInfo.getRole(), privInfo.getPasswd(), true, false);
        } catch (DdlException e) {
            LOG.error("should not happen", e);
        }
    }

    /*
     * Do following steps:
     * 1. Check whether specified role exist. If not, throw exception.
     * 2. Check whether user already exist. If yes, throw exception.
     * 3. set password for specified user.
     * 4. grant privs of role to user, if role is specified.
     */
    private void createUserInternal(UserIdentity userIdent, String roleName, Password password,
                                    boolean isReplay, boolean isSetIfNotExists) throws DdlException {
        writeLock();
        try {
            // 1. check if role exist
            Role role = null;
            if (roleName != null) {
                role = roleManager.getRole(roleName);
                if (role == null) {
                    throw new DdlException("Role: " + roleName + " does not exist");
                }
            }

            // 2. check if user already exist
            if (userPrivTable.doesUserExist(userIdent)) {
                if (isSetIfNotExists) {
                    LOG.info("create user[{}] who already exists", userIdent);
                    return;
                }
                throw new DdlException("User " + userIdent + " already exist");
            }

            // 3. set password
            setPasswordInternal(userIdent, password, null, false /* err on non exist */,
                    false /* set by resolver */, true /* is replay */);

            // 4. grant privs of role to user
            if (role != null) {
                grantRoleInternal(roleName, userIdent, false, true);
            }

            // other user properties
            propertyMgr.addUserResource(userIdent.getQualifiedUser()  /* not system user */);

            if (!userIdent.getQualifiedUser().equals(ROOT_USER)) {
                // grant read privs to database information_schema
                TablePattern tblPattern = new TablePattern(InfoSchemaDb.DATABASE_NAME, "*");
                try {
                    tblPattern.analyze();
                } catch (AnalysisException e) {
                    LOG.warn("should not happen", e);
                }
                grantInternal(userIdent, null /* role */, tblPattern, PrivBitSet.of(Privilege.SELECT_PRIV),
                        false /* err on non exist */, true /* is replay */);
            }

            if (!isReplay) {
                PrivInfo privInfo = new PrivInfo(userIdent, null, password, roleName);
                GlobalStateMgr.getCurrentState().getEditLog().logCreateUser(privInfo);
            }
            LOG.debug("finished to create user: {}, is replay: {}", userIdent, isReplay);
        } finally {
            writeUnlock();
        }
    }

    // drop user
    @Deprecated
    public void dropUser(DropUserStmt stmt) throws DdlException {
        String user = stmt.getUserIdentity().getQualifiedUser();
        String host = stmt.getUserIdentity().getHost();
        if (ROOT_USER.equals(user) && "%".equals(host)) {
            // Dropping `root@%` is not allowed
            throw new DdlException(String.format("User `%s`@`%s` is not allowed to be dropped.", user, host));
        }

        writeLock();
        try {
            if (!doesUserExist(stmt.getUserIdentity())) {
                throw new DdlException(String.format("User `%s`@`%s` does not exist.", user, host));
            }
            dropUserInternal(stmt.getUserIdentity(), false);
        } finally {
            writeUnlock();
        }
    }

    @Deprecated
    public void replayDropUser(UserIdentity userIdent) {
        dropUserInternal(userIdent, true);
    }

    private void dropUserInternal(UserIdentity userIdent, boolean isReplay) {
        writeLock();
        try {
            // we don't check if user exists
            userPrivTable.dropUser(userIdent);
            dbPrivTable.dropUser(userIdent);
            tablePrivTable.dropUser(userIdent);
            resourcePrivTable.dropUser(userIdent);
            // drop user in roles if exist
            roleManager.dropUser(userIdent);

            if (!userPrivTable.doesUsernameExist(userIdent.getQualifiedUser())) {
                // if username does not exist in userPrivTable, which means all userIdent with this name
                // has been remove, then we can drop this user from property manager
                propertyMgr.dropUser(userIdent);
            } else if (userIdent.isDomain()) {
                // if there still has entry with this username, we can not drop user from property map,
                // but we need to remove the specified domain from this user.
                propertyMgr.removeDomainFromUser(userIdent);
            }

            if (!isReplay) {
                GlobalStateMgr.getCurrentState().getEditLog().logNewDropUser(userIdent);
            }
            LOG.info("finished to drop user: {}, is replay: {}", userIdent.getQualifiedUser(), isReplay);
        } finally {
            writeUnlock();
        }
    }

    @Deprecated
    public void grantRole(GrantRoleStmt stmt) throws DdlException {
        writeLock();
        try {
            grantRoleInternal(stmt.getGranteeRole().get(0), stmt.getUserIdent(), true, false);
        } finally {
            writeUnlock();
        }
    }

    @Deprecated
    public void replayGrantRole(PrivInfo privInfo) throws DdlException {
        writeLock();
        try {
            grantRoleInternal(privInfo.getRole(), privInfo.getUserIdent(), true, true);
        } finally {
            writeUnlock();
        }
    }


    /**
     * simply copy all privileges map from role to user.
     * TODO this is a temporary implement that make it impossible to safely revoke privilege from role.
     * We will refactor the whole user privilege framework later to ultimately fix this.
     *
     * @param roleName
     * @param userIdent
     * @throws DdlException
     */
    private void grantRoleInternal(String roleName, UserIdentity userIdent, boolean errOnNonExist, boolean isReplay)
            throws DdlException {
        Role role = roleManager.getRole(roleName);
        if (role == null) {
            throw new DdlException("Role: " + roleName + " does not exist");
        }
        for (Map.Entry<TablePattern, PrivBitSet> entry : role.getTblPatternToPrivs().entrySet()) {
            // use PrivBitSet copy to avoid same object being changed synchronously
            grantInternal(userIdent, null /* role */, entry.getKey(), entry.getValue().copy(),
                    errOnNonExist /* err on non exist */, true /* is replay */);
        }
        for (Map.Entry<ResourcePattern, PrivBitSet> entry : role.getResourcePatternToPrivs().entrySet()) {
            // use PrivBitSet copy to avoid same object being changed synchronously
            grantInternal(userIdent, null /* role */, entry.getKey(), entry.getValue().copy(),
                    errOnNonExist /* err on non exist */, true /* is replay */);
        }
        for (UserIdentity user : role.getImpersonateUsers()) {
            grantImpersonateToUserInternal(userIdent, user, true);
        }

        role.addUser(userIdent);
        if (!isReplay) {
            PrivInfo privInfo = new PrivInfo(userIdent, role.getRoleName());
            GlobalStateMgr.getCurrentState().getEditLog().logGrantRole(privInfo);
        }
        LOG.info("grant {} to {}, isReplay = {}", roleName, userIdent, isReplay);
    }

    @Deprecated
    public void revokeRole(RevokeRoleStmt stmt) throws DdlException {
        writeLock();
        try {
            revokeRoleInternal(stmt.getGranteeRole().get(0), stmt.getUserIdent(), false);
        } finally {
            writeUnlock();
        }
    }

    @Deprecated
    public void replayRevokeRole(PrivInfo privInfo) throws DdlException {
        writeLock();
        try {
            revokeRoleInternal(privInfo.getRole(), privInfo.getUserIdent(), true);
        } finally {
            writeUnlock();
        }
    }

    /**
     * simply remove all privileges of a role from user.
     * TODO this is a temporary implement that make it impossible to safely revoke privilege from role.
     * We will refactor the whole user privilege framework later to ultimately fix this.
     *
     * @param roleName
     * @param userIdent
     * @throws DdlException
     */
    private void revokeRoleInternal(String roleName, UserIdentity userIdent, boolean isReplay) throws DdlException {
        Role role = roleManager.getRole(roleName);
        if (role == null) {
            throw new DdlException("Role: " + roleName + " does not exist");
        }
        for (Map.Entry<TablePattern, PrivBitSet> entry : role.getTblPatternToPrivs().entrySet()) {
            revokeInternal(userIdent, null /* role */, entry.getKey(), entry.getValue().copy(),
                    false /* err on non exist */, true /* isReplay */);
        }
        for (Map.Entry<ResourcePattern, PrivBitSet> entry : role.getResourcePatternToPrivs().entrySet()) {
            revokeInternal(userIdent, null /* role */, entry.getKey(), entry.getValue().copy(),
                    false /* err on non exist */, true /* isReplay */);
        }
        for (UserIdentity user : role.getImpersonateUsers()) {
            revokeImpersonateFromUserInternal(userIdent, user, true);
        }

        role.dropUser(userIdent);
        if (!isReplay) {
            PrivInfo privInfo = new PrivInfo(userIdent, role.getRoleName());
            GlobalStateMgr.getCurrentState().getEditLog().logRevokeRole(privInfo);
        }
        LOG.info("revoke {} from {}, isReplay = {}", roleName, userIdent, isReplay);
    }

    @Deprecated
    public void replayGrantImpersonate(ImpersonatePrivInfo info) {
        try {
            if (info.getAuthorizedRoleName() == null) {
                grantImpersonateToUserInternal(info.getAuthorizedUser(), info.getSecuredUser(), true);
            } else {
                grantImpersonateToRoleInternal(info.getAuthorizedRoleName(), info.getSecuredUser(), true);
            }
        } catch (DdlException e) {
            LOG.error("should not happend", e);
        }
    }

    @Deprecated
    public void replayRevokeImpersonate(ImpersonatePrivInfo info) {
        try {
            if (info.getAuthorizedRoleName() == null) {
                revokeImpersonateFromUserInternal(info.getAuthorizedUser(), info.getSecuredUser(), true);
            } else {
                revokeImpersonateFromRoleInternal(info.getAuthorizedRoleName(), info.getSecuredUser(), true);
            }
        } catch (DdlException e) {
            LOG.error("should not happend", e);
        }
    }

    // grant
    @Deprecated
    public void grant(GrantPrivilegeStmt stmt) throws DdlException {
        PrivBitSet privs = stmt.getPrivBitSet();
        if (stmt.getPrivType().equals("TABLE") || stmt.getPrivType().equals("DATABASE")) {
            grantInternal(stmt.getUserIdentity(), stmt.getRole(), stmt.getTblPattern(), privs, true, false);
        } else if (stmt.getPrivType().equals("RESOURCE")) {
            grantInternal(stmt.getUserIdentity(), stmt.getRole(), stmt.getResourcePattern(), privs, true, false);
        } else {
            if (stmt.getRole() == null) {
                grantImpersonateToUserInternal(stmt.getUserIdentity(), stmt.getUserPrivilegeObject(), false);
            } else {
                grantImpersonateToRoleInternal(stmt.getRole(), stmt.getUserPrivilegeObject(), false);
            }
        }
    }

    @Deprecated
    public void replayGrant(PrivInfo privInfo) {
        try {
            if (privInfo.getTblPattern() != null) {
                grantInternal(privInfo.getUserIdent(), privInfo.getRole(),
                        privInfo.getTblPattern(), privInfo.getPrivs(),
                        true /* err on non exist */, true /* is replay */);
            } else {
                grantInternal(privInfo.getUserIdent(), privInfo.getRole(),
                        privInfo.getResourcePattern(), privInfo.getPrivs(),
                        true /* err on non exist */, true /* is replay */);
            }
        } catch (DdlException e) {
            LOG.error("should not happen", e);
        }
    }

    private void grantInternal(UserIdentity userIdent, String role, TablePattern tblPattern,
                               PrivBitSet privs, boolean errOnNonExist, boolean isReplay)
            throws DdlException {

        if (!isReplay) {
            // check privilege only on leader, in case of replaying old journal
            switch (tblPattern.getPrivLevel()) {
                case DATABASE:
                    if (privs.containsNodePriv() || privs.containsResourcePriv() || privs.containsImpersonatePriv()) {
                        throw new DdlException("Some of the privileges are not for database: " + privs);
                    }
                    break;

                case TABLE:
                    if (privs.containsNodePriv() || privs.containsResourcePriv() || privs.containsImpersonatePriv()) {
                        throw new DdlException("Some of the privileges are not for table: " + privs);
                    }
                    break;

                default:
                    break;
            }
        }
        writeLock();
        try {
            if (role != null) {
                // grant privs to role, role must exist
                Role newRole = new Role(role, tblPattern, privs);
                Role existingRole = roleManager.addRole(newRole, false /* err on exist */);

                // update users' privs of this role
                for (UserIdentity user : existingRole.getUsers()) {
                    for (Map.Entry<TablePattern, PrivBitSet> entry : existingRole.getTblPatternToPrivs().entrySet()) {
                        // copy the PrivBitSet
                        grantPrivs(user, entry.getKey(), entry.getValue().copy(), errOnNonExist);
                    }
                }
            } else {
                grantPrivs(userIdent, tblPattern, privs, errOnNonExist);
            }

            if (!isReplay) {
                PrivInfo info = new PrivInfo(userIdent, tblPattern, privs, null, role);
                GlobalStateMgr.getCurrentState().getEditLog().logGrantPriv(info);
            }
            LOG.debug("finished to grant privilege. is replay: {}", isReplay);
        } finally {
            writeUnlock();
        }
    }

    private void grantInternal(UserIdentity userIdent, String role, ResourcePattern resourcePattern, PrivBitSet privs,
                               boolean errOnNonExist, boolean isReplay) throws DdlException {
        if (!isReplay) {
            // check privilege only on leader, in case of replaying old journal
            switch (resourcePattern.getPrivLevel()) {
                case RESOURCE:
                    if (privs.containsNodePriv() || privs.containsDbTablePriv() || privs.containsImpersonatePriv()) {
                        throw new DdlException("Some of the privileges are not for resource: " + privs);
                    }
                    break;
                default:
                    break;
            }
        }

        writeLock();
        try {
            if (role != null) {
                // grant privs to role, role must exist
                Role newRole = new Role(role, resourcePattern, privs);
                Role existingRole = roleManager.addRole(newRole, false /* err on exist */);

                // update users' privs of this role
                for (UserIdentity user : existingRole.getUsers()) {
                    for (Map.Entry<ResourcePattern, PrivBitSet> entry : existingRole.getResourcePatternToPrivs()
                            .entrySet()) {
                        // copy the PrivBitSet
                        grantPrivs(user, entry.getKey(), entry.getValue().copy(), errOnNonExist);
                    }
                }
            } else {
                grantPrivs(userIdent, resourcePattern, privs, errOnNonExist);
            }

            if (!isReplay) {
                PrivInfo info = new PrivInfo(userIdent, resourcePattern, privs, null, role);
                GlobalStateMgr.getCurrentState().getEditLog().logGrantPriv(info);
            }
            LOG.debug("finished to grant resource privilege. is replay: {}", isReplay);
        } finally {
            writeUnlock();
        }
    }

    @Deprecated
    public void grantPrivs(UserIdentity userIdent, TablePattern tblPattern, PrivBitSet privs,
                           boolean errOnNonExist) throws DdlException {
        LOG.debug("grant {} on {} to {}, err on non exist: {}", privs, tblPattern, userIdent, errOnNonExist);

        writeLock();
        try {
            // check if user identity already exist
            if (errOnNonExist && !doesUserExist(userIdent)) {
                throw new DdlException("user " + userIdent + " does not exist");
            }

            // grant privs to user
            switch (tblPattern.getPrivLevel()) {
                case GLOBAL:
                    grantGlobalPrivs(userIdent,
                            false /* err on exist */,
                            errOnNonExist,
                            privs);
                    break;
                case DATABASE:
                    grantDbPrivs(userIdent, tblPattern.getQuolifiedDb(),
                            false /* err on exist */,
                            false /* err on non exist */,
                            privs);
                    break;
                case TABLE:
                    grantTblPrivs(userIdent, tblPattern.getQuolifiedDb(),
                            tblPattern.getTbl(),
                            false /* err on exist */,
                            false /* err on non exist */,
                            privs);
                    break;
                default:
                    Preconditions.checkNotNull(null, tblPattern.getPrivLevel());
            }
        } finally {
            writeUnlock();
        }
    }

    @Deprecated
    public void grantPrivs(UserIdentity userIdent, ResourcePattern resourcePattern, PrivBitSet privs,
                           boolean errOnNonExist) throws DdlException {
        LOG.debug("grant {} on resource {} to {}, err on non exist: {}", privs, resourcePattern, userIdent,
                errOnNonExist);

        writeLock();
        try {
            // check if user identity already exist
            if (errOnNonExist && !doesUserExist(userIdent)) {
                throw new DdlException("user " + userIdent + " does not exist");
            }

            // grant privs to user
            switch (resourcePattern.getPrivLevel()) {
                case GLOBAL:
                    grantGlobalPrivs(userIdent, false, errOnNonExist, privs);
                    break;
                case RESOURCE:
                    grantResourcePrivs(userIdent, resourcePattern.getResourceName(), false, false, privs);
                    break;
                default:
                    Preconditions.checkNotNull(null, resourcePattern.getPrivLevel());
            }
        } finally {
            writeUnlock();
        }
    }

    private void grantImpersonateToUserInternal(
            UserIdentity authorizedUser, UserIdentity securedUser, boolean isReplay) throws DdlException {
        writeLock();
        try {
            ImpersonateUserPrivEntry entry = ImpersonateUserPrivEntry.create(authorizedUser, securedUser);
            entry.setSetByDomainResolver(false);
            impersonateUserPrivTable.addEntry(entry, false, false);

            if (!isReplay) {
                ImpersonatePrivInfo info = new ImpersonatePrivInfo(authorizedUser, securedUser);
                GlobalStateMgr.getCurrentState().getEditLog().logGrantImpersonate(info);
            }
            LOG.info("grant impersonate on {} to {}, isReplay = {}", securedUser, authorizedUser, isReplay);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        } finally {
            writeUnlock();
        }
    }

    private void grantImpersonateToRoleInternal(
            String authorizedRoleName, UserIdentity securedUser, boolean isReplay) throws DdlException {
        writeLock();
        try {
            // grant privs to role, role must exist
            Role newRole = new Role(authorizedRoleName, securedUser);
            Role existingRole = roleManager.addRole(newRole, false /* err on exist */);

            // update users' privs of this role
            for (UserIdentity user : existingRole.getUsers()) {
                grantImpersonateToUserInternal(user, securedUser, true);
            }

            if (!isReplay) {
                ImpersonatePrivInfo info = new ImpersonatePrivInfo(authorizedRoleName, securedUser);
                GlobalStateMgr.getCurrentState().getEditLog().logGrantImpersonate(info);
            }
            LOG.info("grant impersonate on {} to role {}, isReplay = {}", securedUser, authorizedRoleName, isReplay);
        } finally {
            writeUnlock();
        }
    }

    // return true if user ident exist
    @Deprecated
    public boolean doesUserExist(UserIdentity userIdent) {
        if (userIdent.isDomain()) {
            return propertyMgr.doesUserExist(userIdent);
        } else {
            return userPrivTable.doesUserExist(userIdent);
        }
    }

    // revoke
    @Deprecated
    public void revoke(RevokePrivilegeStmt stmt) throws DdlException {
        PrivBitSet privs = stmt.getPrivBitSet();
        if (stmt.getPrivType().equals("TABLE")) {
            revokeInternal(stmt.getUserIdentity(), stmt.getRole(), stmt.getTblPattern(), privs,
                    true /* err on non exist */, false /* is replay */);
        } else if (stmt.getPrivType().equals("RESOURCE")) {
            revokeInternal(stmt.getUserIdentity(), stmt.getRole(), stmt.getResourcePattern(), privs,
                    true /* err on non exist */, false /* is replay */);
        } else {
            if (stmt.getRole() == null) {
                revokeImpersonateFromUserInternal(stmt.getUserIdentity(), stmt.getUserPrivilegeObject(), false);
            } else {
                revokeImpersonateFromRoleInternal(stmt.getRole(), stmt.getUserPrivilegeObject(), false);
            }
        }
    }

    @Deprecated
    public void replayRevoke(PrivInfo info) {
        try {
            if (info.getTblPattern() != null) {
                revokeInternal(info.getUserIdent(), info.getRole(), info.getTblPattern(), info.getPrivs(),
                        true /* err on non exist */, true /* is replay */);
            } else {
                revokeInternal(info.getUserIdent(), info.getRole(), info.getResourcePattern(), info.getPrivs(),
                        true /* err on non exist */, true /* is replay */);
            }
        } catch (DdlException e) {
            LOG.error("should not happend", e);
        }
    }

    private void revokeInternal(UserIdentity userIdent, String role, TablePattern tblPattern,
                                PrivBitSet privs, boolean errOnNonExist, boolean isReplay) throws DdlException {
        writeLock();
        try {
            if (role != null) {
                // revoke privs from role
                Role existingRole = roleManager.revokePrivs(role, tblPattern, privs, errOnNonExist);
                if (existingRole != null) {
                    // revoke privs from users of this role
                    for (UserIdentity user : existingRole.getUsers()) {
                        revokePrivs(user, tblPattern, privs, false /* err on non exist */);
                    }
                }
            } else {
                revokePrivs(userIdent, tblPattern, privs, errOnNonExist);
            }

            if (!isReplay) {
                PrivInfo info = new PrivInfo(userIdent, tblPattern, privs, null, role);
                GlobalStateMgr.getCurrentState().getEditLog().logRevokePriv(info);
            }
            LOG.info("finished to revoke privilege. is replay: {}", isReplay);
        } finally {
            writeUnlock();
        }
    }

    private void revokeInternal(UserIdentity userIdent, String role, ResourcePattern resourcePattern,
                                PrivBitSet privs, boolean errOnNonExist, boolean isReplay) throws DdlException {
        writeLock();
        try {
            if (role != null) {
                // revoke privs from role
                Role existingRole = roleManager.revokePrivs(role, resourcePattern, privs, errOnNonExist);
                if (existingRole != null) {
                    // revoke privs from users of this role
                    for (UserIdentity user : existingRole.getUsers()) {
                        revokePrivs(user, resourcePattern, privs, false /* err on non exist */);
                    }
                }
            } else {
                revokePrivs(userIdent, resourcePattern, privs, errOnNonExist);
            }

            if (!isReplay) {
                PrivInfo info = new PrivInfo(userIdent, resourcePattern, privs, null, role);
                GlobalStateMgr.getCurrentState().getEditLog().logRevokePriv(info);
            }
            LOG.info("finished to revoke privilege. is replay: {}", isReplay);
        } finally {
            writeUnlock();
        }
    }

    @Deprecated
    public void revokePrivs(UserIdentity userIdent, TablePattern tblPattern, PrivBitSet privs,
                            boolean errOnNonExist) throws DdlException {
        writeLock();
        try {
            switch (tblPattern.getPrivLevel()) {
                case GLOBAL:
                    revokeGlobalPrivs(userIdent, privs, errOnNonExist);
                    break;
                case DATABASE:
                    revokeDbPrivs(userIdent, tblPattern.getQuolifiedDb(), privs, errOnNonExist);
                    break;
                case TABLE:
                    revokeTblPrivs(userIdent, tblPattern.getQuolifiedDb(), tblPattern.getTbl(), privs,
                            errOnNonExist);
                    break;
                default:
                    Preconditions.checkNotNull(null, tblPattern.getPrivLevel());
            }
        } finally {
            writeUnlock();
        }
    }

    @Deprecated
    public void revokePrivs(UserIdentity userIdent, ResourcePattern resourcePattern, PrivBitSet privs,
                            boolean errOnNonExist) throws DdlException {
        writeLock();
        try {
            switch (resourcePattern.getPrivLevel()) {
                case GLOBAL:
                    revokeGlobalPrivs(userIdent, privs, errOnNonExist);
                    break;
                case RESOURCE:
                    revokeResourcePrivs(userIdent, resourcePattern.getResourceName(), privs, errOnNonExist);
                    break;
            }
        } finally {
            writeUnlock();
        }
    }

    private void revokeImpersonateFromUserInternal(
            UserIdentity authorizedUser, UserIdentity securedUser, boolean isReplay) throws DdlException {
        writeLock();
        try {
            ImpersonateUserPrivEntry entry = ImpersonateUserPrivEntry.create(authorizedUser, securedUser);
            entry.setSetByDomainResolver(false);
            impersonateUserPrivTable.revoke(entry, false, true);

            if (!isReplay) {
                ImpersonatePrivInfo info = new ImpersonatePrivInfo(authorizedUser, securedUser);
                GlobalStateMgr.getCurrentState().getEditLog().logRevokeImpersonate(info);
            }
            LOG.info("revoke impersonate on {} from {}. is replay: {}", securedUser, authorizedUser, isReplay);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        } finally {
            writeUnlock();
        }
    }

    private void revokeImpersonateFromRoleInternal(
            String authorizedRoleName, UserIdentity securedUser, boolean isReplay) throws DdlException {
        writeLock();
        try {
            // revoke privs from role, role must exist
            Role existingRole = roleManager.revokePrivs(authorizedRoleName, securedUser);

            // revoke privs from users of this role
            for (UserIdentity user : existingRole.getUsers()) {
                revokeImpersonateFromUserInternal(user, securedUser, true);
            }

            if (!isReplay) {
                ImpersonatePrivInfo info = new ImpersonatePrivInfo(authorizedRoleName, securedUser);
                GlobalStateMgr.getCurrentState().getEditLog().logRevokeImpersonate(info);
            }
            LOG.debug("revoke impersonate on {} from role {}. is replay: {}", securedUser, authorizedRoleName, isReplay);

        } finally {
            writeUnlock();
        }
    }

    /**
     * check password complexity if `enable_validate_password` is set
     * only check for plain text
     **/
    @Deprecated
    public static void validatePassword(String password) throws DdlException {
        if (!Config.enable_validate_password) {
            return;
        }

        //  1. The length of the password should be no less than 8.
        if (password.length() < 8) {
            throw new DdlException("password is too short!");
        }

        // 2. The password should contain at least one digit, one lowercase letter, one uppercase letter
        boolean hasDigit = false;
        boolean hasUpper = false;
        boolean hasLower = false;
        for (int i = 0; i != password.length(); ++i) {
            char c = password.charAt(i);
            if (c >= '0' && c <= '9') {
                hasDigit = true;
            } else if (c >= 'A' && c <= 'Z') {
                hasUpper = true;
            } else if (c >= 'a' && c <= 'z') {
                hasLower = true;
            }
        }
        if (!hasDigit || !hasLower || !hasUpper) {
            throw new DdlException("password should contains at least one digit, one lowercase letter and one uppercase letter!");
        }
    }

    /**
     * prevent password reuse if `enable_password_reuse` is set;
     * only check for plain text
     */
    @Deprecated
    public void checkPasswordReuse(UserIdentity user, String plainPassword) throws DdlException {
        if (Config.enable_password_reuse) {
            return;
        }
        List<UserIdentity> userList = Lists.newArrayList();
        if (checkPlainPassword(user.getQualifiedUser(), user.getHost(), plainPassword, userList)) {
            throw new DdlException("password should not be the same as the previous one!");
        }
    }

    // set password
    @Deprecated
    public void setPassword(SetPassVar stmt) throws DdlException {
        Password passwordToSet = new Password(stmt.getPassword());
        Password currentPassword = userPrivTable.getPassword(stmt.getUserIdent());
        if (currentPassword != null) {
            passwordToSet.setAuthPlugin(currentPassword.getAuthPlugin());
            passwordToSet.setUserForAuthPlugin(currentPassword.getUserForAuthPlugin());
        }
        setPasswordInternal(stmt.getUserIdent(), passwordToSet, null, true /* err on non exist */,
                false /* set by resolver */, false);
    }

    @Deprecated
    public void replaySetPassword(PrivInfo info) {
        try {
            setPasswordInternal(info.getUserIdent(), info.getPasswd(), null, true /* err on non exist */,
                    false /* set by resolver */, true);
        } catch (DdlException e) {
            LOG.error("should not happend", e);
        }
    }

    @Deprecated
    public void setPasswordInternal(UserIdentity userIdent, Password password, UserIdentity domainUserIdent,
                                    boolean errOnNonExist, boolean setByResolver, boolean isReplay)
            throws DdlException {
        Preconditions.checkArgument(!setByResolver || domainUserIdent != null, setByResolver + ", " + domainUserIdent);
        writeLock();
        try {
            if (userIdent.isDomain()) {
                // throw exception if this user already contains this domain
                propertyMgr.setPasswordForDomain(userIdent, password.getPassword(), true /* err on exist */,
                        errOnNonExist /* err on non exist */);
            } else {
                GlobalPrivEntry passwdEntry;
                try {
                    passwdEntry = GlobalPrivEntry.create(userIdent.getHost(), userIdent.getQualifiedUser(),
                            userIdent.isDomain(), password, PrivBitSet.of());
                    passwdEntry.setSetByDomainResolver(setByResolver);
                    if (setByResolver) {
                        Preconditions.checkNotNull(domainUserIdent);
                        passwdEntry.setDomainUserIdent(domainUserIdent);
                    }
                } catch (AnalysisException e) {
                    throw new DdlException(e.getMessage());
                }
                userPrivTable.setPassword(passwdEntry, errOnNonExist);
            }

            if (!isReplay) {
                PrivInfo info = new PrivInfo(userIdent, null, password, null);
                GlobalStateMgr.getCurrentState().getEditLog().logSetPassword(info);
            }
        } finally {
            writeUnlock();
        }
        LOG.debug("finished to set password for {}. is replay: {}", userIdent, isReplay);
    }

    // create role
    @Deprecated
    public void createRole(CreateRoleStmt stmt) throws DdlException {
        createRoleInternal(stmt.getQualifiedRole(), false);
    }

    @Deprecated
    public void replayCreateRole(PrivInfo info) {
        try {
            createRoleInternal(info.getRole(), true);
        } catch (DdlException e) {
            LOG.error("should not happend", e);
        }
    }

    private void createRoleInternal(String role, boolean isReplay) throws DdlException {
        Role emptyPrivsRole = new Role(role);
        writeLock();
        try {
            roleManager.addRole(emptyPrivsRole, true /* err on exist */);

            if (!isReplay) {
                PrivInfo info = new PrivInfo(null, null, null, role);
                GlobalStateMgr.getCurrentState().getEditLog().logCreateRole(info);
            }
        } finally {
            writeUnlock();
        }
        LOG.info("finished to create role: {}, is replay: {}", role, isReplay);
    }

    // drop role
    @Deprecated
    public void dropRole(DropRoleStmt stmt) throws DdlException {
        dropRoleInternal(stmt.getQualifiedRole(), false);
    }

    @Deprecated
    public void replayDropRole(PrivInfo info) {
        try {
            dropRoleInternal(info.getRole(), true);
        } catch (DdlException e) {
            LOG.error("should not happend", e);
        }
    }

    private void dropRoleInternal(String role, boolean isReplay) throws DdlException {
        writeLock();
        try {
            roleManager.dropRole(role, true /* err on non exist */);

            if (!isReplay) {
                PrivInfo info = new PrivInfo(null, null, null, role);
                GlobalStateMgr.getCurrentState().getEditLog().logDropRole(info);
            }
        } finally {
            writeUnlock();
        }
        LOG.info("finished to drop role: {}, is replay: {}", role, isReplay);
    }

    // update user property
    @Deprecated
    public void updateUserProperty(SetUserPropertyStmt stmt) throws DdlException {
        List<Pair<String, String>> properties = stmt.getPropertyPairList();
        updateUserPropertyInternal(stmt.getUser(), properties, false /* is replay */);
    }

    @Deprecated
    public void replayUpdateUserProperty(UserPropertyInfo propInfo) throws DdlException {
        updateUserPropertyInternal(propInfo.getUser(), propInfo.getProperties(), true /* is replay */);
    }

    @Deprecated
    public void updateUserPropertyInternal(String user, List<Pair<String, String>> properties, boolean isReplay)
            throws DdlException {
        writeLock();
        try {
            propertyMgr.updateUserProperty(user, properties, isReplay);
            if (!isReplay) {
                UserPropertyInfo propertyInfo = new UserPropertyInfo(user, properties);
                GlobalStateMgr.getCurrentState().getEditLog().logUpdateUserProperty(propertyInfo);
            }
            LOG.info("finished to set properties for user: {}", user);
        } finally {
            writeUnlock();
        }
    }

    @Deprecated
    public long getMaxConn(String qualifiedUser) {
        readLock();
        try {
            return propertyMgr.getMaxConn(qualifiedUser);
        } finally {
            readUnlock();
        }
    }

    @Deprecated
    public void getAllDomains(Set<String> allDomains) {
        readLock();
        try {
            propertyMgr.getAllDomains(allDomains);
        } finally {
            readUnlock();
        }
    }

    @Deprecated
    public List<DbPrivEntry> getDBPrivEntries(UserIdentity userIdent) {
        List<DbPrivEntry> dbPrivs = Lists.newArrayList();
        readLock();
        try {
            List<PrivEntry> entryList = dbPrivTable.map.get(userIdent);
            if (entryList != null) {
                for (PrivEntry entry : entryList) {
                    dbPrivs.add((DbPrivEntry) entry);
                }
            }
        } finally {
            readUnlock();
        }
        return dbPrivs;
    }

    @Deprecated
    public List<TablePrivEntry> getTablePrivEntries(UserIdentity userIdent) {
        List<TablePrivEntry> tablePrivs = Lists.newArrayList();
        readLock();
        try {
            List<PrivEntry> entryList = tablePrivTable.map.get(userIdent);
            if (entryList != null) {
                for (PrivEntry entry : entryList) {
                    tablePrivs.add((TablePrivEntry) entry);
                }
            }
        } finally {
            readUnlock();
        }
        return tablePrivs;
    }

    // refresh all priv entries set by domain resolver.
    // 1. delete all priv entries in user priv table which are set by domain resolver previously.
    // 2. add priv entries by new resolving IPs
    @Deprecated
    public void refreshUserPrivEntriesByResolvedIPs(Map<String, Set<String>> resolvedIPsMap) {
        writeLock();
        try {
            // 1. delete all previously set entries
            userPrivTable.clearEntriesSetByResolver();
            // 2. add new entries
            propertyMgr.addUserPrivEntriesByResolvedIPs(resolvedIPsMap);
        } finally {
            writeUnlock();
        }
    }

    /**
     * get all `GRANT XX ON XX TO XX` SQLs
     */
    @Deprecated
    public List<List<String>> getGrantsSQLs(UserIdentity currentUser) {
        List<List<String>> ret = Lists.newArrayList();

        // 1. get all possible users
        Set<UserIdentity> identities;
        if (currentUser != null) {
            identities = new HashSet<>();
            identities.add(currentUser);
        } else {
            identities = getAllUserIdents(false);
        }

        // 2. loop for grants SQL
        List<PrivTable> allTables = Arrays.asList(
                userPrivTable, dbPrivTable, tablePrivTable, resourcePrivTable, impersonateUserPrivTable);
        for (UserIdentity userIdentity : identities) {
            List<String> line = Lists.newArrayList();
            line.add(userIdentity.toString());

            // loop all privilege tables
            List<String> allSQLs = new ArrayList<>();
            for (PrivTable table : allTables) {
                Iterator<PrivEntry> iter = table.getReadOnlyIteratorByUser(userIdentity);
                while (iter.hasNext()) {
                    String sql = iter.next().toGrantSQL();
                    if (sql != null) {
                        allSQLs.add(sql);
                    }
                } // for entity
            } // for table
            line.add(String.join("\n", allSQLs));
            ret.add(line);
        }
        return ret;
    }

    // return the auth info of specified user, or infos of all users, if user is not specified.
    // the returned columns are defined in AuthProcDir
    // the specified user identity should be the identity created by CREATE USER, same as result of
    // SELECT CURRENT_USER();
    @Deprecated
    public List<List<String>> getAuthInfo(UserIdentity specifiedUserIdent) {
        List<List<String>> userAuthInfos = Lists.newArrayList();
        readLock();
        try {
            if (specifiedUserIdent == null) {
                // get all users' auth info
                Set<UserIdentity> userIdents = getAllUserIdents(false /* include entry set by resolver */);
                for (UserIdentity userIdent : userIdents) {
                    getUserAuthInfo(userAuthInfos, userIdent);
                }
            } else {
                getUserAuthInfo(userAuthInfos, specifiedUserIdent);
            }
        } finally {
            readUnlock();
        }
        return userAuthInfos;
    }

    @Deprecated
    public List<List<String>> getAuthenticationInfo(UserIdentity specifiedUserIdent) {
        List<List<String>> userAuthInfos = Lists.newArrayList();
        readLock();
        try {
            Set<UserIdentity> userIdents;
            if (specifiedUserIdent == null) {
                userIdents = getAllUserIdents(false /* include entry set by resolver */);
            } else {
                userIdents = new HashSet<>();
                userIdents.add(specifiedUserIdent);
            }

            for (UserIdentity userIdent : userIdents) {
                List<String> userAuthInfo = Lists.newArrayList();
                getUserGlobalPrivs(userAuthInfo, userIdent, true);
                userAuthInfos.add(userAuthInfo);
            }
        } finally {
            readUnlock();
        }
        return userAuthInfos;
    }

    private void getUserGlobalPrivs(List<String> userAuthInfo, UserIdentity userIdent, boolean onlyAuthenticationInfo) {
        List<PrivEntry> userPrivEntries = userPrivTable.map.get(userIdent);
        if (userPrivEntries != null) {
            Preconditions.checkArgument(userPrivEntries.size() == 1,
                    "more than one entries for " + userIdent.toString() + ": " + userPrivEntries.toString());
            GlobalPrivEntry gEntry = (GlobalPrivEntry) userPrivEntries.get(0);
            userAuthInfo.add(userIdent.toString());
            Password password = gEntry.getPassword();
            // Password
            if (userIdent.isDomain()) {
                // for domain user ident, password is saved in property manager
                userAuthInfo.add(propertyMgr.doesUserHasPassword(userIdent) ? "No" : "Yes");
            } else {
                userAuthInfo.add((password == null || password.getPassword().length == 0) ? "No" : "Yes");
            }
            // AuthPlugin and UserForAuthPlugin
            if (password == null) {
                userAuthInfo.add(FeConstants.NULL_STRING);
                userAuthInfo.add(FeConstants.NULL_STRING);
            } else {
                if (password.getAuthPlugin() == null) {
                    userAuthInfo.add(FeConstants.NULL_STRING);
                } else {
                    userAuthInfo.add(password.getAuthPlugin().name());
                }

                if (Strings.isNullOrEmpty(password.getUserForAuthPlugin())) {
                    userAuthInfo.add(FeConstants.NULL_STRING);
                } else {
                    userAuthInfo.add(password.getUserForAuthPlugin());
                }
            }
            if (!onlyAuthenticationInfo) {
                //GlobalPrivs
                userAuthInfo.add(gEntry.getPrivSet().toString() + " (" + gEntry.isSetByDomainResolver() + ")");
            }
        } else {
            // user not in user priv table
            if (!userIdent.isDomain()) {
                // If this is not a domain user identity, it must have global priv entry.
                // TODO(cmy): I don't know why previous comment said:
                // This may happen when we grant non global privs to a non exist user via GRANT stmt.
                LOG.warn("user identity does not have global priv entry: {}", userIdent);
                userAuthInfo.add(userIdent.toString());
                userAuthInfo.add(FeConstants.NULL_STRING);
                userAuthInfo.add(FeConstants.NULL_STRING);
                userAuthInfo.add(FeConstants.NULL_STRING);
            } else {
                // this is a domain user identity and fall in here, which means this user identity does not
                // have global priv, we need to check user property to see if it has password.
                userAuthInfo.add(userIdent.toString());
                userAuthInfo.add(propertyMgr.doesUserHasPassword(userIdent) ? "No" : "Yes");
                userAuthInfo.add(FeConstants.NULL_STRING);
                userAuthInfo.add(FeConstants.NULL_STRING);
            }
            if (!onlyAuthenticationInfo) {
                userAuthInfo.add(FeConstants.NULL_STRING);
            }
        }
    }

    /**
     * TODO: This function is much too long and obscure. I'll refactor it in another PR.
     */
    private void getUserAuthInfo(List<List<String>> userAuthInfos, UserIdentity userIdent) {
        List<String> userAuthInfo = Lists.newArrayList();

        // global
        getUserGlobalPrivs(userAuthInfo, userIdent, false);

        // db
        List<String> dbPrivs = Lists.newArrayList();
        List<PrivEntry> dbPrivEntries = dbPrivTable.map.get(userIdent);
        if (dbPrivEntries != null) {
            for (PrivEntry entry : dbPrivEntries) {
                DbPrivEntry dEntry = (DbPrivEntry) entry;
                dbPrivs.add(dEntry.getOrigDb() + ": " + dEntry.getPrivSet().toString()
                        + " (" + entry.isSetByDomainResolver() + ")");
            }
            userAuthInfo.add(Joiner.on("; ").join(dbPrivs));
        } else {
            userAuthInfo.add(FeConstants.NULL_STRING);
        }

        // tbl
        List<String> tblPrivs = Lists.newArrayList();
        List<PrivEntry> tblPrivEntries = tablePrivTable.map.get(userIdent);
        if (tblPrivEntries != null) {
            for (PrivEntry entry : tblPrivEntries) {
                TablePrivEntry tEntry = (TablePrivEntry) entry;
                tblPrivs.add(tEntry.getOrigDb() + "." + tEntry.getOrigTbl() + ": "
                        + tEntry.getPrivSet().toString()
                        + " (" + entry.isSetByDomainResolver() + ")");
            }
            userAuthInfo.add(Joiner.on("; ").join(tblPrivs));
        } else {
            userAuthInfo.add(FeConstants.NULL_STRING);
        }

        // resource
        List<String> resourcePrivs = Lists.newArrayList();
        List<PrivEntry> resourcePrivEntries = resourcePrivTable.map.get(userIdent);
        if (resourcePrivEntries != null) {
            for (PrivEntry entry : resourcePrivEntries) {
                ResourcePrivEntry rEntry = (ResourcePrivEntry) entry;
                resourcePrivs.add(rEntry.getOrigResource() + ": " + rEntry.getPrivSet().toString()
                        + " (" + entry.isSetByDomainResolver() + ")");
            }
            userAuthInfo.add(Joiner.on("; ").join(resourcePrivs));
        } else {
            userAuthInfo.add(FeConstants.NULL_STRING);
        }

        userAuthInfos.add(userAuthInfo);
    }

    private Set<UserIdentity> getAllUserIdents(boolean includeEntrySetByResolver) {
        Set<UserIdentity> userIdents = Sets.newHashSet();
        List<PrivTable> allTables = Arrays.asList(userPrivTable, dbPrivTable, tablePrivTable, resourcePrivTable);
        for (PrivTable table : allTables) {
            if (includeEntrySetByResolver) {
                userIdents.addAll(table.map.keySet());
            } else {
                for (Map.Entry<UserIdentity, List<PrivEntry>> mapEntry : table.map.entrySet()) {
                    for (PrivEntry privEntry : mapEntry.getValue()) {
                        if (!privEntry.isSetByDomainResolver) {
                            userIdents.add(mapEntry.getKey());
                            break;
                        }
                    } // for privEntry in privEntryList
                } // for userIdentity, privEntryList in table map
            }
        } // for table in all tables

        return userIdents;
    }

    @Deprecated
    public List<List<String>> getUserProperties(String qualifiedUser) {
        readLock();
        try {
            return propertyMgr.fetchUserProperty(qualifiedUser);
        } catch (AnalysisException e) {
            return Lists.newArrayList();
        } finally {
            readUnlock();
        }
    }

    private void initUser() {
        try {
            UserIdentity rootUser = new UserIdentity(ROOT_USER, "%");
            rootUser.setIsAnalyzed();
            createUserInternal(rootUser, Role.OPERATOR_ROLE, new Password(new byte[0]), true /* isReplay */, false);
        } catch (DdlException e) {
            LOG.error("should not happen", e);
        }
    }

    @Deprecated
    public List<List<String>> getRoleInfo() {
        readLock();
        try {
            List<List<String>> results = Lists.newArrayList();
            roleManager.getRoleInfo(results);
            return results;
        } finally {
            readUnlock();
        }
    }

    @Deprecated
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
                    authClazz = Class.forName(Auth.KRB5_AUTH_CLASS_NAME, true, loader);
                }
            } catch (Exception e) {
                LOG.error("Failed to load {}", Auth.KRB5_AUTH_CLASS_NAME, e);
                return false;
            }
        }

        return true;
    }

    @Deprecated
    public Class<?> getAuthClazz() {
        return authClazz;
    }

    public static Auth read(DataInput in) throws IOException {
        Auth auth = new Auth();
        auth.readFields(in);
        return auth;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // role manager must be first, because role should be existed before any user
        roleManager.write(out);
        userPrivTable.write(out);
        dbPrivTable.write(out);
        tablePrivTable.write(out);
        resourcePrivTable.write(out);
        propertyMgr.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        roleManager = RoleManager.read(in);
        userPrivTable = (UserPrivTable) PrivTable.read(in);
        dbPrivTable = (DbPrivTable) PrivTable.read(in);
        tablePrivTable = (TablePrivTable) PrivTable.read(in);
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_87) {
            resourcePrivTable = (ResourcePrivTable) PrivTable.read(in);
        }
        propertyMgr = UserPropertyMgr.read(in);

        if (userPrivTable.isEmpty()) {
            // init root and admin user
            initUser();
        }
    }

    /**
     * newly added metadata entity should deserialize with gson in this method
     **/
    public long readAsGson(DataInput in, long checksum) throws IOException {
        SerializeData data = GsonUtils.GSON.fromJson(Text.readString(in), SerializeData.class);
        try {
            this.impersonateUserPrivTable.loadEntries(data.entries);
            this.roleManager.loadImpersonateRoleToUser(data.impersonateRoleToUser);
        } catch (AnalysisException e) {
            LOG.error("failed to readAsGson, ", e);
            throw new IOException(e.getMessage());
        }
        checksum ^= this.impersonateUserPrivTable.size();
        return checksum;
    }

    /**
     * newly added metadata entity should serialize with gson in this method
     **/
    public long writeAsGson(DataOutput out, long checksum) throws IOException {
        SerializeData data = new SerializeData();
        data.entries = impersonateUserPrivTable.dumpEntries();
        data.impersonateRoleToUser = roleManager.dumpImpersonateRoleToUser();
        Text.writeString(out, GsonUtils.GSON.toJson(data));
        checksum ^= impersonateUserPrivTable.size();
        return checksum;
    }

    public long loadAuth(DataInputStream dis, long checksum) throws IOException {
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_43) {
            // CAN NOT use Auth.read(), cause this auth instance is already passed to DomainResolver
            readFields(dis);
        }
        LOG.info("finished replay auth from image");
        return checksum;
    }

    public long saveAuth(DataOutputStream dos, long checksum) throws IOException {
        write(dos);
        return checksum;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(userPrivTable).append("\n");
        sb.append(dbPrivTable).append("\n");
        sb.append(tablePrivTable).append("\n");
        sb.append(resourcePrivTable).append("\n");
        sb.append(roleManager).append("\n");
        sb.append(propertyMgr).append("\n");
        return sb.toString();
    }

    private static class SerializeData {
        @SerializedName("entries")
        public List<ImpersonateUserPrivEntry> entries = new ArrayList<>();
        @SerializedName("impersonateRoleToUser")
        public Map<String, Set<UserIdentity>> impersonateRoleToUser = new HashMap<>();
    }
}

