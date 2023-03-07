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


package com.starrocks.mysql.privilege;

import com.google.common.collect.Lists;
import com.starrocks.analysis.ResourcePattern;
import com.starrocks.analysis.TablePattern;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationManager;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PEntryObject;
import com.starrocks.privilege.PrivObjNotFoundException;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.privilege.PrivilegeCollection;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.privilege.PrivilegeManager;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.privilege.RolePrivilegeCollection;
import com.starrocks.privilege.UserPrivilegeCollection;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuthUpgrader {

    private static final Logger LOG = LogManager.getLogger(AuthUpgrader.class);

    // constants used when upgrading
    private static final String STAR = "*";
    private final Auth auth;
    private final AuthenticationManager authenticationManager;
    private final PrivilegeManager privilegeManager;
    private final GlobalStateMgr globalStateMgr;

    public AuthUpgrader(
            Auth auth,
            AuthenticationManager authenticationManager,
            PrivilegeManager privilegeManager,
            GlobalStateMgr globalStateMgr) {
        this.auth = auth;
        this.authenticationManager = authenticationManager;
        this.privilegeManager = privilegeManager;
        this.globalStateMgr = globalStateMgr;
    }

    public void upgradeAsLeader() throws RuntimeException {
        try {
            LOG.info("start to upgrade as leader.");
            upgradeUser();
            Map<String, Long> roleNameToId = upgradeRole(null);
            this.globalStateMgr.getEditLog().logAuthUpgrade(roleNameToId);
            LOG.info("upgraded as leader successfully.");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void replayUpgrade(Map<String, Long> roleNameToId) throws AuthUpgradeUnrecoverableException {
        LOG.info("start to replay upgrade journal.");
        upgradeUser();
        upgradeRole(roleNameToId);
        authenticationManager.setLoaded(true);
        privilegeManager.setLoaded(true);
        LOG.info("replayed upgrade journal successfully.");
    }

    private Table getTableObject(String db, String table) {
        Database dbObj = globalStateMgr.getDb(db);
        Table tableObj = null;
        if (dbObj != null) {
            tableObj = dbObj.getTable(table);
        }

        return tableObj;
    }

    protected void upgradeUser() throws AuthUpgradeUnrecoverableException {
        UserPrivTable userPrivTable = this.auth.getUserPrivTable();
        DbPrivTable dbTable = this.auth.getDbPrivTable();
        TablePrivTable tableTable = this.auth.getTablePrivTable();
        ResourcePrivTable resourceTable = this.auth.getResourcePrivTable();
        ImpersonateUserPrivTable impersonateUserPrivTable = this.auth.getImpersonateUserPrivTable();

        // 1. create all ip users
        Iterator<PrivEntry> iter = userPrivTable.getFullReadOnlyIterator();
        while (iter.hasNext()) {
            GlobalPrivEntry entry = (GlobalPrivEntry) iter.next();
            try {
                UserIdentity userIdentity = entry.getUserIdent();
                if (userIdentity.equals(UserIdentity.ROOT)) {
                    // we should keep the password for root after upgrade
                    byte[] p = entry.getPassword().getPassword() == null ?
                            MysqlPassword.EMPTY_PASSWORD : entry.getPassword().getPassword();
                    authenticationManager.getUserToAuthenticationInfo().get(UserIdentity.ROOT).setPassword(p);
                    continue;
                }
                // 1. ignore fake entries created by domain resolver
                if (entry.isSetByDomainResolver) {
                    LOG.warn("ignore entry created by domain resolver : {}", entry);
                    continue;
                }
                // 2. create user in authentication manager
                authenticationManager.upgradeUserUnlocked(userIdentity, entry.getPassword());
            } catch (AuthenticationException e) {
                if (Config.ignore_invalid_privilege_authentications) {
                    LOG.warn("discard user priv entry:{}\n{}", entry, entry.toGrantSQL(), e);
                } else {
                    throw new AuthUpgradeUnrecoverableException("bad user priv entry " + entry, e);
                }
            }
        }

        // 2. create all domain user & set user properties
        Iterator<Map.Entry<String, UserProperty>> upiter = auth.getPropertyMgr().propertyMap.entrySet().iterator();
        while (upiter.hasNext()) {
            Map.Entry<String, UserProperty> entry = upiter.next();
            String userName = entry.getKey();
            UserProperty userProperty = entry.getValue();
            WhiteList whiteList = userProperty.getWhiteList();
            try {
                authenticationManager.upgradeUserProperty(userName, userProperty.getMaxConn());
                for (String hostname : whiteList.getAllDomains()) {
                    byte[] p = whiteList.getPassword(hostname) == null ?
                            MysqlPassword.EMPTY_PASSWORD : whiteList.getPassword(hostname);
                    Password password = new Password(p, null, null);
                    UserIdentity user = UserIdentity.createAnalyzedUserIdentWithDomain(userName, hostname);
                    authenticationManager.upgradeUserUnlocked(user, password);
                }
            } catch (AuthenticationException e) {
                if (Config.ignore_invalid_privilege_authentications) {
                    LOG.warn("discard user {} whitelist {}", userName, whiteList, e);
                } else {
                    throw new AuthUpgradeUnrecoverableException("bad domain user  " + userName, e);
                }
            }
        }

        // 3. grant privileges on all ip users
        // must loop again after all the users is created, otherwise impersonate may fail on non-existence user
        iter = userPrivTable.getFullReadOnlyIterator();
        while (iter.hasNext()) {
            GlobalPrivEntry entry = (GlobalPrivEntry) iter.next();
            try {
                UserIdentity userIdentity = entry.getUserIdent();
                if (userIdentity.equals(UserIdentity.ROOT)) {
                    LOG.info("ignore root entry : {}", entry);
                    continue;
                }
                // 1. ignore fake entries created by domain resolver
                if (entry.isSetByDomainResolver || entry.isDomain) {
                    LOG.info("ignore entry created by domain resolver : {}", entry);
                    continue;
                }

                LOG.info("upgrade auth for user '{}'", userIdentity);

                UserPrivilegeCollection collection = new UserPrivilegeCollection();
                // mark all the old grant pattern, will be used in lower level
                Set<Pair<String, String>> grantPatterns = new HashSet<>();

                // 2. grant global privileges
                upgradeUserGlobalPrivileges(entry, collection);

                // 3. grant db privileges
                upgradeUserDbPrivileges(dbTable, userIdentity, collection, grantPatterns);

                // 4. grant table privilege
                upgradeUserTablePrivileges(tableTable, userIdentity, collection, grantPatterns);

                // 5. grant resource privileges
                upgradeUserResourcePrivileges(resourceTable, userIdentity, collection);

                // 6. grant impersonate privileges
                upgradeUserImpersonate(impersonateUserPrivTable.getReadOnlyIteratorByUser(userIdentity), collection);

                // 7. set privilege to user
                privilegeManager.upgradeUserInitPrivilegeUnlock(userIdentity, collection);
            } catch (AuthUpgradeUnrecoverableException | PrivilegeException e) {
                if (Config.ignore_invalid_privilege_authentications) {
                    LOG.warn("discard user priv entry:{}\n{}", entry, entry.toGrantSQL(), e);
                } else {
                    throw new AuthUpgradeUnrecoverableException("bad user priv entry " + entry, e);
                }
            }
        } // for iter in globalTable

        // 4. grant privileges on all domain users
        // must loop again after all the users is created, otherwise impersonate may fail on non-existence user
        upiter = auth.getPropertyMgr().propertyMap.entrySet().iterator();
        while (upiter.hasNext()) {
            Map.Entry<String, UserProperty> entry = upiter.next();
            String userName = entry.getKey();
            WhiteList whiteList = entry.getValue().getWhiteList();
            try {
                for (String hostname : whiteList.getAllDomains()) {
                    UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithDomain(userName, hostname);
                    UserPrivilegeCollection collection = new UserPrivilegeCollection();
                    // mark all the old grant pattern, will be used in lower level
                    Set<Pair<String, String>> grantPatterns = new HashSet<>();

                    // 1. grant global privileges
                    Iterator<PrivEntry> globalIter = userPrivTable.getReadOnlyIteratorByUser(userIdentity);
                    if (globalIter.hasNext()) {
                        upgradeUserGlobalPrivileges((GlobalPrivEntry) globalIter.next(), collection);
                    }

                    // 2. grant db privileges
                    upgradeUserDbPrivileges(dbTable, userIdentity, collection, grantPatterns);

                    // 3. grant table privilege
                    upgradeUserTablePrivileges(tableTable, userIdentity, collection, grantPatterns);

                    // 4. grant resource privileges
                    upgradeUserResourcePrivileges(resourceTable, userIdentity, collection);

                    // 5. grant impersonate privileges
                    upgradeUserImpersonate(impersonateUserPrivTable.getReadOnlyIteratorByUser(userIdentity),
                            collection);

                    // 6. set privilege to user
                    privilegeManager.upgradeUserInitPrivilegeUnlock(userIdentity, collection);
                }
            } catch (AuthUpgradeUnrecoverableException | PrivilegeException e) {
                if (Config.ignore_invalid_privilege_authentications) {
                    LOG.warn("discard domain user priv for {}", userName);
                } else {
                    throw new AuthUpgradeUnrecoverableException("bad user priv for " + userName, e);
                }
            }
        } // for upiter in UserPropertyMap
    }

    protected void upgradeUserGlobalPrivileges(
            GlobalPrivEntry entry, UserPrivilegeCollection collection)
            throws PrivilegeException, AuthUpgradeUnrecoverableException {
        PrivBitSet bitSet = entry.getPrivSet();
        for (Privilege privilege : bitSet.toPrivilegeList()) {
            switch (privilege) {
                case SELECT_PRIV:
                    upgradeTablePrivileges(STAR, STAR, privilege, collection, null);
                    upgradeViewPrivileges(STAR, STAR, privilege, collection, null);
                    upgradeMaterializedViewPrivileges(STAR, STAR, privilege, collection, null);
                    break;

                case LOAD_PRIV:
                    upgradeTablePrivileges(STAR, STAR, privilege, collection, null);
                    break;

                case USAGE_PRIV:
                    upgradeResourcePrivileges(STAR, privilege, collection, false);
                    break;

                case CREATE_PRIV:
                    upgradeDbPrivileges(STAR, privilege, collection, null);
                    break;

                case DROP_PRIV:
                case ALTER_PRIV:
                    upgradeDbPrivileges(STAR, privilege, collection, null);
                    upgradeTablePrivileges(STAR, STAR, privilege, collection, null);
                    upgradeViewPrivileges(STAR, STAR, privilege, collection, null);
                    upgradeMaterializedViewPrivileges(STAR, STAR, privilege, collection, null);
                    break;

                case ADMIN_PRIV:
                case NODE_PRIV:
                case GRANT_PRIV:
                    upgradeBuiltInRoles(privilege, collection);
                    break;

                default:
                    throw new AuthUpgradeUnrecoverableException(
                            "unsupported global " + privilege + " for user " + entry.getUserIdent());
            }
        }
    }

    protected void upgradeUserDbPrivileges(
            DbPrivTable table, UserIdentity user, UserPrivilegeCollection collection,
            Set<Pair<String, String>> grantPatterns)
            throws PrivilegeException, AuthUpgradeUnrecoverableException {
        Iterator<PrivEntry> iterator;
        // loop twice, the first one is for GRANT_PRIV
        iterator = table.getReadOnlyIteratorByUser(user);
        while (iterator.hasNext()) {
            DbPrivEntry entry = (DbPrivEntry) iterator.next();
            PrivBitSet bitSet = entry.getPrivSet();
            if (bitSet.containsPrivs(Privilege.GRANT_PRIV)) {
                grantPatterns.add(Pair.create(entry.getOrigDb(), STAR));
            }
        }

        // loop twice, the second one is for all privilege except GRANT_PRIV
        iterator = table.getReadOnlyIteratorByUser(user);
        while (iterator.hasNext()) {
            DbPrivEntry entry = (DbPrivEntry) iterator.next();
            String db = entry.getOrigDb();
            PrivBitSet bitSet = entry.getPrivSet();
            for (Privilege privilege : bitSet.toPrivilegeList()) {
                switch (privilege) {
                    case SELECT_PRIV:
                        upgradeTablePrivileges(db, STAR, privilege, collection, grantPatterns);
                        upgradeViewPrivileges(db, STAR, privilege, collection, grantPatterns);
                        upgradeMaterializedViewPrivileges(db, STAR, privilege, collection, grantPatterns);
                        break;

                    case LOAD_PRIV:
                        upgradeTablePrivileges(db, STAR, privilege, collection, grantPatterns);
                        break;

                    case DROP_PRIV:
                    case ALTER_PRIV:
                        upgradeDbPrivileges(db, privilege, collection, grantPatterns);
                        upgradeTablePrivileges(db, STAR, privilege, collection, grantPatterns);
                        upgradeViewPrivileges(db, STAR, privilege, collection, grantPatterns);
                        upgradeMaterializedViewPrivileges(db, STAR, privilege, collection, grantPatterns);
                        break;

                    case CREATE_PRIV:
                        upgradeDbPrivileges(db, privilege, collection, grantPatterns);
                        break;

                    case GRANT_PRIV:
                        break;

                    default:
                        throw new AuthUpgradeUnrecoverableException("unsupported db " +
                                privilege + " for user " + user);
                }
            }
        }
    }

    protected void upgradeUserTablePrivileges(
            TablePrivTable table, UserIdentity user, UserPrivilegeCollection collection,
            Set<Pair<String, String>> grantPatterns)
            throws PrivilegeException, AuthUpgradeUnrecoverableException {
        // loop twice, the first one is for GRANT_PRIV
        Iterator<PrivEntry> iterator = table.getReadOnlyIteratorByUser(user);
        while (iterator.hasNext()) {
            TablePrivEntry entry = (TablePrivEntry) iterator.next();
            PrivBitSet bitSet = entry.getPrivSet();
            if (bitSet.containsPrivs(Privilege.GRANT_PRIV)) {
                grantPatterns.add(Pair.create(entry.getOrigDb(), entry.getOrigTbl()));
            }
        }

        // loop twice, the second one is for all privilege except GRANT_PRIV
        iterator = table.getReadOnlyIteratorByUser(user);
        while (iterator.hasNext()) {
            TablePrivEntry entry = (TablePrivEntry) iterator.next();
            String dbName = entry.getOrigDb();
            String tableName = entry.getOrigTbl();

            PrivBitSet bitSet = entry.getPrivSet();
            for (Privilege privilege : bitSet.toPrivilegeList()) {
                switch (privilege) {
                    case DROP_PRIV:
                    case ALTER_PRIV:
                    case SELECT_PRIV: {
                        Table tableObj = getTableObject(dbName, tableName);
                        if (tableObj == null) {
                            break;
                        }
                        Table.TableType tableType = tableObj.getType();
                        if (tableType.equals(Table.TableType.VIEW)) {
                            upgradeViewPrivileges(dbName, tableName, privilege, collection, grantPatterns);
                        } else if (tableObj.isMaterializedView()) {
                            upgradeMaterializedViewPrivileges(dbName, tableName, privilege, collection, grantPatterns);
                        } else {
                            upgradeTablePrivileges(dbName, tableName, privilege, collection, grantPatterns);
                        }
                        break;
                    }

                    case LOAD_PRIV:
                        upgradeTablePrivileges(dbName, tableName, privilege, collection, grantPatterns);
                        break;

                    case GRANT_PRIV:
                        break;

                    case CREATE_PRIV:
                        // discard create privilege on table
                        break;

                    default:
                        throw new AuthUpgradeUnrecoverableException(
                                "unsupported table " + privilege + " for user " + user);
                }
            } // for privilege
        }
    }

    protected void upgradeUserResourcePrivileges(ResourcePrivTable table, UserIdentity user,
                                                 UserPrivilegeCollection collection)
            throws PrivilegeException, AuthUpgradeUnrecoverableException {

        Set<String> grantPatterns = new HashSet<>();
        // loop twice, the first one is for GRANT_PRIV
        Iterator<PrivEntry> iterator = table.getReadOnlyIteratorByUser(user);
        while (iterator.hasNext()) {
            ResourcePrivEntry entry = (ResourcePrivEntry) iterator.next();
            PrivBitSet bitSet = entry.getPrivSet();
            if (bitSet.containsPrivs(Privilege.GRANT_PRIV)) {
                if (entry.getOrigResource().equals(STAR)) {
                    upgradeBuiltInRoles(Privilege.GRANT_PRIV, collection);
                } else {
                    grantPatterns.add(entry.getOrigResource());
                }
            }
        }

        iterator = table.getReadOnlyIteratorByUser(user);
        while (iterator.hasNext()) {
            ResourcePrivEntry entry = (ResourcePrivEntry) iterator.next();
            String name = entry.getOrigResource();
            PrivBitSet bitSet = entry.getPrivSet();
            for (Privilege privilege : bitSet.toPrivilegeList()) {
                switch (privilege) {
                    case USAGE_PRIV:
                        upgradeResourcePrivileges(
                                name, privilege, collection, grantPatterns.contains(name));
                        break;

                    case GRANT_PRIV:
                        break;

                    default:
                        throw new AuthUpgradeUnrecoverableException("user resource privilege " +
                                privilege + " is not supported");
                }
            }
        }
    }

    protected void upgradeUserImpersonate(Iterator<PrivEntry> iterator, UserPrivilegeCollection collection)
            throws PrivilegeException {
        while (iterator.hasNext()) {
            ImpersonateUserPrivEntry entry = (ImpersonateUserPrivEntry) iterator.next();
            upgradeImpersonatePrivileges(entry.getSecuredUserIdentity(), collection);
        }
    }

    /**
     * input param roleNameToId, can be null, meaning that leader is upgrading
     * return roleNameToId if it's leader
     */
    protected Map<String, Long> upgradeRole(Map<String, Long> roleNameToId) throws AuthUpgradeUnrecoverableException {
        Map<String, Long> ret = new HashMap<>();
        Iterator<Map.Entry<String, Role>> iterator = this.auth.roleManager.roles.entrySet().iterator();
        Long roleId;
        while (iterator.hasNext()) {
            Map.Entry<String, Role> entry = iterator.next();
            String roleName = entry.getKey();
            if (roleNameToId == null) {
                roleId = globalStateMgr.getNextId();
                ret.put(roleName, roleId);
            } else {
                roleId = roleNameToId.get(roleName);
                if (roleId == null) {
                    LOG.warn("cannot find {} in {}", roleName, roleNameToId);
                    throw new AuthUpgradeUnrecoverableException(
                            "failed to upgrade role " + roleName + " while replaying!");
                }
            }

            Role role = entry.getValue();
            try {
                if (isBuiltInRoles(roleName)) {
                    // built roles has been automatically created
                    continue;
                }

                LOG.info("upgrade auth for role '{}'", roleName);

                // create new role
                RolePrivilegeCollection collection = new RolePrivilegeCollection(
                        roleName, RolePrivilegeCollection.RoleFlags.MUTABLE,
                        RolePrivilegeCollection.RoleFlags.REMOVABLE);

                // 1. table privileges(including global+db)
                upgradeRoleTablePrivileges(role.getTblPatternToPrivs(), collection);

                // 2. resource privileges
                upgradeRoleResourcePrivileges(role.getResourcePatternToPrivs(), collection);

                // 3. impersonate privileges
                upgradeRoleImpersonatePrivileges(role.getImpersonateUsers(), collection);

                privilegeManager.upgradeRoleInitPrivilegeUnlock(roleId, collection);
            } catch (AuthUpgradeUnrecoverableException | PrivilegeException e) {
                if (Config.ignore_invalid_privilege_authentications) {
                    LOG.warn("discard role[{}] priv:{}", roleName, role, e);
                } else {
                    throw new AuthUpgradeUnrecoverableException("bad role priv " + roleName, e);
                }
            }
        }
        return ret;
    }

    protected void upgradeRoleTablePrivileges(
            Map<TablePattern, PrivBitSet> tblPatternToPrivs, RolePrivilegeCollection collection)
            throws PrivilegeException, AuthUpgradeUnrecoverableException {
        Iterator<Map.Entry<TablePattern, PrivBitSet>> iterator;
        Set<Pair<String, String>> grantPatterns = new HashSet<>();

        // loop twice, the first one is for GRANT_PRIV
        iterator = tblPatternToPrivs.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<TablePattern, PrivBitSet> entry = iterator.next();
            TablePattern pattern = entry.getKey();
            PrivBitSet bitSet = entry.getValue();
            if (bitSet.containsPrivs(Privilege.GRANT_PRIV)) {
                if (pattern.equals(TablePattern.ALL)) {
                    upgradeBuiltInRoles(Privilege.GRANT_PRIV, collection);
                } else {
                    grantPatterns.add(Pair.create(pattern.getQuolifiedDb(), pattern.getTbl()));
                }
            }
        }

        // loop twice, the second one is for all privilege except GRANT_PRIV
        iterator = tblPatternToPrivs.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<TablePattern, PrivBitSet> entry = iterator.next();
            TablePattern pattern = entry.getKey();
            String db = pattern.getQuolifiedDb();
            String table = pattern.getTbl();
            PrivBitSet bitSet = entry.getValue();

            for (Privilege privilege : bitSet.toPrivilegeList()) {
                switch (privilege) {
                    case SELECT_PRIV:
                        if (!table.equals(STAR)) {
                            Table tableObj = getTableObject(db, table);
                            if (tableObj == null) {
                                break;
                            }
                            Table.TableType tableType = tableObj.getType();
                            if (tableType.equals(Table.TableType.VIEW)) {
                                upgradeViewPrivileges(db, table, privilege, collection, grantPatterns);
                            } else if (tableObj.isMaterializedView()) {
                                upgradeMaterializedViewPrivileges(db, table, privilege, collection, grantPatterns);
                            } else {
                                upgradeTablePrivileges(db, table, privilege, collection, grantPatterns);
                            }
                        } else {
                            upgradeTablePrivileges(db, table, privilege, collection, grantPatterns);
                            upgradeViewPrivileges(db, table, privilege, collection, grantPatterns);
                            upgradeMaterializedViewPrivileges(db, table, privilege, collection, grantPatterns);
                        }
                        break;

                    case LOAD_PRIV:
                        upgradeTablePrivileges(db, table, privilege, collection, grantPatterns);
                        break;

                    case USAGE_PRIV:
                        if (!db.equals(STAR) || !table.equals(STAR)) {
                            throw new AuthUpgradeUnrecoverableException(privilege + " on " +
                                    pattern + " is not supported!");
                        }
                        upgradeResourcePrivileges(STAR, privilege, collection, false);
                        break;

                    case CREATE_PRIV:
                        if (table.equals(STAR)) {
                            upgradeDbPrivileges(db, privilege, collection, grantPatterns);
                        } // otherwise just ignore create
                        break;

                    case DROP_PRIV:
                    case ALTER_PRIV:
                        if (!table.equals(STAR)) {
                            Table tableObj = getTableObject(db, table);
                            if (tableObj == null) {
                                break;
                            }
                            Table.TableType tableType = tableObj.getType();
                            if (tableType.equals(Table.TableType.VIEW)) {
                                upgradeViewPrivileges(db, table, privilege, collection, grantPatterns);
                            } else if (tableObj.isMaterializedView()) {
                                upgradeMaterializedViewPrivileges(db, table, privilege, collection, grantPatterns);
                            } else {
                                upgradeTablePrivileges(db, table, privilege, collection, grantPatterns);
                            }
                        } else {
                            upgradeDbPrivileges(db, privilege, collection, grantPatterns);
                            upgradeTablePrivileges(db, table, privilege, collection, grantPatterns);
                            upgradeViewPrivileges(db, table, privilege, collection, grantPatterns);
                            upgradeMaterializedViewPrivileges(db, table, privilege, collection, grantPatterns);
                        }
                        break;

                    case NODE_PRIV:
                    case ADMIN_PRIV:
                        if (!db.equals(STAR) || !table.equals(STAR)) {
                            throw new AuthUpgradeUnrecoverableException(privilege + " on " +
                                    pattern + " is not supported!");
                        }
                        upgradeBuiltInRoles(privilege, collection);
                        break;

                    case GRANT_PRIV:
                        break;

                    default:
                        throw new AuthUpgradeUnrecoverableException("role table privilege " +
                                privilege + " hasn't implemented");
                }
            }
        }
    }

    protected void assertGlobalResource(Privilege privilege, String name) throws AuthUpgradeUnrecoverableException {
        if (!name.equals(STAR)) {
            throw new AuthUpgradeUnrecoverableException(privilege + " on " + name + " is not supported!");
        }
    }

    protected void upgradeRoleResourcePrivileges(
            Map<ResourcePattern, PrivBitSet> resourcePatternToPrivs, RolePrivilegeCollection collection)
            throws PrivilegeException, AuthUpgradeUnrecoverableException {
        Iterator<Map.Entry<ResourcePattern, PrivBitSet>> iterator;
        Set<String> grantPatterns = new HashSet<>();

        // loop twice, the first one is for GRANT_PRIV
        iterator = resourcePatternToPrivs.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<ResourcePattern, PrivBitSet> entry = iterator.next();
            ResourcePattern pattern = entry.getKey();
            PrivBitSet bitSet = entry.getValue();

            if (bitSet.containsPrivs(Privilege.GRANT_PRIV)) {
                if (pattern.getResourceName().equals(STAR)) {
                    upgradeBuiltInRoles(Privilege.GRANT_PRIV, collection);
                } else {
                    grantPatterns.add(pattern.getResourceName());
                }
            }
        }

        // loop twice, the second one is for all privilege except GRANT_PRIV
        iterator = resourcePatternToPrivs.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<ResourcePattern, PrivBitSet> entry = iterator.next();
            ResourcePattern pattern = entry.getKey();
            String name = pattern.getResourceName();
            PrivBitSet bitSet = entry.getValue();

            for (Privilege privilege : bitSet.toPrivilegeList()) {
                switch (privilege) {
                    case USAGE_PRIV:
                        upgradeResourcePrivileges(name, privilege, collection, grantPatterns.contains(name));
                        break;

                    case LOAD_PRIV:
                        assertGlobalResource(privilege, name);
                        upgradeTablePrivileges(STAR, STAR, privilege, collection, null);
                        break;

                    case NODE_PRIV:
                    case ADMIN_PRIV:
                        assertGlobalResource(privilege, name);
                        upgradeBuiltInRoles(privilege, collection);
                        break;

                    case GRANT_PRIV:
                        break;

                    case SELECT_PRIV:
                        assertGlobalResource(privilege, name);
                        upgradeTablePrivileges(STAR, STAR, privilege, collection, null);
                        upgradeViewPrivileges(STAR, STAR, privilege, collection, null);
                        upgradeMaterializedViewPrivileges(STAR, STAR, privilege, collection, null);
                        break;

                    case ALTER_PRIV:
                    case DROP_PRIV:
                        assertGlobalResource(privilege, name);
                        upgradeDbPrivileges(STAR, privilege, collection, null);
                        upgradeViewPrivileges(STAR, STAR, privilege, collection, null);
                        upgradeMaterializedViewPrivileges(STAR, STAR, privilege, collection, null);
                        upgradeTablePrivileges(STAR, STAR, privilege, collection, null);
                        break;

                    case CREATE_PRIV:
                        assertGlobalResource(privilege, name);
                        upgradeDbPrivileges(STAR, privilege, collection, null);
                        break;

                    default:
                        throw new AuthUpgradeUnrecoverableException("role resource privilege " +
                                privilege + " hasn't implemented");
                }
            }
        }
    }

    protected void upgradeRoleImpersonatePrivileges(Set<UserIdentity> impersonateUsers,
                                                    RolePrivilegeCollection collection)
            throws PrivilegeException {
        Iterator<UserIdentity> iterator = impersonateUsers.iterator();
        while (iterator.hasNext()) {
            UserIdentity securedUser = iterator.next();
            upgradeImpersonatePrivileges(securedUser, collection);
        }
    }

    protected boolean isBuiltInRoles(String roleName) {
        return roleName.equals(Role.ADMIN_ROLE) || roleName.equals(Role.OPERATOR_ROLE);
    }

    protected void upgradeDbPrivileges(
            String db, Privilege privilege, PrivilegeCollection collection,
            Set<Pair<String, String>> grantPatterns)
            throws PrivilegeException, AuthUpgradeUnrecoverableException {

        // action
        List<PrivilegeType> privilegeTypes;
        switch (privilege) {
            case CREATE_PRIV:
                privilegeTypes = Arrays.asList(
                        PrivilegeType.CREATE_TABLE,
                        PrivilegeType.CREATE_VIEW,
                        PrivilegeType.CREATE_MATERIALIZED_VIEW);
                break;

            case DROP_PRIV:
                privilegeTypes = Collections.singletonList(PrivilegeType.DROP);
                break;

            case ALTER_PRIV:
                privilegeTypes = Collections.singletonList(PrivilegeType.ALTER);
                break;
            default:
                throw new AuthUpgradeUnrecoverableException("db privilege " + privilege + " hasn't implemented");
        }

        // isGrant
        boolean isGrant = false;
        if (grantPatterns != null) {
            isGrant = matchTableGrant(grantPatterns, db, STAR);
        }


        // object
        List<PEntryObject> objects;
        try {
            if (db.equals(STAR)) {
                // for *.*
                objects = Collections.singletonList(
                        privilegeManager.generateObject(ObjectType.DATABASE, Lists.newArrayList("*")));
                if (privilege == Privilege.CREATE_PRIV) {
                    // for CREATE_PRIV on *.*, we also need to grant create_database on default_catalog
                    collection.grant(ObjectType.CATALOG,
                            Collections.singletonList(PrivilegeType.CREATE_DATABASE),
                            Collections.singletonList(privilegeManager.generateObject(ObjectType.CATALOG,
                                    Collections.singletonList(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME))),
                            isGrant);
                }
            } else {
                // for db.*
                objects = Collections.singletonList(privilegeManager.generateObject(
                        ObjectType.DATABASE, Collections.singletonList(db)));
            }
        } catch (PrivObjNotFoundException e) {
            // In old {@link Auth} module, privilege entry is not removed after corresponding object(db, table etc.)
            // is dropped, so in the upgrade process we should always ignore the exception because of non-existed
            // object, for those privilege entries, we don't need to transform them to privilege entry in new RBAC
            // based privilege framework.
            LOG.info("Privilege '{}' on db {} is ignored when upgrading from" +
                    " old auth because of non-existed object, message: {}", privilege, db, e.getMessage());
            return;
        }

        // grant db
        collection.grant(ObjectType.DATABASE, privilegeTypes, objects, isGrant);
    }

    protected void upgradeViewPrivileges(
            String db, String view, Privilege privilege, PrivilegeCollection collection,
            Set<Pair<String, String>> grantPatterns)
            throws PrivilegeException, AuthUpgradeUnrecoverableException {

        // action
        PrivilegeType action;
        switch (privilege) {
            case SELECT_PRIV:
                action = PrivilegeType.SELECT;
                break;

            case ALTER_PRIV:
                action = PrivilegeType.ALTER;
                break;

            case DROP_PRIV:
                action = PrivilegeType.DROP;
                break;

            default:
                throw new AuthUpgradeUnrecoverableException("view privilege " + privilege + " hasn't implemented");
        }
        List<PrivilegeType> actionSet = Collections.singletonList(action);

        // object
        List<PEntryObject> objects;
        try {
            if (db.equals(STAR)) {
                objects = Collections.singletonList(privilegeManager.generateObject(
                        ObjectType.VIEW, Lists.newArrayList("*", "*")));
            } else if (view.equals(STAR)) {
                // ALL TABLES in db
                objects = Collections.singletonList(
                        privilegeManager.generateObject(ObjectType.VIEW, Lists.newArrayList(db, "*")));
            } else {
                // db.view
                objects = Collections.singletonList(privilegeManager.generateObject(
                        ObjectType.VIEW, Arrays.asList(db, view)));
            }
        } catch (PrivObjNotFoundException e) {
            LOG.info("Privilege '{}' on view {}.{} is ignored when upgrading from" +
                    " old auth because of non-existed object, message: {}", privilege, db, view, e.getMessage());
            return;
        }


        // isGrant
        boolean isGrant = false;
        if (grantPatterns != null) {
            isGrant = matchTableGrant(grantPatterns, db, view);
        }

        // grant table
        collection.grant(ObjectType.VIEW, actionSet, objects, isGrant);
    }

    protected void upgradeTablePrivileges(
            String db, String table, Privilege privilege, PrivilegeCollection collection,
            Set<Pair<String, String>> grantPatterns)
            throws PrivilegeException, AuthUpgradeUnrecoverableException {

        // action
        List<PrivilegeType> privilegeTypes;
        switch (privilege) {
            case SELECT_PRIV:
                privilegeTypes = Collections.singletonList(PrivilegeType.SELECT);
                break;

            case LOAD_PRIV:
                privilegeTypes = Arrays.asList(
                        PrivilegeType.INSERT,
                        PrivilegeType.DELETE,
                        PrivilegeType.EXPORT);
                break;

            case DROP_PRIV:
                privilegeTypes = Collections.singletonList(PrivilegeType.DROP);
                break;

            case ALTER_PRIV:
                privilegeTypes = Collections.singletonList(PrivilegeType.ALTER);
                break;

            default:
                throw new AuthUpgradeUnrecoverableException("table privilege " + privilege + " hasn't implemented");
        }

        // object
        List<PEntryObject> objects;
        try {
            if (db.equals(STAR)) {
                objects = Collections.singletonList(privilegeManager.generateObject(ObjectType.TABLE,
                        Lists.newArrayList("*", "*")));
            } else if (table.equals(STAR)) {
                // ALL TABLES in db
                objects = Collections.singletonList(privilegeManager.generateObject(ObjectType.TABLE,
                        Lists.newArrayList(db, "*")));
            } else {
                // db.table
                objects = Collections.singletonList(privilegeManager.generateObject(ObjectType.TABLE,
                        Arrays.asList(db, table)));
            }
        } catch (PrivObjNotFoundException e) {
            LOG.info("Privilege '{}' on table {}.{} is ignored when upgrading from" +
                    " old auth because of non-existed object, message: {}", privilege, db, table, e.getMessage());
            return;
        }

        // isGrant
        boolean isGrant = false;
        if (grantPatterns != null) {
            isGrant = matchTableGrant(grantPatterns, db, table);
        }

        // grant table
        collection.grant(ObjectType.TABLE, privilegeTypes, objects, isGrant);
    }

    protected void upgradeMaterializedViewPrivileges(
            String db, String mv, Privilege privilege, PrivilegeCollection collection,
            Set<Pair<String, String>> grantPatterns) throws PrivilegeException, AuthUpgradeUnrecoverableException {

        // actionSet
        List<PrivilegeType> privilegeTypes;
        switch (privilege) {
            case ALTER_PRIV:
                privilegeTypes = Arrays.asList(
                        PrivilegeType.ALTER,
                        PrivilegeType.REFRESH);
                break;

            case DROP_PRIV:
                privilegeTypes = Collections.singletonList(PrivilegeType.DROP);
                break;
            case SELECT_PRIV:
                privilegeTypes = Collections.singletonList(PrivilegeType.SELECT);
                break;
            default:
                throw new AuthUpgradeUnrecoverableException("materialized view privilege "
                        + privilege + " hasn't implemented");
        }

        // object
        List<PEntryObject> objects;
        try {
            if (db.equals(STAR)) {
                objects = Collections.singletonList(
                        privilegeManager.generateObject(ObjectType.MATERIALIZED_VIEW, Lists.newArrayList("*", "*")));
            } else if (mv.equals(STAR)) {
                // ALL TABLES in db
                objects = Collections.singletonList(privilegeManager.generateObject(
                        ObjectType.MATERIALIZED_VIEW, Lists.newArrayList(db, "*")));
            } else {
                // db.mv
                objects = Collections.singletonList(
                        privilegeManager.generateObject(ObjectType.MATERIALIZED_VIEW, Arrays.asList(db, mv)));
            }
        } catch (PrivObjNotFoundException e) {
            LOG.info("Privilege '{}' on materialized view {}.{} is ignored when upgrading from" +
                    " old auth because of non-existed object, message: {}", privilege, db, mv, e.getMessage());
            return;
        }


        // isGrant
        boolean isGrant = false;
        if (grantPatterns != null) {
            isGrant = matchTableGrant(grantPatterns, db, mv);
        }

        // grant table
        collection.grant(ObjectType.MATERIALIZED_VIEW, privilegeTypes, objects, isGrant);
    }

    // `grantPattern` only contains dbx.tblx or dbx.*,
    // grant_priv on *.* will be upgraded to user_admin built-in role
    private boolean matchTableGrant(Set<Pair<String, String>> grantPattern, String db, String table) {
        return grantPattern.contains(Pair.create(db, table)) || grantPattern.contains(Pair.create(db, STAR));
    }

    protected void upgradeImpersonatePrivileges(UserIdentity user, PrivilegeCollection collection)
            throws PrivilegeException {
        List<PEntryObject> objects;
        try {
            objects = Collections.singletonList(privilegeManager.generateUserObject(ObjectType.USER, user));
        } catch (PrivObjNotFoundException e) {
            LOG.info("Privilege 'IMPERSONATE' on user {} is ignored when upgrading from" +
                    " old auth because of non-existed object, message: {}", user, e.getMessage());
            return;
        }
        collection.grant(ObjectType.USER, Collections.singletonList(PrivilegeType.IMPERSONATE), objects, false);
    }

    protected void upgradeResourcePrivileges(
            String name, Privilege privilege, PrivilegeCollection collection, boolean isGrant)
            throws PrivilegeException, AuthUpgradeUnrecoverableException {
        switch (privilege) {
            case USAGE_PRIV: {
                List<PEntryObject> objects;
                try {
                    if (name.equals(STAR)) {
                        objects = Collections.singletonList(privilegeManager.generateObject(
                                ObjectType.RESOURCE, Lists.newArrayList("*")));
                    } else {
                        objects = Collections.singletonList(privilegeManager.generateObject(
                                ObjectType.RESOURCE, Collections.singletonList(name)));
                    }
                } catch (PrivObjNotFoundException e) {
                    LOG.info("Privilege '{}' on resource {} is ignored when upgrading from" +
                            " old auth because of non-existed object, message: {}", privilege, name, e.getMessage());
                    return;
                }
                collection.grant(ObjectType.RESOURCE, Collections.singletonList(PrivilegeType.USAGE), objects, isGrant);
                break;
            }

            default:
                throw new AuthUpgradeUnrecoverableException("resource privilege " + privilege + " hasn't implemented");
        }
    }

    protected void upgradeBuiltInRoles(Privilege privilege, PrivilegeCollection collection)
            throws PrivilegeException, AuthUpgradeUnrecoverableException {
        switch (privilege) {
            case ADMIN_PRIV:   // ADMIN_PRIV -> db_admin + user_admin
                grantRoleToCollection(collection,
                        PrivilegeBuiltinConstants.DB_ADMIN_ROLE_ID, PrivilegeBuiltinConstants.USER_ADMIN_ROLE_ID);
                break;
            case NODE_PRIV:    // NODE_PRIV -> cluster_admin
                grantRoleToCollection(collection, PrivilegeBuiltinConstants.CLUSTER_ADMIN_ROLE_ID);
                break;
            case GRANT_PRIV:   // GRANT_PRIV -> user_admin
                grantRoleToCollection(collection, PrivilegeBuiltinConstants.USER_ADMIN_ROLE_ID);
                break;

            default:
                throw new AuthUpgradeUnrecoverableException("unsupported " + privilege + " for built-in roles!");
        }
    }

    private void grantRoleToCollection(PrivilegeCollection collection, Long... parentRoleIds)
            throws PrivilegeException {
        for (long parentRoleId : parentRoleIds) {
            RolePrivilegeCollection rolePrivilegeCollection =
                    privilegeManager.getRolePrivilegeCollectionUnlocked(parentRoleId, true);
            for (Map.Entry<ObjectType, List<PrivilegeCollection.PrivilegeEntry>> entry :
                    rolePrivilegeCollection.getTypeToPrivilegeEntryList().entrySet()) {
                for (PrivilegeCollection.PrivilegeEntry privEntry : entry.getValue()) {
                    collection.grant(entry.getKey(), privilegeManager.analyzeActionSet(entry.getKey(), privEntry.getActionSet()),
                            Collections.singletonList(privEntry.getObject()), false);
                }
            }
        }
    }

    public static class AuthUpgradeUnrecoverableException extends Exception {
        public AuthUpgradeUnrecoverableException(String s) {
            super(s);
        }

        public AuthUpgradeUnrecoverableException(String s, Exception e) {
            super(s);
            initCause(e);
        }
    }
}
