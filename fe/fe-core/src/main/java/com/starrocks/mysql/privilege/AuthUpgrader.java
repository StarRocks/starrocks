// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.mysql.privilege;

import com.starrocks.analysis.ResourcePattern;
import com.starrocks.analysis.TablePattern;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationManager;
import com.starrocks.common.Config;
import com.starrocks.privilege.ActionSet;
import com.starrocks.privilege.PEntryObject;
import com.starrocks.privilege.PrivilegeCollection;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.privilege.PrivilegeManager;
import com.starrocks.privilege.PrivilegeTypes;
import com.starrocks.privilege.RolePrivilegeCollection;
import com.starrocks.privilege.UserPrivilegeCollection;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuthUpgrader {

    private static final Logger LOG = LogManager.getLogger(AuthUpgrader.class);
    private Auth auth;
    private AuthenticationManager authenticationManager;
    private PrivilegeManager privilegeManager;
    private GlobalStateMgr globalStateMgr;

    // constants used when upgrading
    private short tableTypeId;
    private short userTypeId;
    private String tableTypeStr;
    private String dbTypeStr;
    private String userTypeStr;
    private List<PEntryObject> allTablesInAllDbObject;
    private ActionSet selectActionSet;
    private ActionSet impersonateActionSet;

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

    public boolean needUpgrade() {
        return !this.authenticationManager.isLoaded() || !this.privilegeManager.isLoaded();
    }

    public void upgradeAsLeader() throws RuntimeException {
        try {
            LOG.info("start to upgrade as leader.");
            init();
            upgradeUser();
            Map<String, Long> roleNameToId = upgradeRole(null);
            this.globalStateMgr.getEditLog().logAuthUpgrade(roleNameToId);
            LOG.info("upgraded as leader successfully.");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void replayUpgradeAsFollower(Map<String, Long> roleNameToId) throws AuthUpgradeUnrecoveredException {
        LOG.info("start to replay upgrade as follower.");
        init();
        upgradeUser();
        upgradeRole(roleNameToId);
        authenticationManager.setLoaded();
        privilegeManager.setLoaded();
        LOG.info("replayed upgrade as follower successfully.");
    }

    private void init() throws AuthUpgradeUnrecoveredException {
        try {
            tableTypeStr = PrivilegeTypes.TABLE.name();
            dbTypeStr = PrivilegeTypes.DATABASE.name();
            userTypeStr = PrivilegeTypes.USER.name();
            tableTypeId = privilegeManager.analyzeType(tableTypeStr);
            userTypeId = privilegeManager.analyzeType(userTypeStr);
            allTablesInAllDbObject = Arrays.asList(privilegeManager.analyzeObject(
                    tableTypeStr,
                    Arrays.asList(PrivilegeTypes.TABLE.getPlural(), PrivilegeTypes.DATABASE.getPlural()),
                    null, null));
            selectActionSet = privilegeManager.analyzeActionSet(
                    tableTypeStr,
                    tableTypeId,
                    Arrays.asList(PrivilegeTypes.TableActions.SELECT.toString()));
            impersonateActionSet = privilegeManager.analyzeActionSet(
                    userTypeStr,
                    userTypeId,
                    Arrays.asList(PrivilegeTypes.UserActions.IMPERSONATE.toString()));
        } catch (PrivilegeException e) {
            throw new AuthUpgradeUnrecoveredException("should not happen", e);
        }
    }

    protected void upgradeUser() throws AuthUpgradeUnrecoveredException {
        UserPrivTable globalTable = this.auth.getUserPrivTable();
        DbPrivTable dbTable = this.auth.getDbPrivTable();
        TablePrivTable tableTable = this.auth.getTablePrivTable();
        ResourcePrivTable resourceTable = this.auth.getResourcePrivTable();
        ImpersonateUserPrivTable impersonateUserPrivTable = this.auth.getImpersonateUserPrivTable();

        // 1. create all ip users
        Iterator<PrivEntry> iter = globalTable.getFullReadOnlyIterator();
        while (iter.hasNext()) {
            GlobalPrivEntry entry = (GlobalPrivEntry) iter.next();
            try {
                UserIdentity userIdentity = entry.getUserIdent();
                if (userIdentity.equals(UserIdentity.ROOT)) {
                    LOG.info("ignore root entry : {}", entry);
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
                    throw new AuthUpgradeUnrecoveredException("bad user priv entry " + entry, e);
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
                    byte[] p = whiteList.getPassword(hostname);
                    Password password = new Password(p, null, null);
                    UserIdentity user = UserIdentity.createAnalyzedUserIdentWithDomain(userName, hostname);
                    authenticationManager.upgradeUserUnlocked(user, password);
                }
            } catch (AuthenticationException e) {
                if (Config.ignore_invalid_privilege_authentications) {
                    LOG.warn("discard user {} whitelist {}", userName, whiteList, e);
                } else {
                    throw new AuthUpgradeUnrecoveredException("bad domain user  " + userName, e);
                }
            }
        }

        // 3. grant privileges on all ip users
        // must loop twice, otherwise impersonate may fail on non-existence user
        iter = globalTable.getFullReadOnlyIterator();
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
                UserPrivilegeCollection collection = new UserPrivilegeCollection();

                // 2. grant global privileges
                upgradeUserGlobalPrivileges(entry, collection);

                // 3. grant db privileges
                upgradeUserDbPrivileges(dbTable.getReadOnlyIteratorByUser(userIdentity), collection);

                // 4. grant table privilege
                upgradeUserTablePrivileges(tableTable.getReadOnlyIteratorByUser(userIdentity), collection);

                // 5. grant resource privileges
                upgradeUserResourcePrivileges(resourceTable.getReadOnlyIteratorByUser(userIdentity), collection);

                // 6. grant impersonate privileges
                upgradeUserImpersonate(impersonateUserPrivTable.getReadOnlyIteratorByUser(userIdentity), collection);

                // 7. set privilege to user
                privilegeManager.upgradeUserInitPrivilegeUnlock(userIdentity, collection);

            } catch (AuthUpgradeUnrecoveredException | PrivilegeException e) {
                if (Config.ignore_invalid_privilege_authentications) {
                    LOG.warn("discard user priv entry:{}\n{}", entry, entry.toGrantSQL(), e);
                } else {
                    throw new AuthUpgradeUnrecoveredException("bad user priv entry " + entry, e);
                }
            }
        } // for iter in globalTable

        // 4. grant privileges on all domain users
        // must loop twice, otherwise impersonate may fail on non-existence user
        upiter = auth.getPropertyMgr().propertyMap.entrySet().iterator();
        while (upiter.hasNext()) {
            Map.Entry<String, UserProperty> entry = upiter.next();
            String userName = entry.getKey();
            WhiteList whiteList = entry.getValue().getWhiteList();
            try {
                for (String hostname : whiteList.getAllDomains()) {
                    UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithDomain(userName, hostname);
                    UserPrivilegeCollection collection = new UserPrivilegeCollection();

                    // 1. grant global privileges
                    Iterator<PrivEntry> globalIter = globalTable.getReadOnlyIteratorByUser(userIdentity);
                    if (globalIter.hasNext()) {
                        upgradeUserGlobalPrivileges((GlobalPrivEntry) globalIter.next(), collection);
                    }

                    // 2. grant db privileges
                    upgradeUserDbPrivileges(dbTable.getReadOnlyIteratorByUser(userIdentity), collection);

                    // 3. grant table privilege
                    upgradeUserTablePrivileges(tableTable.getReadOnlyIteratorByUser(userIdentity), collection);

                    // 4. grant resource privileges
                    upgradeUserResourcePrivileges(resourceTable.getReadOnlyIteratorByUser(userIdentity), collection);

                    // 5. grant impersonate privileges
                    upgradeUserImpersonate(impersonateUserPrivTable.getReadOnlyIteratorByUser(userIdentity),
                            collection);

                    // 6. set privilege to user
                    privilegeManager.upgradeUserInitPrivilegeUnlock(userIdentity, collection);
                }
            } catch (AuthUpgradeUnrecoveredException | PrivilegeException e) {
                if (Config.ignore_invalid_privilege_authentications) {
                    LOG.warn("discard domain user priv for {}", userName);
                } else {
                    throw new AuthUpgradeUnrecoveredException("bad user priv for " + userName, e);
                }
            }
        } // for upiter in UserPropertyMap
    }

    protected void upgradeUserGlobalPrivileges(GlobalPrivEntry entry, UserPrivilegeCollection collection)
            throws PrivilegeException, AuthUpgradeUnrecoveredException {
        PrivBitSet bitSet = entry.getPrivSet();
        for (Privilege privilege : bitSet.toPrivilegeList()) {
            upgradeTablePrivileges(DbPrivEntry.ANY_DB, DbPrivEntry.ANY_DB, privilege, collection);
        }
    }

    protected void upgradeUserDbPrivileges(Iterator<PrivEntry> iterator, UserPrivilegeCollection collection)
            throws PrivilegeException, AuthUpgradeUnrecoveredException {
        while (iterator.hasNext()) {
            DbPrivEntry entry = (DbPrivEntry) iterator.next();
            PrivBitSet bitSet = entry.getPrivSet();
            for (Privilege privilege : bitSet.toPrivilegeList()) {
                upgradeTablePrivileges(entry.getOrigDb(), DbPrivEntry.ANY_DB, privilege, collection);
            }
        }
    }

    protected void upgradeUserTablePrivileges(Iterator<PrivEntry> iterator, UserPrivilegeCollection collection)
            throws PrivilegeException, AuthUpgradeUnrecoveredException {
        while (iterator.hasNext()) {
            TablePrivEntry entry = (TablePrivEntry) iterator.next();
            PrivBitSet bitSet = entry.getPrivSet();
            for (Privilege privilege : bitSet.toPrivilegeList()) {
                upgradeTablePrivileges(entry.getOrigDb(), entry.getOrigTbl(), privilege, collection);
            }
        }
    }

    protected void upgradeUserResourcePrivileges(Iterator<PrivEntry> iterator, UserPrivilegeCollection collection) {
        // TODO: resource object has not implemented yet
    }

    protected void upgradeUserImpersonate(Iterator<PrivEntry> iterator, UserPrivilegeCollection collection)
            throws PrivilegeException {
        while (iterator.hasNext()) {
            ImpersonateUserPrivEntry entry = (ImpersonateUserPrivEntry) iterator.next();
            upgradeImpersontaePrivileges(entry.getSecuredUserIdentity(), collection);
        }
    }

    /**
     * input param roleNameToId, can be null, meaning that leader is upgrading
     * return roleNameToId if it's leader
     */
    protected Map<String, Long> upgradeRole(Map<String, Long> roleNameToId) throws AuthUpgradeUnrecoveredException {
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
                    throw new AuthUpgradeUnrecoveredException(
                            "failed to upgrade role " + roleName + " while replaying!");
                }
            }

            Role role = entry.getValue();
            try {
                if (isBuiltInRoles(roleName)) {
                    // built roles has been automatically created
                    continue;
                }
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
                // grant role to user
                for (UserIdentity user : role.getUsers()) {
                    privilegeManager.upgradeUserRoleUnlock(user, roleId);
                }
            } catch (AuthUpgradeUnrecoveredException | PrivilegeException e) {
                if (Config.ignore_invalid_privilege_authentications) {
                    LOG.warn("discard role[{}] priv:{}", roleName, role, e);
                } else {
                    throw new AuthUpgradeUnrecoveredException("bad role priv " + roleName, e);
                }
            }
        }
        return ret;
    }

    protected void upgradeRoleTablePrivileges(
            Map<TablePattern, PrivBitSet> tblPatternToPrivs, RolePrivilegeCollection collection)
            throws PrivilegeException, AuthUpgradeUnrecoveredException {
        Iterator<Map.Entry<TablePattern, PrivBitSet>> iterator = tblPatternToPrivs.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<TablePattern, PrivBitSet> entry = iterator.next();
            TablePattern pattern = entry.getKey();
            PrivBitSet bitSet = entry.getValue();

            for (Privilege privilege : bitSet.toPrivilegeList()) {
                upgradeTablePrivileges(pattern.getQuolifiedDb(), pattern.getTbl(), privilege, collection);
            }
        }
    }

    protected void upgradeRoleResourcePrivileges(
            Map<ResourcePattern, PrivBitSet> resourcePatternToPrivs, RolePrivilegeCollection collection) {
        // TODO: resource object has not implemented yet
    }

    protected void upgradeRoleImpersonatePrivileges(Set<UserIdentity> impersonateUsers, RolePrivilegeCollection collection)
            throws PrivilegeException {
        Iterator<UserIdentity> iterator = impersonateUsers.iterator();
        while (iterator.hasNext()) {
            UserIdentity securedUser = iterator.next();
            upgradeImpersontaePrivileges(securedUser, collection);
        }
    }

    protected boolean isBuiltInRoles(String roleName) {
        return roleName.equals(Role.ADMIN_ROLE) || roleName.equals(Role.OPERATOR_ROLE);
    }

    protected void upgradeTablePrivileges(String db, String table, Privilege privilege, PrivilegeCollection collection)
            throws PrivilegeException, AuthUpgradeUnrecoveredException {
        switch (privilege) {
            case SELECT_PRIV: {
                List<PEntryObject> objects;
                if (db.equals(TablePattern.ALL.getQuolifiedDb())) {
                    objects = allTablesInAllDbObject;
                } else if (table.equals(TablePattern.ALL.getTbl())) {
                    // ALL TABLES in db
                    objects = Arrays.asList(privilegeManager.analyzeObject(
                            tableTypeStr, Arrays.asList(tableTypeStr), dbTypeStr, db));
                } else {
                    // db.table
                    objects = Arrays.asList(privilegeManager.analyzeObject(
                            tableTypeStr, Arrays.asList(db, table)));
                }
                collection.grant(tableTypeId, selectActionSet, objects, false);
                break;
            }

            default:
                throw new AuthUpgradeUnrecoveredException("table privilege " + privilege + " hasn't implemented");
        }
    }

    protected void upgradeImpersontaePrivileges(UserIdentity user, PrivilegeCollection collection)
            throws PrivilegeException {
        List<PEntryObject> objects = Arrays.asList(privilegeManager.analyzeUserObject(
                userTypeStr, user));
        collection.grant(userTypeId, impersonateActionSet, objects, false);
    }

    public static class AuthUpgradeUnrecoveredException extends Exception {
        public AuthUpgradeUnrecoveredException(String s) {
            super(s);
        }

        public AuthUpgradeUnrecoveredException(String s, Exception e) {
            super(s);
            initCause(e);
        }
    }
}
