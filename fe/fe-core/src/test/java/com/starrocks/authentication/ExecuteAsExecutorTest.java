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

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.DefaultAuthorizationProvider;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorReportException;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ExecuteAsExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.GrantType;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.sql.ast.RevokeRoleStmt;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

public class ExecuteAsExecutorTest {
    private AuthenticationMgr authenticationMgr;
    private AuthorizationMgr authorizationMgr;
    private String[] savedGroupProvider;

    // setUp() points Config.group_provider at this class's provider. Without restoring it the static
    // FE config - and the warmed group cache behind it - stays mutated for the rest of the surefire
    // fork, so a later test that logs in u1/u2/u3 and expects no groups would fail.
    @AfterEach
    public void restoreGlobalConfig() {
        Config.group_provider = savedGroupProvider;
    }

    @BeforeEach
    public void setUp() throws Exception {
        // Mock EditLog
        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);
        savedGroupProvider = Config.group_provider;

        authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        authorizationMgr = new AuthorizationMgr(new DefaultAuthorizationProvider());
        GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorizationMgr);

        new MockUp<LDAPGroupProvider>() {
            @Mock
            public void init() throws DdlException {
                // do nothing
            }
        };

        Map<String, String> properties = new HashMap<>();
        properties.put(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "ldap");
        properties.put(LDAPGroupProvider.LDAP_USER_SEARCH_ATTR, "uid");

        String groupName = "ldap_group_provider";
        authenticationMgr.replayCreateGroupProvider(groupName, properties);
        Config.group_provider = new String[] {groupName};
        LDAPGroupProvider ldapGroupProvider = (LDAPGroupProvider) authenticationMgr.getGroupProvider(groupName);

        Map<String, Set<String>> groups = new HashMap<>();
        groups.put("u1", Set.of("group1"));
        groups.put("u2", Set.of("group2"));
        groups.put("u3", Set.of("group1", "group2"));
        ldapGroupProvider.setUserToGroupCache(groups);
    }

    @Test
    public void testExecuteAsGetGroups() throws Exception {
        authorizationMgr.createRole(new CreateRoleStmt(List.of("r1"), true, ""));
        authorizationMgr.createRole(new CreateRoleStmt(List.of("r2"), true, ""));

        authenticationMgr.createUser(
                new CreateUserStmt(new UserRef("impersonate_user", "%"), true, null, List.of(), Map.of(),
                        NodePosition.ZERO));
        authenticationMgr.createUser(
                new CreateUserStmt(new UserRef("u1", "%"), true, null, List.of("r1"), Map.of(), NodePosition.ZERO));
        authenticationMgr.createUser(
                new CreateUserStmt(new UserRef("u2", "%"), true, null, List.of("r2"), Map.of(), NodePosition.ZERO));

        long roleId1 = authorizationMgr.getRoleIdByNameAllowNull("r1");
        long roleId2 = authorizationMgr.getRoleIdByNameAllowNull("r2");

        // login as impersonate_user

        ConnectContext context = new ConnectContext();
        AuthenticationHandler.authenticate(context, "impersonate_user", "%", MysqlPassword.EMPTY_PASSWORD);

        Assertions.assertEquals("impersonate_user", context.getAccessControlContext().getQualifiedUser());
        Assertions.assertEquals(Set.of(), context.getGroups());

        ExecuteAsStmt executeAsStmt = new ExecuteAsStmt(new UserRef("u1", "%"), false);
        ExecuteAsExecutor.execute(executeAsStmt, context);
        Assertions.assertEquals(Set.of("group1"), context.getGroups());
        Assertions.assertEquals(Set.of(roleId1), context.getCurrentRoleIds());

        ExecuteAsStmt executeAsStmt2 = new ExecuteAsStmt(new UserRef("u2", "%"), false);
        ExecuteAsExecutor.execute(executeAsStmt2, context);
        Assertions.assertEquals(Set.of("group2"), context.getGroups());
        Assertions.assertEquals(Set.of(roleId2), context.getCurrentRoleIds());
    }

    /** memberOf value the stubbed directory publishes for whoever is asked about. */
    private static final String TARGET_MEMBEROF_GROUP = "SR Analysts";

    /**
     * Replace only the two methods that talk to the directory. Note that neither of them takes a
     * password: resolving somebody else's groups never needed their credential, which is what makes
     * the target-side resolution possible at all.
     */
    private static void stubDirectoryForAnyUser() {
        new MockUp<LDAPAuthProvider>() {
            @Mock
            protected LdapUserEntry findUserEntryByRoot(String user, String[] requestedAttributes) {
                BasicAttributes attributes = new BasicAttributes(true);
                attributes.put(new BasicAttribute("memberOf",
                        "CN=" + TARGET_MEMBEROF_GROUP + ",OU=Groups,DC=starrocks,DC=com"));
                return new LdapUserEntry("uid=" + user + ",ou=People,dc=starrocks,dc=com", attributes);
            }

            @Mock
            protected void checkPassword(String dn, String password) {
            }
        };
    }

    /**
     * The target of `EXECUTE AS` is never authenticated - there is no credential for it - but its
     * groups can still be resolved, because reading another entry's attributes only needs the
     * service account. A pre-created LDAP user resolves through the FE configuration, exactly as its
     * own login would.
     */
    @Test
    public void testExecuteAsResolvesTheMemberOfGroupsOfTheTargetUser() throws Exception {
        stubDirectoryForAnyUser();
        String savedGroupSource = Config.authentication_ldap_simple_group_source;
        String savedRootDn = Config.authentication_ldap_simple_bind_root_dn;
        String savedRootPwd = Config.authentication_ldap_simple_bind_root_pwd;
        try {
            Config.authentication_ldap_simple_group_source = "both";
            Config.authentication_ldap_simple_bind_root_dn = "cn=svc,dc=starrocks,dc=com";
            Config.authentication_ldap_simple_bind_root_pwd = "secret";

            authenticationMgr.createUser(
                    new CreateUserStmt(new UserRef("impersonate_user", "%"), true, null, List.of(), Map.of(),
                            NodePosition.ZERO));
            // u1 is an LDAP user declared without AS '<dn>', and the group provider knows it as well.
            authenticationMgr.createUser(new CreateUserStmt(
                    new UserRef("u1", "%", false, NodePosition.ZERO), true,
                    new UserAuthOption("AUTHENTICATION_LDAP_SIMPLE", null, false, NodePosition.ZERO),
                    List.of(), Map.of(), NodePosition.ZERO));

            ConnectContext context = new ConnectContext();
            AuthenticationHandler.authenticate(context, "impersonate_user", "%", MysqlPassword.EMPTY_PASSWORD);

            ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserRef("u1", "%"), false), context);

            // Both of the target's own sources: its group provider entry and its own memberOf.
            Assertions.assertEquals(Set.of("group1", TARGET_MEMBEROF_GROUP), context.getGroups());
        } finally {
            Config.authentication_ldap_simple_group_source = savedGroupSource;
            Config.authentication_ldap_simple_bind_root_dn = savedRootDn;
            Config.authentication_ldap_simple_bind_root_pwd = savedRootPwd;
        }
    }

    /**
     * `EXECUTE AS EXTERNAL USER` has no catalog entry to look the target's method up in, so the
     * security integration has to be chosen from `authentication_chain`. Choosing "the first LDAP
     * integration" is not enough: a chain often carries more than one - a leftover from an earlier
     * setup, or two directories side by side - and the first may not read memberOf at all. The chain
     * is a fallback list on the login path, and it has to behave like one here too.
     * <p>
     * Without this, the target resolved to no groups at all while an ordinary login for the same user
     * resolved them fine, which is exactly how it showed up in the system test.
     */
    @Test
    public void testExecuteAsExternalUserSkipsAnIntegrationThatCannotResolveTheTarget() throws Exception {
        stubDirectoryForAnyUser();
        String savedGroupSource = Config.authentication_ldap_simple_group_source;
        String savedRootDn = Config.authentication_ldap_simple_bind_root_dn;
        String savedRootPwd = Config.authentication_ldap_simple_bind_root_pwd;
        String[] savedChain = Config.authentication_chain;
        try {
            Config.authentication_ldap_simple_bind_root_dn = "cn=svc,dc=starrocks,dc=com";
            Config.authentication_ldap_simple_bind_root_pwd = "secret";
            Config.authentication_ldap_simple_group_source = "group_provider";

            // First in the chain: an LDAP integration that does NOT read memberOf (the leftover).
            authenticationMgr.replayCreateSecurityIntegration("stale_ldap", ldapIntegrationProperties("group_provider"));
            // Second: the one that is actually configured for memberOf.
            authenticationMgr.replayCreateSecurityIntegration("real_ldap", ldapIntegrationProperties("memberof"));
            Config.authentication_chain = new String[] {"stale_ldap", "real_ldap", "native"};

            authenticationMgr.createUser(
                    new CreateUserStmt(new UserRef("impersonate_user", "%"), true, null, List.of(), Map.of(),
                            NodePosition.ZERO));

            ConnectContext context = new ConnectContext();
            AuthenticationHandler.authenticate(context, "impersonate_user", "%", MysqlPassword.EMPTY_PASSWORD);

            ExecuteAsExecutor.execute(
                    new ExecuteAsStmt(new UserRef("u1", "%", false, true, NodePosition.ZERO), false), context);

            Assertions.assertTrue(context.getGroups().contains(TARGET_MEMBEROF_GROUP),
                    "the second integration in the chain should have resolved the target, got "
                            + context.getGroups());
        } finally {
            Config.authentication_ldap_simple_group_source = savedGroupSource;
            Config.authentication_ldap_simple_bind_root_dn = savedRootDn;
            Config.authentication_ldap_simple_bind_root_pwd = savedRootPwd;
            Config.authentication_chain = savedChain;
        }
    }

    private static Map<String, String> ldapIntegrationProperties(String groupSource) {
        Map<String, String> properties = new HashMap<>();
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "authentication_ldap_simple");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_SERVER_HOST, "localhost");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_ROOT_DN, "cn=svc,dc=starrocks,dc=com");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_ROOT_PWD, "secret");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_BIND_BASE_DN, "dc=starrocks,dc=com");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_USER_SEARCH_ATTR, "uid");
        properties.put(SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_GROUP_SOURCE, groupSource);
        return properties;
    }

    /**
     * A native-password target has no LDAP identity, so nothing is read from a directory for it even
     * when the FE configuration turns memberOf on for LDAP users.
     */
    @Test
    public void testExecuteAsToANativeUserReadsNoDirectory() throws Exception {
        stubDirectoryForAnyUser();
        String savedGroupSource = Config.authentication_ldap_simple_group_source;
        try {
            Config.authentication_ldap_simple_group_source = "both";

            authenticationMgr.createUser(
                    new CreateUserStmt(new UserRef("impersonate_user", "%"), true, null, List.of(), Map.of(),
                            NodePosition.ZERO));
            authenticationMgr.createUser(
                    new CreateUserStmt(new UserRef("u1", "%"), true, null, List.of(), Map.of(), NodePosition.ZERO));

            ConnectContext context = new ConnectContext();
            AuthenticationHandler.authenticate(context, "impersonate_user", "%", MysqlPassword.EMPTY_PASSWORD);

            ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserRef("u1", "%"), false), context);

            Assertions.assertEquals(Set.of("group1"), context.getGroups());
            Assertions.assertFalse(context.getGroups().contains(TARGET_MEMBEROF_GROUP));
        } finally {
            Config.authentication_ldap_simple_group_source = savedGroupSource;
        }
    }

    /**
     * A group provider without `ldap_user_search_attr` keys its cache by DN. Resolving the target's
     * own entry produces that DN as a by-product, so the provider can finally be asked with something
     * it can match - before, this path only had a login name to offer and such a provider always
     * came back empty after `EXECUTE AS`.
     */
    @Test
    public void testExecuteAsAsksADnKeyedGroupProviderWithTheResolvedDn() throws Exception {
        stubDirectoryForAnyUser();
        String savedGroupSource = Config.authentication_ldap_simple_group_source;
        String savedRootDn = Config.authentication_ldap_simple_bind_root_dn;
        String savedRootPwd = Config.authentication_ldap_simple_bind_root_pwd;
        String[] savedGroupProvider = Config.group_provider;
        try {
            Config.authentication_ldap_simple_group_source = "both";
            Config.authentication_ldap_simple_bind_root_dn = "cn=svc,dc=starrocks,dc=com";
            Config.authentication_ldap_simple_bind_root_pwd = "secret";

            // No ldap_user_search_attr -> the cache is keyed by the (lower-cased) DN.
            Map<String, String> properties = new HashMap<>();
            properties.put(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "ldap");
            authenticationMgr.replayCreateGroupProvider("dn_keyed_provider", properties);
            LDAPGroupProvider dnKeyed =
                    (LDAPGroupProvider) authenticationMgr.getGroupProvider("dn_keyed_provider");
            Map<String, Set<String>> cache = new HashMap<>();
            cache.put("uid=u1,ou=people,dc=starrocks,dc=com", Set.of("Platform Ops"));
            dnKeyed.setUserToGroupCache(cache);
            Config.group_provider = new String[] {"dn_keyed_provider"};

            authenticationMgr.createUser(
                    new CreateUserStmt(new UserRef("impersonate_user", "%"), true, null, List.of(), Map.of(),
                            NodePosition.ZERO));
            authenticationMgr.createUser(new CreateUserStmt(
                    new UserRef("u1", "%", false, NodePosition.ZERO), true,
                    new UserAuthOption("AUTHENTICATION_LDAP_SIMPLE", null, false, NodePosition.ZERO),
                    List.of(), Map.of(), NodePosition.ZERO));

            ConnectContext context = new ConnectContext();
            AuthenticationHandler.authenticate(context, "impersonate_user", "%", MysqlPassword.EMPTY_PASSWORD);

            ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserRef("u1", "%"), false), context);

            Assertions.assertEquals(Set.of("Platform Ops", TARGET_MEMBEROF_GROUP), context.getGroups());
        } finally {
            Config.authentication_ldap_simple_group_source = savedGroupSource;
            Config.authentication_ldap_simple_bind_root_dn = savedRootDn;
            Config.authentication_ldap_simple_bind_root_pwd = savedRootPwd;
            Config.group_provider = savedGroupProvider;
        }
    }

    /**
     * AC-8d: the groups read from the *original* user's own LDAP entry must not follow the session
     * into the target identity. The group set is replaced anyway, so this also pins the explicit
     * clearing of the carrier field - drop that line and the last assertion fails, which is the
     * point: it stops the guarantee from resting on "nothing happens to read the field".
     */
    @Test
    public void testExecuteAsDropsTheMemberOfGroupsOfTheOriginalUser() throws Exception {
        authenticationMgr.createUser(
                new CreateUserStmt(new UserRef("impersonate_user", "%"), true, null, List.of(), Map.of(),
                        NodePosition.ZERO));
        authenticationMgr.createUser(
                new CreateUserStmt(new UserRef("u1", "%"), true, null, List.of(), Map.of(), NodePosition.ZERO));

        ConnectContext context = new ConnectContext();
        AuthenticationHandler.authenticate(context, "impersonate_user", "%", MysqlPassword.EMPTY_PASSWORD);

        // Stand in for a login that resolved groups from the original user's own entry.
        String originalOnly = "Original Only Group";
        context.getAccessControlContext().setMemberOfGroups(Set.of(originalOnly));
        context.setGroups(Set.of(originalOnly));

        ExecuteAsExecutor.execute(new ExecuteAsStmt(new UserRef("u1", "%"), false), context);

        // u1 gets its own groups from the group provider, and nothing of the original user's.
        Assertions.assertEquals(Set.of("group1"), context.getGroups());
        Assertions.assertFalse(context.getGroups().contains(originalOnly));
        Assertions.assertTrue(context.getAccessControlContext().getMemberOfGroups().isEmpty(),
                "the memberOf groups of the original user must not survive the identity switch");
    }

    @Test
    public void testExecuteAsGroupWithRoles() throws Exception {
        authorizationMgr.createRole(new CreateRoleStmt(List.of("r1"), true, ""));
        authorizationMgr.createRole(new CreateRoleStmt(List.of("r2"), true, ""));
        authorizationMgr.createRole(new CreateRoleStmt(List.of("r3"), true, ""));

        authenticationMgr.createUser(
                new CreateUserStmt(new UserRef("impersonate_user", "%"), true, null, List.of(), Map.of(), NodePosition.ZERO));
        authenticationMgr.createUser(
                new CreateUserStmt(new UserRef("u1", "%"), true, null, List.of("r1"), Map.of(), NodePosition.ZERO));
        authenticationMgr.createUser(
                new CreateUserStmt(new UserRef("u2", "%"), true, null, List.of("r2"), Map.of(), NodePosition.ZERO));
        authenticationMgr.createUser(
                new CreateUserStmt(new UserRef("u3", "%"), true, null, List.of("r3"), Map.of(), NodePosition.ZERO));

        long roleId1 = authorizationMgr.getRoleIdByNameAllowNull("r1");
        long roleId2 = authorizationMgr.getRoleIdByNameAllowNull("r2");
        long roleId3 = authorizationMgr.getRoleIdByNameAllowNull("r3");

        authorizationMgr.grantRole(new GrantRoleStmt(List.of("r1"), "group1", GrantType.GROUP, NodePosition.ZERO));
        authorizationMgr.grantRole(new GrantRoleStmt(List.of("r2"), "group2", GrantType.GROUP, NodePosition.ZERO));

        // login as impersonate_user

        ConnectContext context = new ConnectContext();
        AuthenticationHandler.authenticate(context, "impersonate_user", "%", MysqlPassword.EMPTY_PASSWORD);

        Assertions.assertEquals("impersonate_user", context.getAccessControlContext().getQualifiedUser());
        Assertions.assertEquals(Set.of(), context.getGroups());

        ExecuteAsStmt executeAsStmt = new ExecuteAsStmt(new UserRef("u1", "%"), false);
        ExecuteAsExecutor.execute(executeAsStmt, context);
        Assertions.assertEquals(Set.of("group1"), context.getGroups());
        Assertions.assertEquals(Set.of(roleId1), context.getCurrentRoleIds());

        ExecuteAsStmt executeAsStmt2 = new ExecuteAsStmt(new UserRef("u2", "%"), false);
        ExecuteAsExecutor.execute(executeAsStmt2, context);
        Assertions.assertEquals(Set.of("group2"), context.getGroups());
        Assertions.assertEquals(Set.of(roleId2), context.getCurrentRoleIds());

        ExecuteAsStmt executeAsStmt3 = new ExecuteAsStmt(new UserRef("u3", "%"), false);
        ExecuteAsExecutor.execute(executeAsStmt3, context);
        Assertions.assertEquals(Set.of("group1", "group2"), context.getGroups());
        Assertions.assertEquals(Set.of(roleId1, roleId2, roleId3), context.getCurrentRoleIds());

        ExecuteAsStmt executeAsStmt4 =
                new ExecuteAsStmt(new UserRef("impersonate_user", "%", false, true, NodePosition.ZERO), false);
        ExecuteAsExecutor.execute(executeAsStmt4, context);
        Assertions.assertEquals(Set.of(), context.getGroups());
        Assertions.assertEquals(Set.of(), context.getCurrentRoleIds());
    }

    @Test
    public void testImpersonatePermissionWithRoleGroupUser() throws Exception {
        // Create roles
        authorizationMgr.createRole(new CreateRoleStmt(List.of("impersonate_role"), true, ""));

        // Create users
        authenticationMgr.createUser(
                new CreateUserStmt(new UserRef("admin_user", "%"), true, null, List.of(), Map.of(), NodePosition.ZERO));
        authenticationMgr.createUser(
                new CreateUserStmt(new UserRef("target_user", "%"), true, null, List.of(), Map.of(), NodePosition.ZERO));
        authenticationMgr.createUser(
                new CreateUserStmt(new UserRef("group_user", "%"), true, null, List.of(), Map.of(), NodePosition.ZERO));

        // Grant impersonate permission to role
        GrantPrivilegeStmt grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT IMPERSONATE ON USER target_user TO ROLE impersonate_role",
                new ConnectContext());
        authorizationMgr.grant(grantStmt);

        // Grant role to external group
        GrantRoleStmt grantRoleStmt =
                new GrantRoleStmt(List.of("impersonate_role"), "test_group", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgr.grantRole(grantRoleStmt);

        // Set up LDAP group mapping for group_user to belong to test_group
        LDAPGroupProvider ldapGroupProvider = (LDAPGroupProvider) authenticationMgr.getGroupProvider("ldap_group_provider");
        Map<String, Set<String>> groups = new HashMap<>();
        groups.put("group_user", Set.of("test_group"));
        ldapGroupProvider.setUserToGroupCache(groups);

        // Test 1: User with impersonate permission through role-group can execute as target user
        ConnectContext context = new ConnectContext();
        AuthenticationHandler.authenticate(context, "group_user", "%", MysqlPassword.EMPTY_PASSWORD);

        // Verify user has the role through group membership
        long roleId = authorizationMgr.getRoleIdByNameAllowNull("impersonate_role");
        Assertions.assertEquals(Set.of("test_group"), context.getGroups());
        Assertions.assertEquals(Set.of(roleId), context.getCurrentRoleIds());

        // Verify impersonate permission check passes
        UserIdentity targetUser = new UserIdentity("target_user", "%");
        try {
            Authorizer.checkUserAction(context, targetUser, PrivilegeType.IMPERSONATE);
            // If no exception is thrown, permission check passed
        } catch (AccessDeniedException e) {
            Assertions.fail("User should have impersonate permission through role-group membership");
        }

        // Execute as target user should succeed
        ExecuteAsStmt executeAsStmt = new ExecuteAsStmt(new UserRef("target_user", "%"), false);
        ExecuteAsExecutor.execute(executeAsStmt, context);
        Assertions.assertEquals("target_user", context.getAccessControlContext().getCurrentUserIdentity().getUser());

        // Test 2: Revoke impersonate permission from role
        RevokePrivilegeStmt revokeStmt = (RevokePrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "REVOKE IMPERSONATE ON USER target_user FROM ROLE impersonate_role",
                new ConnectContext());
        authorizationMgr.revoke(revokeStmt);

        // Re-authenticate to refresh context
        AuthenticationHandler.authenticate(context, "group_user", "%", MysqlPassword.EMPTY_PASSWORD);

        // Verify user still has the role through group membership
        Assertions.assertEquals(Set.of("test_group"), context.getGroups());
        Assertions.assertEquals(Set.of(roleId), context.getCurrentRoleIds());

        // Verify impersonate permission check now fails
        Assertions.assertThrows(AccessDeniedException.class,
                () -> Authorizer.checkUserAction(context, targetUser, PrivilegeType.IMPERSONATE));

        // Execute as target user should fail
        Assertions.assertThrows(ErrorReportException.class, () -> Authorizer.check(executeAsStmt, context));

        // Test 3: Grant impersonate permission back to role
        grantStmt = (GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                "GRANT IMPERSONATE ON USER target_user TO ROLE impersonate_role",
                new ConnectContext());
        authorizationMgr.grant(grantStmt);

        // Re-authenticate to refresh context
        AuthenticationHandler.authenticate(context, "group_user", "%", MysqlPassword.EMPTY_PASSWORD);

        // Verify impersonate permission check passes again
        try {
            Authorizer.checkUserAction(context, targetUser, PrivilegeType.IMPERSONATE);
            // If no exception is thrown, permission check passed
        } catch (AccessDeniedException e) {
            Assertions.fail("User should have impersonate permission after re-granting to role");
        }

        // Execute as target user should succeed again
        ExecuteAsExecutor.execute(executeAsStmt, context);
        Assertions.assertEquals("target_user", context.getAccessControlContext().getCurrentUserIdentity().getUser());

        // Test 4: Revoke role from group
        RevokeRoleStmt
                revokeRoleStmt =
                new RevokeRoleStmt(List.of("impersonate_role"), "test_group", GrantType.GROUP, NodePosition.ZERO);
        authorizationMgr.revokeRole(revokeRoleStmt);

        // Re-authenticate to refresh context
        AuthenticationHandler.authenticate(context, "group_user", "%", MysqlPassword.EMPTY_PASSWORD);

        // Verify user no longer has the role
        Assertions.assertEquals(Set.of("test_group"), context.getGroups());
        Assertions.assertEquals(Set.of(), context.getCurrentRoleIds());

        // Verify impersonate permission check fails
        Assertions.assertThrows(AccessDeniedException.class,
                () -> Authorizer.checkUserAction(context, targetUser, PrivilegeType.IMPERSONATE));

        // Execute as target user should fail
        Assertions.assertThrows(ErrorReportException.class, () -> Authorizer.check(executeAsStmt, context));
    }
}
