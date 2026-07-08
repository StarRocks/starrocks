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
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

public class ExecuteAsExecutorTest {
    private AuthenticationMgr authenticationMgr;
    private AuthorizationMgr authorizationMgr;

    @BeforeEach
    public void setUp() throws Exception {
        // Mock EditLog
        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);

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

    /**
     * Verifies that EXECUTE AS EXTERNAL with a security integration loads the external user's groups
     * from the SI's dedicated group provider into the context. Those groups are exactly what
     * RangerStarRocksAccessController passes to Ranger via RangerStarRocksAccessRequest.setUserGroups(),
     * enabling group-based Ranger policies to apply to the impersonated external user.
     */
    @Test
    public void testExecuteAsExternalWithSecurityIntegrationForRanger() throws Exception {
        // Create a dedicated group provider for the security integration (separate from Config.group_provider)
        Map<String, String> siProviderProps = new HashMap<>();
        siProviderProps.put(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "ldap");
        siProviderProps.put(LDAPGroupProvider.LDAP_USER_SEARCH_ATTR, "uid");
        String siGroupProviderName = "si_group_provider";
        authenticationMgr.replayCreateGroupProvider(siGroupProviderName, siProviderProps);

        // Map the external user to Ranger groups via that group provider
        LDAPGroupProvider siGroupProvider = (LDAPGroupProvider) authenticationMgr.getGroupProvider(siGroupProviderName);
        Map<String, Set<String>> extGroupMap = new HashMap<>();
        extGroupMap.put("ext_user", Set.of("ranger_group1", "ranger_group2"));
        siGroupProvider.setUserToGroupCache(extGroupMap);

        // Create a security integration that references the dedicated group provider
        Map<String, String> siProps = new HashMap<>();
        siProps.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "ldap");
        siProps.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_GROUP_PROVIDER, siGroupProviderName);
        authenticationMgr.replayCreateSecurityIntegration("ldap_si", siProps);

        // Create roles and bind them to the Ranger groups so we can verify role resolution too
        authorizationMgr.createRole(new CreateRoleStmt(List.of("ranger_role1"), true, ""));
        authorizationMgr.createRole(new CreateRoleStmt(List.of("ranger_role2"), true, ""));
        authorizationMgr.grantRole(
                new GrantRoleStmt(List.of("ranger_role1"), "ranger_group1", GrantType.GROUP, NodePosition.ZERO));
        authorizationMgr.grantRole(
                new GrantRoleStmt(List.of("ranger_role2"), "ranger_group2", GrantType.GROUP, NodePosition.ZERO));

        // Create the internal service account that performs the impersonation
        authenticationMgr.createUser(
                new CreateUserStmt(new UserRef("svc_user", "%"), true, null, List.of(), Map.of(), NodePosition.ZERO));

        // Authenticate as svc_user; then set the security integration to simulate a session that
        // was originally established via the LDAP security integration
        ConnectContext context = new ConnectContext();
        AuthenticationHandler.authenticate(context, "svc_user", "%", MysqlPassword.EMPTY_PASSWORD);
        context.setSecurityIntegration("ldap_si");

        // Execute EXECUTE AS EXTERNAL 'ext_user'@'%' WITH NO REVERT
        ExecuteAsStmt stmt = new ExecuteAsStmt(
                new UserRef("ext_user", "%", false, true, NodePosition.ZERO), false);
        ExecuteAsExecutor.execute(stmt, context);

        // The external user's groups are loaded from the SI's group provider.
        // RangerStarRocksAccessController.check*() passes context.getGroups() directly into
        // RangerStarRocksAccessRequest.setUserGroups(), so Ranger evaluates group-based policies
        // using exactly the set asserted here.
        Assertions.assertEquals(Set.of("ranger_group1", "ranger_group2"), context.getGroups());

        // Roles derived from those groups are also active — the native access controller
        // (and any Ranger fallback) both honour them
        long roleId1 = authorizationMgr.getRoleIdByNameAllowNull("ranger_role1");
        long roleId2 = authorizationMgr.getRoleIdByNameAllowNull("ranger_role2");
        Assertions.assertEquals(Set.of(roleId1, roleId2), context.getCurrentRoleIds());

        // The identity is ephemeral — the external user has no entry in the internal user table
        Assertions.assertTrue(context.getCurrentUserIdentity().isEphemeral());
        Assertions.assertEquals("ext_user", context.getCurrentUserIdentity().getUser());

        // Verify groups come from the SI's own group provider, not from Config.group_provider.
        // The global ldap_group_provider in setUp has no entry for "ext_user", so if the SI
        // group provider were ignored, context.getGroups() would be empty.
        LDAPGroupProvider globalProvider =
                (LDAPGroupProvider) authenticationMgr.getGroupProvider("ldap_group_provider");
        Assertions.assertTrue(
                globalProvider.getGroup(context.getCurrentUserIdentity(), "ext_user").isEmpty(),
                "ext_user must NOT exist in the global group provider — confirms SI provider was used");
    }
}
