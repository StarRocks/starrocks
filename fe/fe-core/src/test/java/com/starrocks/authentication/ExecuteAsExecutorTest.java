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
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorReportException;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ExecuteAsExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropUserStmt;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class ExecuteAsExecutorTest {

    @BeforeAll
    public static void setUpPersistJournal() throws Exception {
        // Real EditLog on an auto-committing pseudo journal (shields BDB): journal writes complete so the
        // WALApplier.apply() inside logJsonObject() still runs and the DDL takes effect in memory.
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterAll
    public static void tearDownPersistJournal() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private AuthenticationMgr authenticationMgr;
    private AuthorizationMgr authorizationMgr;

    @BeforeEach
    public void setUp() throws Exception {

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
     * End to end on the real managers: a member of an allowed group is impersonable with no CREATE USER,
     * and the roles mapped to its groups are what authorize the session.
     *
     * <p>Worth running against real managers rather than mocks because the native path has a gate beyond
     * the executor's own: {@code AuthorizationMgr.canExecuteAs} builds a {@link
     * com.starrocks.authorization.UserPEntryObject} for the target, and that used to insist the user
     * exists. A test that only drove the analyzer and the executor would pass while `EXECUTE AS` still
     * failed with AccessDenied under native access control.
     */
    @Test
    public void testExecuteAsExternalUserInAllowedGroup() throws Exception {
        String[] savedAllowedGroups = Config.execute_as_external_user_allowed_groups;
        try {
            Config.execute_as_external_user_allowed_groups = new String[] {"group1"};

            authorizationMgr.createRole(new CreateRoleStmt(List.of("r1"), true, ""));
            long roleId1 = authorizationMgr.getRoleIdByNameAllowNull("r1");
            authorizationMgr.grantRole(new GrantRoleStmt(List.of("r1"), "group1", GrantType.GROUP, NodePosition.ZERO));

            // Only the impersonator has an account. u1 and u2 exist in the directory alone.
            authenticationMgr.createUser(
                    new CreateUserStmt(new UserRef("impersonate_user", "%"), true, null, List.of(), Map.of(),
                            NodePosition.ZERO));
            // A per-user grant cannot name a user that does not exist, so ALL USERS is the only option.
            authorizationMgr.grant((GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                    "GRANT IMPERSONATE ON ALL USERS TO USER 'impersonate_user'@'%'", new ConnectContext()));

            ConnectContext context = new ConnectContext();
            AuthenticationHandler.authenticate(context, "impersonate_user", "%", MysqlPassword.EMPTY_PASSWORD);
            UserIdentity impersonator = context.getCurrentUserIdentity();

            // u1 is in group1, which is allowed: admitted, and authorized by the group's role.
            ExecuteAsStmt executeAsStmt = (ExecuteAsStmt) UtFrameUtils.parseStmtWithNewParser(
                    "EXECUTE AS u1 WITH NO REVERT", context);
            Authorizer.check(executeAsStmt, context);
            ExecuteAsExecutor.execute(executeAsStmt, context);

            UserIdentity current = context.getCurrentUserIdentity();
            Assertions.assertEquals("u1", current.getUser());
            Assertions.assertTrue(current.isEphemeral(), "a target with no account must be ephemeral");
            Assertions.assertEquals(Set.of("group1"), context.getGroups());
            Assertions.assertEquals(Set.of(roleId1), context.getCurrentRoleIds());

            // u2 is only in group2, which is not allowed. The refusal now happens in the executor, after
            // the IMPERSONATE check, and it must leave the session exactly as it was.
            ConnectContext other = new ConnectContext();
            AuthenticationHandler.authenticate(other, "impersonate_user", "%", MysqlPassword.EMPTY_PASSWORD);
            other.setGroups(Set.of("service-accounts"));
            ExecuteAsStmt refused = (ExecuteAsStmt) UtFrameUtils.parseStmtWithNewParser(
                    "EXECUTE AS u2 WITH NO REVERT", other);
            Authorizer.check(refused, other);
            DdlException e = Assertions.assertThrows(DdlException.class,
                    () -> ExecuteAsExecutor.execute(refused, other));
            Assertions.assertTrue(e.getMessage().contains("cannot find user"), e.getMessage());
            Assertions.assertEquals(impersonator, other.getCurrentUserIdentity(),
                    "a refused EXECUTE AS must leave the session as the impersonator");
            Assertions.assertEquals(Set.of("service-accounts"), other.getGroups(),
                    "a refused EXECUTE AS must not touch the session's groups");

            // An explicit host is refused even for the admitted member: Ranger uses the identity's host as
            // the request's client IP, so the text of a statement must not get to choose it.
            ExecuteAsStmt hosted = (ExecuteAsStmt) UtFrameUtils.parseStmtWithNewParser(
                    "EXECUTE AS 'u1'@'10.0.0.1' WITH NO REVERT", other);
            Authorizer.check(hosted, other);
            Assertions.assertThrows(DdlException.class, () -> ExecuteAsExecutor.execute(hosted, other));
        } finally {
            Config.execute_as_external_user_allowed_groups = savedAllowedGroups;
        }
    }

    /**
     * A grant left over from a dropped namesake must not cover the accountless name that replaces it.
     * `UserIdentity.equals()` ignores the ephemeral flag, so without the check in
     * {@code UserPEntryObject.match()} the old grant resurrects and `ON ALL USERS` stops being required.
     */
    @Test
    public void testGrantOutlivingItsUserDoesNotCoverTheExternalNamesake() throws Exception {
        String[] savedAllowedGroups = Config.execute_as_external_user_allowed_groups;
        try {
            Config.execute_as_external_user_allowed_groups = new String[] {"group1"};

            authenticationMgr.createUser(
                    new CreateUserStmt(new UserRef("impersonate_user", "%"), true, null, List.of(), Map.of(),
                            NodePosition.ZERO));
            authenticationMgr.createUser(
                    new CreateUserStmt(new UserRef("u1", "%"), true, null, List.of(), Map.of(), NodePosition.ZERO));
            authorizationMgr.grant((GrantPrivilegeStmt) UtFrameUtils.parseStmtWithNewParser(
                    "GRANT IMPERSONATE ON USER u1 TO USER 'impersonate_user'@'%'", new ConnectContext()));
            authenticationMgr.dropUser(new DropUserStmt(new UserRef("u1", "%"), false, NodePosition.ZERO));

            ConnectContext context = new ConnectContext();
            AuthenticationHandler.authenticate(context, "impersonate_user", "%", MysqlPassword.EMPTY_PASSWORD);

            // u1 is still in group1 in the directory, and the named grant is still on the books - but the
            // name has no account now, so only ON ALL USERS may cover it.
            ExecuteAsStmt executeAsStmt = (ExecuteAsStmt) UtFrameUtils.parseStmtWithNewParser(
                    "EXECUTE AS u1 WITH NO REVERT", context);
            Assertions.assertThrows(ErrorReportException.class, () -> Authorizer.check(executeAsStmt, context));
        } finally {
            Config.execute_as_external_user_allowed_groups = savedAllowedGroups;
        }
    }

    @Test
    public void testExecuteAsExternalUserIsOffByDefault() {
        Assertions.assertEquals(0, Config.execute_as_external_user_allowed_groups.length,
                "the feature must stay off unless an operator opts in");

        ConnectContext context = new ConnectContext();
        // u1 is in group1 in the directory, but nobody allowed that group and nobody created the user.
        // With the feature off the analyzer rejects, so this never reaches the executor at all.
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser("EXECUTE AS u1 WITH NO REVERT", context));
        Assertions.assertTrue(e.getMessage().contains("cannot find user"), e.getMessage());
    }
}
