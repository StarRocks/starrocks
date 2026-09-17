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

package com.starrocks.scheduler;

import com.starrocks.authentication.AuthenticationHandler;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.GroupProvider;
import com.starrocks.authentication.LDAPAuthProvider;
import com.starrocks.authentication.LDAPGroupProvider;
import com.starrocks.authentication.LdapUserEntry;
import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.authentication.SimpleLDAPSecurityIntegration;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.DefaultAuthorizationProvider;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ExecuteAsExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.naming.NamingException;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AITaskExecutionIdentityTest {
    private boolean creatorAuthorization;

    @BeforeEach
    public void setUp() {
        UtFrameUtils.mockInitWarehouseEnv();
        creatorAuthorization = Config.mv_use_creator_based_authorization;
        Config.mv_use_creator_based_authorization = false;
    }

    @AfterEach
    public void tearDown() {
        Config.mv_use_creator_based_authorization = creatorAuthorization;
        ConnectContext.remove();
    }

    @Test
    public void testAISubmitRejectsAnUnauthenticatedContext() {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setQualifiedUser("root");
        context.setThreadLocalInfo();
        SubmitTaskStmt statement = resolvedAISubmit(context);

        SemanticException error = assertThrows(SemanticException.class, () -> TaskBuilder.buildTask(statement, context));
        assertTrue(error.getMessage().contains("authenticated"));
    }

    @Test
    public void testDisabledAuthenticationCannotEstablishAITaskProvenance() throws Exception {
        boolean previousAuthCheck = Config.enable_auth_check;
        try {
            Config.enable_auth_check = false;
            ConnectContext context = authenticateRoot();
            assertThrows(SemanticException.class, () -> TaskBuilder.buildTask(resolvedAISubmit(context), context));
        } finally {
            Config.enable_auth_check = previousAuthCheck;
        }
    }

    @Test
    public void testAISubmitPersistsAuthenticatedIdentityWithoutSessionSecrets() throws Exception {
        ConnectContext context = authenticateRoot();
        context.setAuthToken("private-session-token");
        Task task = TaskBuilder.buildTask(resolvedAISubmit(context), context);
        String serialized = GsonUtils.GSON.toJson(task);
        assertTrue(serialized.contains("\"executionIdentity\""));
        assertTrue(serialized.contains("\"version\":1"));
        assertFalse(serialized.contains("private-session-token"));
        assertFalse(serialized.contains("currentRoleIds"));
        assertFalse(serialized.contains("groups"));
    }

    @Test
    public void testExecuteAsCannotReuseOriginalAuthenticationProvenance() throws Exception {
        ConnectContext context = authenticateRoot();
        ExecuteAsStmt executeAs = (ExecuteAsStmt) SqlParser.parseSingleStatement(
                "execute as 'root' with no revert", context.getSessionVariable().getSqlMode());
        ExecuteAsExecutor.execute(executeAs, context);
        assertThrows(SemanticException.class, () -> TaskBuilder.buildTask(resolvedAISubmit(context), context));
    }

    @Test
    public void testMismatchedDNCannotBeCapturedAsCreatorIdentity() throws Exception {
        ConnectContext context = authenticateRoot();
        context.setDistinguishedName("uid=another_user,dc=example,dc=com");
        assertThrows(SemanticException.class, () -> TaskBuilder.buildTask(resolvedAISubmit(context), context));
    }

    @Test
    public void testReplayedAIIdentityCannotFallBackToRootForDeletedUser() {
        Task task = replayTask("missing_ai_task_creator", 1);
        SemanticException error = assertThrows(SemanticException.class,
                () -> taskRun(task).buildTaskRunConnectContext());
        assertTrue(error.getMessage().contains("no longer exists"));
    }

    @Test
    public void testUnknownIdentityVersionCannotFallBackToRoot() {
        Task task = replayTask("root", 99);
        SemanticException error = assertThrows(SemanticException.class,
                () -> taskRun(task).buildTaskRunConnectContext());
        assertTrue(error.getMessage().contains("version"));
    }

    @Test
    public void testIncompletePersistedIdentityCannotBecomeNative() {
        Task task = GsonUtils.GSON.fromJson("{\"name\":\"incomplete_ai\","
                + "\"executionIdentity\":{\"version\":1,\"user\":\"root\",\"host\":\"%\"}}", Task.class);
        SemanticException error = assertThrows(SemanticException.class,
                () -> taskRun(task).buildTaskRunConnectContext());
        assertTrue(error.getMessage().contains("incomplete"));
    }

    @Test
    public void testDeletedGroupProviderCannotBeSilentlySkipped() {
        String[] previousProviders = Config.group_provider;
        try {
            Config.group_provider = new String[] {"missing_ai_task_group_provider"};
            SemanticException error = assertThrows(SemanticException.class,
                    () -> taskRun(replayTask("root", 1)).buildTaskRunConnectContext());
            assertTrue(error.getMessage().contains("group provider"));
        } finally {
            Config.group_provider = previousProviders;
        }
    }

    @Test
    public void testGroupsAreResolvedAgainForEveryRun() throws Exception {
        AuthenticationMgr previousManager = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        String[] previousProviders = Config.group_provider;
        try {
            new MockUp<LDAPGroupProvider>() {
                @Mock
                public void init() throws DdlException {
                }
            };
            AuthenticationMgr manager = new AuthenticationMgr();
            GlobalStateMgr.getCurrentState().setAuthenticationMgr(manager);
            manager.replayCreateGroupProvider("task_groups", Map.of(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "ldap"));
            Config.group_provider = new String[] {"task_groups"};
            LDAPGroupProvider provider = (LDAPGroupProvider) manager.getGroupProvider("task_groups");
            Task task = replayTask("root", 1);

            provider.setUserToGroupCache(Map.of("root", Set.of("before")));
            assertEquals(Set.of("before"), taskRun(task).buildTaskRunConnectContext().getGroups());
            provider.setUserToGroupCache(Map.of("root", Set.of("after")));
            assertEquals(Set.of("after"), taskRun(task).buildTaskRunConnectContext().getGroups());
        } finally {
            Config.group_provider = previousProviders;
            GlobalStateMgr.getCurrentState().setAuthenticationMgr(previousManager);
        }
    }

    @Test
    public void testEphemeralIdentitySurvivesReplayWithoutNativeRootRoles() throws Exception {
        AuthenticationMgr previousManager = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        AuthorizationMgr previousAuthorization = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        String[] previousChain = Config.authentication_chain;
        UtFrameUtils.setUpForPersistTest();
        try {
            new MockUp<LDAPGroupProvider>() {
                @Mock
                public void init() {
                }
            };
            AuthenticationMgr manager = new AuthenticationMgr();
            GlobalStateMgr.getCurrentState().setAuthenticationMgr(manager);
            AuthorizationMgr authorization = new AuthorizationMgr(new DefaultAuthorizationProvider());
            GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorization);
            manager.replayCreateGroupProvider("task_groups", Map.of(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "ldap"));
            LDAPGroupProvider provider = (LDAPGroupProvider) manager.getGroupProvider("task_groups");
            provider.setUserToGroupCache(Map.of("uid=external_root,dc=example", Set.of("task_group")));
            manager.replayCreateSecurityIntegration("task_external", Map.of(
                    SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "AUTHENTICATION_LDAP_SIMPLE",
                    SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_GROUP_PROVIDER, "task_groups",
                    SecurityIntegration.SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN, "task_group"));
            Config.authentication_chain = new String[] {"task_external"};
            ConnectContext root = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
            execute(root, "CREATE ROLE task_role");
            execute(root, "GRANT USAGE ON AI FUNCTION ai_complete TO ROLE task_role");
            execute(root, "GRANT task_role TO EXTERNAL GROUP task_group");
            Task task = replayExternalTask();
            Task replayed = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(task), Task.class);
            ConnectContext restored = taskRun(replayed).buildTaskRunConnectContext();
            assertTrue(restored.getCurrentUserIdentity().isEphemeral());
            assertEquals("task_external", restored.getSecurityIntegration());
            assertEquals("uid=external_root,dc=example", restored.getDistinguishedName());
            assertEquals(Set.of("task_group"), restored.getGroups());
            assertEquals(Set.of(authorization.getRoleIdByNameAllowNull("task_role")), restored.getCurrentRoleIds(),
                    "Ephemeral root must only inherit current group roles, not native root roles");
            Authorizer.checkAIFunctionAction(restored, "ai_complete", PrivilegeType.USAGE);

            execute(root, "REVOKE task_role FROM EXTERNAL GROUP task_group");
            ConnectContext revoked = taskRun(replayed).buildTaskRunConnectContext();
            assertEquals(Set.of("task_group"), revoked.getGroups());
            assertTrue(revoked.getCurrentRoleIds().isEmpty());
            assertThrows(AccessDeniedException.class,
                    () -> Authorizer.checkAIFunctionAction(revoked, "ai_complete", PrivilegeType.USAGE));

            execute(root, "GRANT task_role TO EXTERNAL GROUP task_group");
            Authorizer.checkAIFunctionAction(taskRun(replayed).buildTaskRunConnectContext(),
                    "ai_complete", PrivilegeType.USAGE);
            manager.replayAlterSecurityIntegration("task_external", Map.of(
                    SecurityIntegration.SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN, "another_group"));
            SemanticException error = assertThrows(SemanticException.class,
                    () -> taskRun(replayed).buildTaskRunConnectContext());
            assertTrue(error.getMessage().contains("permitted security integration group"));

            manager.replayDropSecurityIntegration("task_external");
            error = assertThrows(SemanticException.class, () -> taskRun(replayed).buildTaskRunConnectContext());
            assertTrue(error.getMessage().contains("security integration no longer exists"));
        } finally {
            Config.authentication_chain = previousChain;
            GlobalStateMgr.getCurrentState().setAuthenticationMgr(previousManager);
            GlobalStateMgr.getCurrentState().setAuthorizationMgr(previousAuthorization);
            UtFrameUtils.tearDownForPersisTest();
        }
    }

    @Test
    public void testDisabledSecurityIntegrationCannotBeRestored() {
        AuthenticationMgr previousManager = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        String[] previousChain = Config.authentication_chain;
        try {
            AuthenticationMgr manager = new AuthenticationMgr();
            GlobalStateMgr.getCurrentState().setAuthenticationMgr(manager);
            manager.replayCreateSecurityIntegration("task_external", Map.of(
                    SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "AUTHENTICATION_LDAP_SIMPLE"));
            Config.authentication_chain = new String[] {"native"};
            assertThrows(SemanticException.class, () -> taskRun(replayExternalTask()).buildTaskRunConnectContext());
        } finally {
            Config.authentication_chain = previousChain;
            GlobalStateMgr.getCurrentState().setAuthenticationMgr(previousManager);
        }
    }

    @Test
    public void testNativeLDAPMemberOfExcludesProviderRoles() throws Exception {
        try (LDAPTaskFixture fixture = new LDAPTaskFixture("memberof")) {
            ConnectContext restored = fixture.restore(false);
            assertEquals(Set.of("DirectoryGroup"), restored.getGroups());
            assertTrue(restored.getCurrentRoleIds().contains(fixture.directoryRole()));
            assertFalse(restored.getCurrentRoleIds().contains(fixture.providerRole()));
            assertThrows(AccessDeniedException.class,
                    () -> Authorizer.checkAIFunctionAction(restored, "ai_complete", PrivilegeType.USAGE));

            // A provider excluded by the current source cannot prevent restoration either.
            Config.group_provider = new String[] {"missing_ignored_provider"};
            assertEquals(Set.of("DirectoryGroup"), fixture.restore(false).getGroups());
        }
    }

    @Test
    public void testExternalLDAPMemberOfUsesOriginalIntegration() throws Exception {
        try (LDAPTaskFixture fixture = new LDAPTaskFixture("memberof")) {
            fixture.manager.replayCreateSecurityIntegration("other_ldap", Map.of(
                    SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "AUTHENTICATION_LDAP_SIMPLE",
                    SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_GROUP_SOURCE, "group_provider"));
            Config.authentication_chain = new String[] {"other_ldap", "task_external"};
            // The SI overrides this global policy. The earlier SI must not replace the stored one.
            Config.authentication_ldap_simple_group_source = "group_provider";
            ConnectContext restored = fixture.restore(true);
            assertEquals("task_external", restored.getSecurityIntegration());
            assertEquals(Set.of("DirectoryGroup"), restored.getGroups());
            assertEquals(Set.of(fixture.directoryRole()), restored.getCurrentRoleIds());
            assertThrows(AccessDeniedException.class,
                    () -> Authorizer.checkAIFunctionAction(restored, "ai_complete", PrivilegeType.USAGE));
        }
    }

    @Test
    public void testLDAPBothRefreshesMembershipAndRoleRevocation() throws Exception {
        try (LDAPTaskFixture fixture = new LDAPTaskFixture("both")) {
            ConnectContext restored = fixture.restore(true);
            assertEquals(Set.of("ProviderGroup", "DirectoryGroup"), restored.getGroups());
            assertEquals(Set.of(fixture.providerRole(), fixture.directoryRole()), restored.getCurrentRoleIds());
            Authorizer.checkAIFunctionAction(restored, "ai_complete", PrivilegeType.USAGE);

            fixture.memberOfGroups = Set.of();
            restored = fixture.restore(true);
            assertEquals(Set.of("ProviderGroup"), restored.getGroups());
            assertEquals(Set.of(fixture.providerRole()), restored.getCurrentRoleIds());

            fixture.memberOfGroups = Set.of("DirectoryGroup");
            execute(fixture.root, "REVOKE task_provider_role FROM EXTERNAL GROUP ProviderGroup");
            ConnectContext revoked = fixture.restore(true);
            assertEquals(Set.of("ProviderGroup", "DirectoryGroup"), revoked.getGroups());
            assertEquals(Set.of(fixture.directoryRole()), revoked.getCurrentRoleIds());
            assertThrows(AccessDeniedException.class,
                    () -> Authorizer.checkAIFunctionAction(revoked, "ai_complete", PrivilegeType.USAGE));
        }
    }

    @Test
    public void testSecurityIntegrationFallsBackToGlobalGroupProviders() throws Exception {
        try (LDAPTaskFixture fixture = new LDAPTaskFixture("group_provider")) {
            fixture.manager.replayAlterSecurityIntegration("task_external", Map.of(
                    SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_GROUP_PROVIDER, ""));
            ConnectContext restored = fixture.restore(true);
            assertEquals(Set.of("ProviderGroup"), restored.getGroups());
            assertEquals(Set.of(fixture.providerRole()), restored.getCurrentRoleIds());
            Authorizer.checkAIFunctionAction(restored, "ai_complete", PrivilegeType.USAGE);
        }
    }

    @Test
    public void testPermittedGroupsIgnoresCaseWithoutFilteringGroups() throws Exception {
        try (LDAPTaskFixture fixture = new LDAPTaskFixture("both")) {
            fixture.manager.replayAlterSecurityIntegration("task_external", Map.of(
                    SecurityIntegration.SECURITY_INTEGRATION_GROUP_ALLOWED_LOGIN, "directorygroup"));
            ConnectContext restored = fixture.restore(true);
            assertEquals(Set.of("ProviderGroup", "DirectoryGroup"), restored.getGroups());
            assertEquals(Set.of(fixture.providerRole(), fixture.directoryRole()), restored.getCurrentRoleIds());
            fixture.memberOfGroups = Set.of();
            assertThrows(SemanticException.class, () -> fixture.restore(true));
        }
    }

    @Test
    public void testMemberOfWithoutServiceAccountCannotRestoreTask() throws Exception {
        try (LDAPTaskFixture fixture = new LDAPTaskFixture("both")) {
            Config.authentication_ldap_simple_bind_root_pwd = "";
            assertThrows(SemanticException.class, () -> fixture.restore(false));
            assertThrows(SemanticException.class, () -> fixture.restore(true));
        }
    }

    @Test
    public void testMemberOfLookupFailureCannotRestoreTaskOrExposeDirectoryDetails() throws Exception {
        try (LDAPTaskFixture fixture = new LDAPTaskFixture("both")) {
            fixture.directoryFailure = new NamingException("private directory details");
            SemanticException error = assertThrows(SemanticException.class, () -> fixture.restore(true));
            assertFalse(error.getMessage().contains("private directory details"));
        }
    }

    @Test
    public void testMemberOfCannotChangeStoredTaskPrincipal() throws Exception {
        try (LDAPTaskFixture fixture = new LDAPTaskFixture("both")) {
            fixture.resolvedDN = "uid=another_user,dc=example";
            assertThrows(SemanticException.class, () -> fixture.restore(false));
            assertThrows(SemanticException.class, () -> fixture.restore(true));
        }
    }

    @Test
    public void testUnusableSecurityIntegrationProviderCannotRestoreTask() throws Exception {
        try (LDAPTaskFixture fixture = new LDAPTaskFixture("both")) {
            fixture.manager.replayAlterSecurityIntegration("task_external", Map.of(
                    SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_SERVER_PORT, "private-invalid-port"));
            SemanticException error = assertThrows(SemanticException.class, () -> fixture.restore(true));
            assertFalse(error.getMessage().contains("private-invalid-port"));
        }
    }

    @Test
    public void testNativePasswordDoesNotUseLDAPMemberOfPolicy() throws Exception {
        try (LDAPTaskFixture fixture = new LDAPTaskFixture("memberof")) {
            execute(fixture.root, "CREATE USER task_native_user IDENTIFIED BY ''");
            ((LDAPGroupProvider) fixture.manager.getGroupProvider("task_groups"))
                    .setUserToGroupCache(Map.of("task_native_user", Set.of("ProviderGroup")));
            ConnectContext restored = taskRun(replayTask("task_native_user", 1)).buildTaskRunConnectContext();
            assertEquals(Set.of("ProviderGroup"), restored.getGroups());
            assertTrue(restored.getCurrentRoleIds().contains(fixture.providerRole()));
            assertFalse(restored.getCurrentRoleIds().contains(fixture.directoryRole()));
        }
    }

    @Test
    public void testMemberOfAcceptsEquivalentStoredDN() throws Exception {
        try (LDAPTaskFixture fixture = new LDAPTaskFixture("memberof")) {
            fixture.resolvedDN = "UID=TASK_LDAP_USER, DC=example";
            assertEquals(Set.of("DirectoryGroup"), fixture.restore(true).getGroups());
        }
    }

    @Test
    public void testLegacyNativeLDAPKeepsLoginMemberOfExclusion() throws Exception {
        try (LDAPTaskFixture fixture = new LDAPTaskFixture("both")) {
            execute(fixture.root, "ALTER USER task_ldap_user IDENTIFIED WITH authentication_ldap_simple "
                    + "AS 'uid=task_ldap_user,dc=example'");
            ConnectContext restored = fixture.restore(false);
            assertEquals(Set.of("ProviderGroup"), restored.getGroups());
            assertTrue(restored.getCurrentRoleIds().contains(fixture.providerRole()));
            assertFalse(restored.getCurrentRoleIds().contains(fixture.directoryRole()));
        }
    }

    @Test
    public void testNativeLDAPTaskRejectsChangedExplicitPrincipal() throws Exception {
        try (LDAPTaskFixture fixture = new LDAPTaskFixture("group_provider")) {
            execute(fixture.root, "ALTER USER task_ldap_user IDENTIFIED WITH authentication_ldap_simple "
                    + "AS 'uid=reassigned_user,dc=example'");
            SemanticException error = assertThrows(SemanticException.class, () -> fixture.restore(false));
            assertFalse(error.getMessage().contains("uid="));
        }
    }

    @Test
    public void testNativeLDAPTaskRejectsSwitchToPasswordPrincipal() throws Exception {
        try (LDAPTaskFixture fixture = new LDAPTaskFixture("group_provider")) {
            execute(fixture.root, "ALTER USER task_ldap_user IDENTIFIED WITH mysql_native_password BY ''");
            SemanticException error = assertThrows(SemanticException.class, () -> fixture.restore(false));
            assertFalse(error.getMessage().contains("uid="));
        }
    }

    @Test
    public void testNativeLDAPTaskAcceptsEquivalentExplicitPrincipal() throws Exception {
        try (LDAPTaskFixture fixture = new LDAPTaskFixture("group_provider")) {
            execute(fixture.root, "ALTER USER task_ldap_user IDENTIFIED WITH authentication_ldap_simple "
                    + "AS 'UID=TASK_LDAP_USER, DC=example'");
            assertEquals(Set.of("ProviderGroup"), fixture.restore(false).getGroups());
        }
    }

    private static class LDAPTaskFixture implements AutoCloseable {
        private static final String USER = "task_ldap_user";
        private static final String DN = "uid=task_ldap_user,dc=example";
        private final AuthenticationMgr previousManager = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        private final AuthorizationMgr previousAuthorization = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        private final String[] previousProviders = Config.group_provider;
        private final String[] previousChain = Config.authentication_chain;
        private final String previousSource = Config.authentication_ldap_simple_group_source;
        private final String previousRootDN = Config.authentication_ldap_simple_bind_root_dn;
        private final String previousRootPassword = Config.authentication_ldap_simple_bind_root_pwd;
        private final String previousPattern = Config.authentication_ldap_simple_bind_dn_pattern;
        private final AuthenticationMgr manager = new AuthenticationMgr();
        private final AuthorizationMgr authorization = new AuthorizationMgr(new DefaultAuthorizationProvider());
        private final ConnectContext root;
        private Set<String> memberOfGroups = Set.of("DirectoryGroup");
        private String resolvedDN = DN;
        private NamingException directoryFailure;

        private LDAPTaskFixture(String source) throws Exception {
            UtFrameUtils.setUpForPersistTest();
            GlobalStateMgr.getCurrentState().setAuthenticationMgr(manager);
            GlobalStateMgr.getCurrentState().setAuthorizationMgr(authorization);
            Config.group_provider = new String[] {"task_groups"};
            Config.authentication_chain = new String[] {"task_external", "native"};
            Config.authentication_ldap_simple_group_source = source;
            Config.authentication_ldap_simple_bind_root_dn = "cn=service,dc=example";
            Config.authentication_ldap_simple_bind_root_pwd = "service-password";
            Config.authentication_ldap_simple_bind_dn_pattern = "";
            new MockUp<LDAPGroupProvider>() {
                @Mock
                public void init() {
                }
            };
            // Keep provider construction, group-source policy and memberOf extraction real.
            new MockUp<LDAPAuthProvider>() {
                @Mock
                protected LdapUserEntry findUserEntryByRoot(String user, String[] requestedAttributes)
                        throws NamingException {
                    if (directoryFailure != null) {
                        throw directoryFailure;
                    }
                    BasicAttributes attributes = new BasicAttributes(true);
                    BasicAttribute membership = new BasicAttribute("memberOf");
                    memberOfGroups.forEach(group -> membership.add("cn=" + group + ",dc=example"));
                    attributes.put(membership);
                    return new LdapUserEntry(resolvedDN, attributes);
                }
            };
            manager.replayCreateGroupProvider("task_groups", Map.of(
                    GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "ldap"));
            ((LDAPGroupProvider) manager.getGroupProvider("task_groups"))
                    .setUserToGroupCache(Map.of(DN, Set.of("ProviderGroup")));
            manager.replayCreateSecurityIntegration("task_external", Map.of(
                    SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY, "AUTHENTICATION_LDAP_SIMPLE",
                    SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_GROUP_PROVIDER, "task_groups",
                    SimpleLDAPSecurityIntegration.AUTHENTICATION_LDAP_SIMPLE_GROUP_SOURCE, source));
            root = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
            execute(root, "CREATE USER task_ldap_user IDENTIFIED WITH authentication_ldap_simple");
            execute(root, "CREATE ROLE task_provider_role, task_directory_role");
            execute(root, "GRANT USAGE ON AI FUNCTION ai_complete TO ROLE task_provider_role");
            execute(root, "GRANT task_provider_role TO EXTERNAL GROUP ProviderGroup");
            execute(root, "GRANT task_directory_role TO EXTERNAL GROUP DirectoryGroup");
        }

        private long providerRole() {
            return authorization.getRoleIdByNameAllowNull("task_provider_role");
        }

        private long directoryRole() {
            return authorization.getRoleIdByNameAllowNull("task_directory_role");
        }

        private ConnectContext restore(boolean external) {
            Task task = GsonUtils.GSON.fromJson("{\"name\":\"ldap_ai\",\"definition\":\"select ai_complete('p')\","
                    + "\"executionIdentity\":{\"version\":1,\"user\":\"" + USER + "\",\"host\":\"%\","
                    + "\"domain\":false,\"ephemeral\":" + external + ",\"securityIntegration\":\""
                    + (external ? "task_external" : "native") + "\",\"distinguishedName\":\"" + DN + "\"}}", Task.class);
            return taskRun(task).buildTaskRunConnectContext();
        }

        @Override
        public void close() {
            Config.group_provider = previousProviders;
            Config.authentication_chain = previousChain;
            Config.authentication_ldap_simple_group_source = previousSource;
            Config.authentication_ldap_simple_bind_root_dn = previousRootDN;
            Config.authentication_ldap_simple_bind_root_pwd = previousRootPassword;
            Config.authentication_ldap_simple_bind_dn_pattern = previousPattern;
            GlobalStateMgr.getCurrentState().setAuthenticationMgr(previousManager);
            GlobalStateMgr.getCurrentState().setAuthorizationMgr(previousAuthorization);
            UtFrameUtils.tearDownForPersisTest();
        }
    }

    @Test
    public void testLegacyNonAITaskRetainsRootMode() {
        Task task = new Task("legacy_non_ai");
        task.setDefinition("select 1");
        task.setCreateUser("legacy_user");
        assertEquals(UserIdentity.ROOT, taskRun(task).buildTaskRunConnectContext().getCurrentUserIdentity());
    }

    private static Task replayTask(String user, int version) {
        return GsonUtils.GSON.fromJson("{\"name\":\"replayed_ai\",\"definition\":\"select ai_complete('p')\","
                + "\"executionIdentity\":{\"version\":" + version + ",\"user\":\"" + user + "\","
                + "\"host\":\"%\",\"domain\":false,\"ephemeral\":false,"
                + "\"securityIntegration\":\"native\",\"distinguishedName\":\"" + user + "\"}}", Task.class);
    }

    private static Task replayExternalTask() {
        return GsonUtils.GSON.fromJson("{\"name\":\"external_ai\",\"definition\":\"select ai_complete('p')\","
                + "\"executionIdentity\":{\"version\":1,\"user\":\"root\",\"host\":\"%\","
                + "\"domain\":false,\"ephemeral\":true,\"securityIntegration\":\"task_external\","
                + "\"distinguishedName\":\"uid=external_root,dc=example\"}}", Task.class);
    }

    private static ConnectContext authenticateRoot() throws Exception {
        ConnectContext context = new ConnectContext();
        AuthenticationHandler.authenticate(context, "root", "127.0.0.1", new byte[0]);
        context.setThreadLocalInfo();
        return context;
    }

    private static TaskRun taskRun(Task task) {
        TaskRun run = TaskRunBuilder.newBuilder(task).build();
        run.initStatus(UUID.randomUUID().toString(), System.currentTimeMillis());
        return run;
    }

    private static void execute(ConnectContext context, String sql) throws Exception {
        try (var ignored = context.bindScope()) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, context), context);
        }
    }

    private static SubmitTaskStmt resolvedAISubmit(ConnectContext context) {
        SubmitTaskStmt statement = (SubmitTaskStmt) SqlParser.parseSingleStatement(
                "submit task identity_test as create table identity_output as select ai_complete('prompt') as answer",
                context.getSessionVariable().getSqlMode());
        SelectRelation select = (SelectRelation) statement.getCreateTableAsSelectStmt()
                .getQueryStatement().getQueryRelation();
        FunctionCallExpr call = (FunctionCallExpr) select.getSelectList().getItems().get(0).getExpr();
        Function function = new Function(new FunctionName("ai_complete"),
                new Type[] {VarcharType.VARCHAR}, VarcharType.VARCHAR, false);
        function.setBinaryType(TFunctionBinaryType.AI);
        call.setFn(function);
        select.setOutputExpr(List.of(call));
        return statement;
    }
}
