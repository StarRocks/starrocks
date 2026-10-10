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
import com.starrocks.authentication.LDAPGroupProvider;
import com.starrocks.authentication.SecurityIntegration;
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
