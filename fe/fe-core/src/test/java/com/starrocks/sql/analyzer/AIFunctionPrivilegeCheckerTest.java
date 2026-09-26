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

package com.starrocks.sql.analyzer;

import com.starrocks.authentication.AuthenticationHandler;
import com.starrocks.authorization.AccessControlProvider;
import com.starrocks.authorization.AccessController;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.PrepareStmtContext;
import com.starrocks.qe.QueryDetail;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.server.AIProviderMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.PrepareStmtPlanner;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.ExecuteStmt;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestNoneDBBase;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class AIFunctionPrivilegeCheckerTest extends PlanTestNoneDBBase {
    private static final AtomicInteger NEXT_USER = new AtomicInteger();
    private static String previousEndpoint;
    private static String previousModel;
    private static String previousProvider;
    private ConnectContext caller;
    private String user;

    @BeforeAll
    public static void setUpAIObjects() throws Exception {
        previousEndpoint = Config.ai_default_chat_endpoint;
        previousModel = Config.ai_default_chat_model;
        previousProvider = Config.ai_default_chat_provider;
        Config.ai_default_chat_endpoint = "https://models.example.test/v1/chat/completions";
        Config.ai_default_chat_model = "rbac-model";
        Config.ai_default_chat_provider = "openai_compatible";
        starRocksAssert.withDatabase("ai_rbac").useDatabase("ai_rbac");
        starRocksAssert.withTable("CREATE TABLE prompts (id BIGINT, prompt VARCHAR(100)) "
                + "PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        starRocksAssert.withView("CREATE VIEW ai_none SECURITY NONE AS SELECT ai_complete(prompt) AS answer FROM prompts");
        starRocksAssert.withView("CREATE VIEW ai_invoker SECURITY INVOKER AS "
                + "SELECT ai_complete(prompt) AS answer FROM prompts");
        starRocksAssert.withView("CREATE VIEW ai_nested SECURITY NONE AS SELECT answer FROM ai_invoker");
        GlobalStateMgr.getCurrentState().getAIProviderMgr().createProvider("ai_rbac_provider", AIProviderType.CHAT,
                Map.of("endpoint", "https://models.example.test/v1/chat/completions", "model", "provider-model",
                        "api_key", "private-rbac-test-key"), "");
        starRocksAssert.withView("CREATE VIEW ai_provider_view SECURITY NONE AS "
                + "SELECT ai_custom_query('ai_rbac_provider', prompt) AS answer FROM prompts");
    }

    @AfterAll
    public static void restoreAIConfiguration() throws Exception {
        Config.ai_default_chat_endpoint = previousEndpoint;
        Config.ai_default_chat_model = previousModel;
        Config.ai_default_chat_provider = previousProvider;
        GlobalStateMgr.getCurrentState().getAIProviderMgr().dropProvider("ai_rbac_provider", true);
    }

    @BeforeEach
    public void createCaller() throws Exception {
        user = "ai_usage_" + NEXT_USER.incrementAndGet();
        executeAsRoot("CREATE USER '" + user + "' IDENTIFIED BY ''");
        caller = UtFrameUtils.initCtxForNewPrivilege(new UserIdentity(user, "%"));
        caller.setCurrentRoleIds(Set.of());
        caller.setDatabase("ai_rbac");
        executeAsRoot("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE ai_rbac.prompts TO '" + user + "'");
        executeAsRoot("GRANT SELECT ON ALL VIEWS IN DATABASE ai_rbac TO '" + user + "'");
        executeAsRoot("GRANT CREATE TABLE ON DATABASE ai_rbac TO '" + user + "'");
    }

    @AfterEach
    public void removeCaller() throws Exception {
        executeAsRoot("DROP USER '" + user + "'");
        connectContext.setThreadLocalInfo();
    }

    @Test
    public void testPublicRoleDoesNotGrantAIUsage() throws Exception {
        assertDenied("SELECT ai_complete('prompt')");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT answer FROM ai_none",
            "SELECT answer FROM ai_invoker",
            "SELECT answer FROM ai_nested",
            "WITH answers AS (SELECT ai_complete(prompt) AS answer FROM prompts) SELECT answer FROM answers",
            "SELECT 1 FROM (SELECT ai_complete(prompt) AS unused FROM prompts) answers",
            "INSERT INTO prompts SELECT id, ai_complete(prompt) FROM prompts",
            "INSERT OVERWRITE prompts SELECT id, ai_complete(prompt) FROM prompts",
            "UPDATE prompts SET prompt = ai_complete(prompt) WHERE id = 1",
            "DELETE FROM prompts WHERE prompt IN (SELECT ai_complete(prompt) FROM prompts)",
            "CREATE TABLE copied_prompts AS SELECT id, ai_complete(prompt) AS prompt FROM prompts"
    })
    public void testStatementAndViewWrappersCannotBypassCallerUsage(String sql) throws Exception {
        assertDenied(sql);
    }

    @Test
    public void testPrepareRequiresAIUsage() throws Exception {
        caller.getSessionVariable().setEnablePrepareStmt(true);
        assertDenied("PREPARE ai_denied FROM SELECT ai_complete('prompt')");
    }

    @Test
    public void testViewDefinitionsRequireCallerUsage() throws Exception {
        executeAsRoot("GRANT CREATE VIEW ON DATABASE ai_rbac TO '" + user + "'");
        executeAsRoot("GRANT ALTER ON VIEW ai_rbac.ai_none TO '" + user + "'");
        assertDenied("CREATE VIEW caller_ai AS SELECT ai_complete(prompt) AS answer FROM prompts");
        assertDenied("ALTER VIEW ai_none AS SELECT ai_complete(prompt) AS answer FROM prompts");
    }

    @Test
    public void testOrdinaryFunctionsRemainUnrestricted() throws Exception {
        authorize("SELECT upper(prompt) FROM prompts");
        authorize("SELECT ai_query(prompt, parse_json('{}')) FROM prompts");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testLegacyTaskRequiresProvenanceBeforeAIGrants(boolean grantUsage) throws Exception {
        caller.setQuerySource(QueryDetail.QuerySource.TASK);
        if (grantUsage) {
            executeAsRoot("GRANT USAGE ON AI FUNCTION ai_complete TO '" + user + "'");
        }
        StatementBase statement = analyze("SELECT answer FROM ai_nested");
        try (var ignored = caller.bindScope()) {
            SemanticException error = Assertions.assertThrows(SemanticException.class,
                    () -> Authorizer.check(statement, caller));
            Assertions.assertTrue(error.getMessage().contains("trusted task creator identity"), error.getMessage());
        }
    }

    @Test
    public void testTaskAuthorizationRejectsChangedAuthenticatedIdentity() throws Exception {
        executeAsRoot("GRANT USAGE ON AI FUNCTION ai_complete TO '" + user + "'");
        AuthenticationHandler.authenticate(caller, user, "127.0.0.1", new byte[0]);
        caller.setQuerySource(QueryDetail.QuerySource.TASK);
        caller.setDistinguishedName("changed_task_creator");
        StatementBase statement = analyze("SELECT ai_complete('prompt')");
        try (var ignored = caller.bindScope()) {
            SemanticException error = Assertions.assertThrows(SemanticException.class,
                    () -> Authorizer.check(statement, caller));
            Assertions.assertTrue(error.getMessage().contains("authenticated creator identity"), error.getMessage());
        }
    }

    @Test
    public void testLegacyTaskRetryRequiresProvenance() throws Exception {
        executeAsRoot("GRANT USAGE ON AI FUNCTION ai_complete TO '" + user + "'");
        caller.setQuerySource(QueryDetail.QuerySource.TASK);
        StatementBase statement = analyze("SELECT ai_complete(prompt) FROM prompts");
        try (var ignored = caller.bindScope()) {
            SemanticException error = Assertions.assertThrows(SemanticException.class,
                    () -> StatementPlanner.reAnalyzeStmt(statement, caller, new PlannerMetaLocker(caller, statement)));
            Assertions.assertTrue(error.getMessage().contains("trusted task creator identity"), error.getMessage());
        }
    }

    @Test
    public void testLegacyTaskOrdinaryFunctionsRemainUnrestricted() throws Exception {
        caller.setQuerySource(QueryDetail.QuerySource.TASK);
        authorize("SELECT upper(prompt) FROM prompts");
        authorize("SELECT ai_query(prompt, parse_json('{}')) FROM prompts");
    }

    @Test
    public void testNamedAndGlobalFunctionGrantsAreAlternatives() throws Exception {
        executeAsRoot("GRANT USAGE ON AI FUNCTION ai_complete TO '" + user + "'");
        authorize("SELECT ai_complete(prompt) FROM prompts");
        assertDenied("SELECT ai_sentiment(prompt) FROM prompts");

        executeAsRoot("GRANT USE AI FUNCTIONS ON SYSTEM TO '" + user + "'");
        executeAsRoot("REVOKE USAGE ON AI FUNCTION ai_complete FROM '" + user + "'");
        authorize("SELECT ai_complete(prompt), ai_sentiment(prompt) FROM prompts");
        executeAsRoot("REVOKE USE AI FUNCTIONS ON SYSTEM FROM '" + user + "'");
        assertDenied("SELECT ai_complete(prompt) FROM prompts");
    }

    @Test
    public void testFunctionAndProviderUsageAreBothRequired() throws Exception {
        String query = "SELECT ai_custom_query('ai_rbac_provider', prompt) FROM prompts";
        executeAsRoot("GRANT USAGE ON AI PROVIDER ai_rbac_provider TO '" + user + "'");
        assertDenied(query);
        executeAsRoot("GRANT USAGE ON AI FUNCTION ai_custom_query TO '" + user + "'");
        authorize(query);
        executeAsRoot("REVOKE USAGE ON AI PROVIDER ai_rbac_provider FROM '" + user + "'");
        assertDenied(query, "AI PROVIDER");
        assertDenied("SELECT answer FROM ai_provider_view", "AI PROVIDER");
        executeAsRoot("GRANT USE AI FUNCTIONS ON SYSTEM TO '" + user + "'");
        assertDenied(query, "AI PROVIDER");
    }

    @Test
    public void testReplayedTaskRechecksCurrentRolePrivileges() throws Exception {
        String role = user + "_task_role";
        List<String> objects = List.of("AI FUNCTION ai_custom_query", "AI PROVIDER ai_rbac_provider");
        executeAsRoot("CREATE ROLE " + role);
        try {
            for (String object : objects) {
                executeAsRoot("GRANT USAGE ON " + object + " TO ROLE " + role);
            }
            executeAsRoot("GRANT " + role + " TO '" + user + "'");
            AuthenticationHandler.authenticate(caller, user, "127.0.0.1", new byte[0]);
            SubmitTaskStmt submit = (SubmitTaskStmt) analyze("SUBMIT TASK AS CREATE TABLE task_output AS "
                    + "SELECT ai_custom_query('ai_rbac_provider', 'prompt') AS answer");
            Task task = GsonUtils.GSON.fromJson(
                    GsonUtils.GSON.toJson(TaskBuilder.buildTask(submit, caller)), Task.class);
            Long roleId = GlobalStateMgr.getCurrentState().getAuthorizationMgr().getRoleIdByNameAllowNull(role);

            caller = restoreTaskContext(task);
            Assertions.assertEquals(new UserIdentity(user, "%"), caller.getCurrentUserIdentity());
            Assertions.assertTrue(caller.getCurrentRoleIds().contains(roleId));
            Assertions.assertEquals(QueryDetail.QuerySource.TASK, caller.getQuerySource());
            Assertions.assertNotNull(caller.getAuthenticatedTaskIdentity());
            authorize(task.getDefinition());
            for (String object : objects) {
                executeAsRoot("REVOKE USAGE ON " + object + " FROM ROLE " + role);
                caller = restoreTaskContext(task);
                assertDenied(task.getDefinition(), object);
                executeAsRoot("GRANT USAGE ON " + object + " TO ROLE " + role);
                caller = restoreTaskContext(task);
                authorize(task.getDefinition());
            }
        } finally {
            executeAsRoot("DROP ROLE " + role);
        }
    }

    @Test
    public void testActiveRoleAndGroupGrantsUseCurrentMergedPrivileges() throws Exception {
        String role = user + "_role";
        String group = user + "_group";
        executeAsRoot("CREATE ROLE " + role);
        try {
            executeAsRoot("GRANT USAGE ON AI FUNCTION ai_complete TO ROLE " + role);
            executeAsRoot("GRANT " + role + " TO '" + user + "'");
            assertDenied("SELECT ai_complete('prompt')");
            Long roleId = GlobalStateMgr.getCurrentState().getAuthorizationMgr().getRoleIdByNameAllowNull(role);
            caller.setCurrentRoleIds(Set.of(roleId));
            authorize("SELECT ai_complete('prompt')");
            caller.setCurrentRoleIds(Set.of());
            assertDenied("SELECT ai_complete('prompt')");

            executeAsRoot("GRANT " + role + " TO EXTERNAL GROUP " + group);
            caller.setGroups(Set.of(group));
            authorize("SELECT ai_complete('prompt')");
            executeAsRoot("REVOKE USAGE ON AI FUNCTION ai_complete FROM ROLE " + role);
            assertDenied("SELECT ai_complete('prompt')");
        } finally {
            caller.setGroups(Set.of());
            caller.setCurrentRoleIds(Set.of());
            executeAsRoot("DROP ROLE " + role);
        }
    }

    @Test
    public void testPreparedAIIsFreshlyPlannedAndRechecksRevokedUsage() throws Exception {
        executeAsRoot("GRANT USAGE ON AI FUNCTION ai_complete TO '" + user + "'");
        PrepareStmt prepared = prepare("SELECT ai_complete(prompt) FROM prompts WHERE id = ?");
        PrepareStmtContext preparedContext = caller.getPreparedStmt(prepared.getName());
        try {
            ExecPlan first = executePrepared(prepared, 1);
            ExecPlan second = executePrepared(prepared, 2);
            Assertions.assertFalse(preparedContext.isCached());
            Assertions.assertNotSame(first.getPhysicalPlan(), second.getPhysicalPlan());
            executeAsRoot("REVOKE USAGE ON AI FUNCTION ai_complete FROM '" + user + "'");
            Assertions.assertThrows(ErrorReportException.class, () -> executePrepared(prepared, 3));
        } finally {
            caller.removePreparedStmt(prepared.getName());
        }
    }

    @Test
    public void testPreparedAIChecksCurrentActiveRole() throws Exception {
        String role = user + "_prepared_role";
        executeAsRoot("CREATE ROLE " + role);
        try {
            executeAsRoot("GRANT USAGE ON AI FUNCTION ai_complete TO ROLE " + role);
            executeAsRoot("GRANT " + role + " TO '" + user + "'");
            Long roleId = GlobalStateMgr.getCurrentState().getAuthorizationMgr().getRoleIdByNameAllowNull(role);
            caller.setCurrentRoleIds(Set.of(roleId));
            PrepareStmt prepared = prepare("SELECT ai_complete(prompt) FROM prompts WHERE id = ?");
            try {
                executePrepared(prepared, 1);
                caller.setCurrentRoleIds(Set.of());
                Assertions.assertThrows(ErrorReportException.class, () -> executePrepared(prepared, 2));
                caller.setCurrentRoleIds(Set.of(roleId));
                executePrepared(prepared, 3);
                executeAsRoot("REVOKE " + role + " FROM '" + user + "'");
                Assertions.assertThrows(ErrorReportException.class, () -> executePrepared(prepared, 4));
            } finally {
                caller.removePreparedStmt(prepared.getName());
            }
        } finally {
            caller.setCurrentRoleIds(Set.of());
            executeAsRoot("DROP ROLE " + role);
        }
    }

    @Test
    public void testPreparedProviderRecreationRequiresNewUUIDGrant() throws Exception {
        AIProviderMgr manager = GlobalStateMgr.getCurrentState().getAIProviderMgr();
        String provider = user + "_prepared_provider";
        Map<String, String> properties = Map.of("endpoint", "https://models.example.test/v1/chat/completions",
                "model", "original-model", "api_key", "private-rbac-test-key");
        String originalId = manager.createProvider(provider, AIProviderType.CHAT, properties, "");
        try {
            executeAsRoot("GRANT USAGE ON AI FUNCTION ai_custom_query TO '" + user + "'");
            executeAsRoot("GRANT USAGE ON AI PROVIDER " + provider + " TO '" + user + "'");
            PrepareStmt prepared = prepare("SELECT ai_custom_query('" + provider + "', prompt) FROM prompts WHERE id = ?");
            try {
                ExecPlan first = executePrepared(prepared, 1);
                Assertions.assertEquals(Set.of("original-model"), providerModels(first));
                manager.dropProvider(provider, false);
                String recreatedId = manager.createProvider(provider, AIProviderType.CHAT,
                        Map.of("endpoint", properties.get("endpoint"), "model", "recreated-model",
                                "api_key", properties.get("api_key")), "");
                Assertions.assertNotEquals(originalId, recreatedId);
                ErrorReportException denied = Assertions.assertThrows(ErrorReportException.class,
                        () -> executePrepared(prepared, 2));
                Assertions.assertTrue(denied.getMessage().contains("AI PROVIDER"), denied.getMessage());
                executeAsRoot("GRANT USAGE ON AI PROVIDER " + provider + " TO '" + user + "'");
                ExecPlan current = executePrepared(prepared, 3);
                Assertions.assertEquals(Set.of("recreated-model"), providerModels(current));
                Assertions.assertEquals(Set.of("original-model"), providerModels(first));
                Assertions.assertFalse(caller.getPreparedStmt(prepared.getName()).isCached());
            } finally {
                caller.removePreparedStmt(prepared.getName());
            }
        } finally {
            manager.dropProvider(provider, true);
        }
    }

    @Test
    public void testPreparedViewKeepsDefinitionButRechecksExecutedAIFamily() throws Exception {
        String view = user + "_prepared_view";
        executeAsRoot("CREATE VIEW " + view + " SECURITY NONE AS SELECT id, prompt AS answer FROM prompts");
        try {
            PrepareStmt prepared = prepare("SELECT answer FROM " + view + " WHERE id = ?");
            try {
                Assertions.assertTrue(executedAIFamilies(executePrepared(prepared, 1)).isEmpty());
                executeAsRoot("ALTER VIEW " + view + " AS SELECT id, ai_complete(prompt) AS answer FROM prompts");
                // Main retains the already-expanded view definition in a prepared statement. The old
                // ordinary query must not execute the new AI call, and a new query must authorize it.
                Assertions.assertTrue(executedAIFamilies(executePrepared(prepared, 2)).isEmpty());
                assertDenied("SELECT answer FROM " + view);
            } finally {
                caller.removePreparedStmt(prepared.getName());
            }

            executeAsRoot("GRANT USAGE ON AI FUNCTION ai_complete TO '" + user + "'");
            PrepareStmt aiPrepared = prepare("SELECT answer FROM " + view + " WHERE id = ?");
            try {
                Assertions.assertEquals(Set.of("ai_complete"), executedAIFamilies(executePrepared(aiPrepared, 3)));
                executeAsRoot("ALTER VIEW " + view + " AS SELECT id, ai_sentiment(prompt) AS answer FROM prompts");
                Assertions.assertEquals(Set.of("ai_complete"), executedAIFamilies(executePrepared(aiPrepared, 4)));
                assertDenied("SELECT answer FROM " + view);
                executeAsRoot("REVOKE USAGE ON AI FUNCTION ai_complete FROM '" + user + "'");
                Assertions.assertThrows(ErrorReportException.class, () -> executePrepared(aiPrepared, 5));
            } finally {
                caller.removePreparedStmt(aiPrepared.getName());
            }
        } finally {
            executeAsRoot("DROP VIEW " + view);
        }
    }

    @Test
    public void testRetryRechecksAIUsage() throws Exception {
        StatementBase statement = analyze("SELECT ai_complete(prompt) FROM prompts");
        try (var ignored = caller.bindScope()) {
            ErrorReportException error = Assertions.assertThrows(ErrorReportException.class,
                    () -> StatementPlanner.reAnalyzeStmt(statement, caller, new PlannerMetaLocker(caller, statement)));
            Assertions.assertTrue(error.getMessage().contains("AI FUNCTION"), error.getMessage());

            caller.setBypassAuthorizerCheck(true);
            Assertions.assertDoesNotThrow(() -> StatementPlanner.reAnalyzeStmt(
                    statement, caller, new PlannerMetaLocker(caller, statement)));
        }
    }

    @Test
    public void testCurrentControllerCannotFallBackToNative() throws Exception {
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser("SELECT ai_complete('prompt')", connectContext);
        AccessControlProvider provider = Authorizer.getInstance();
        AccessController original = provider.catalogToAccessControl.put(
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, new AccessController() { });
        try {
            Assertions.assertThrows(ErrorReportException.class, () -> Authorizer.check(statement, connectContext));
        } finally {
            provider.catalogToAccessControl.put(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, original);
        }
    }

    private void assertDenied(String sql) throws Exception {
        assertDenied(sql, "AI FUNCTION");
    }

    private ExecPlan executePrepared(PrepareStmt prepared, long id) {
        try (var ignored = caller.bindScope()) {
            caller.setQueryId(UUIDUtil.genUUID());
            caller.setExecutionId(UUIDUtil.toTUniqueId(caller.getQueryId()));
            ExecuteStmt execute = new ExecuteStmt(prepared.getName(), List.of(new IntLiteral(id, IntegerType.BIGINT)));
            Analyzer.analyze(execute, caller);
            return PrepareStmtPlanner.plan(execute, prepared.assignValues(execute.getParamsExpr()), caller);
        }
    }

    private PrepareStmt prepare(String query) throws Exception {
        caller.getSessionVariable().setEnablePrepareStmt(true);
        PrepareStmt prepared = (PrepareStmt) analyze("PREPARE ai_revoke FROM " + query);
        try (var ignored = caller.bindScope()) {
            Authorizer.check(prepared, caller);
        }
        caller.putPreparedStmt(prepared.getName(), new PrepareStmtContext(prepared, caller, null));
        return prepared;
    }

    private static Set<String> providerModels(ExecPlan plan) {
        return plan.getFragments().stream()
                .flatMap(fragment -> fragment.getPlanRoot().treeToThrift().getNodes().stream())
                .filter(node -> node.getNode_type() == TPlanNodeType.AI_PROJECT_NODE)
                .flatMap(node -> node.getAi_project_node().getAi_model_configs().values().stream())
                .map(config -> config.getChat().getModel())
                .collect(Collectors.toSet());
    }

    private static Set<String> executedAIFamilies(ExecPlan plan) {
        return plan.getFragments().stream()
                .flatMap(fragment -> fragment.getPlanRoot().treeToThrift().getNodes().stream())
                .filter(node -> node.getNode_type() == TPlanNodeType.AI_PROJECT_NODE)
                .flatMap(node -> node.getAi_project_node().getSlot_map().values().stream())
                .flatMap(expression -> expression.getNodes().stream())
                .filter(node -> node.isSetFn() && node.getFn().getBinary_type() == TFunctionBinaryType.AI)
                .map(node -> node.getFn().getName().getFunction_name())
                .collect(Collectors.toSet());
    }

    private void assertDenied(String sql, String objectType) throws Exception {
        StatementBase statement = analyze(sql);
        try (var ignored = caller.bindScope()) {
            ErrorReportException error = Assertions.assertThrows(ErrorReportException.class,
                    () -> Authorizer.check(statement, caller));
            Assertions.assertTrue(error.getMessage().contains(objectType), error.getMessage());
            Assertions.assertFalse(error.getMessage().contains("private-rbac-test-key"));
        }
    }

    private void authorize(String sql) throws Exception {
        try (var ignored = caller.bindScope()) {
            Authorizer.check(analyze(sql), caller);
        }
    }

    private StatementBase analyze(String sql) throws Exception {
        try (var ignored = caller.bindScope()) {
            return UtFrameUtils.parseStmtWithNewParser(sql, caller);
        }
    }

    private static void executeAsRoot(String sql) throws Exception {
        try (var ignored = connectContext.bindScope()) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, connectContext), connectContext);
        }
    }

    private static ConnectContext restoreTaskContext(Task task) {
        TaskRun run = TaskRunBuilder.newBuilder(task).build();
        run.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        return run.buildTaskRunConnectContext();
    }
}
