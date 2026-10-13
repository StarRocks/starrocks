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

import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ExecuteAsExecutor;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.PlanTestNoneDBBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class AIFunctionAuthorizationTest extends PlanTestNoneDBBase {
    private static final AtomicInteger NEXT_USER = new AtomicInteger();
    private static String[] previousAIConfig;
    private ConnectContext caller;
    private String user;

    @BeforeAll
    public static void setUpAIObjects() throws Exception {
        previousAIConfig = new String[] {Config.ai_default_chat_endpoint, Config.ai_default_chat_model,
                Config.ai_default_chat_provider, Config.ai_default_embedding_endpoint,
                Config.ai_default_embedding_model, Config.ai_default_embedding_provider};
        Config.ai_default_chat_endpoint = "https://models.example.test/v1/chat/completions";
        Config.ai_default_chat_model = "authorization-model";
        Config.ai_default_chat_provider = "openai_compatible";
        Config.ai_default_embedding_endpoint = "https://models.example.test/v1/embeddings";
        Config.ai_default_embedding_model = "authorization-embedding-model";
        Config.ai_default_embedding_provider = "openai_compatible";
        starRocksAssert.withDatabase("ai_authorization").useDatabase("ai_authorization");
        GlobalStateMgr.getCurrentState().getAIProviderMgr().createProvider("ai_authorization_provider", AIProviderType.CHAT,
                Map.of("endpoint", Config.ai_default_chat_endpoint, "model", "provider-model",
                        "api_key", "private-authorization-test-key"), "");
    }

    @AfterAll
    public static void restoreAIConfiguration() throws Exception {
        Config.ai_default_chat_endpoint = previousAIConfig[0];
        Config.ai_default_chat_model = previousAIConfig[1];
        Config.ai_default_chat_provider = previousAIConfig[2];
        Config.ai_default_embedding_endpoint = previousAIConfig[3];
        Config.ai_default_embedding_model = previousAIConfig[4];
        Config.ai_default_embedding_provider = previousAIConfig[5];
        GlobalStateMgr.getCurrentState().getAIProviderMgr().dropProvider("ai_authorization_provider", true);
    }

    @BeforeEach
    public void createCaller() throws Exception {
        user = "ai_analyzer_usage_" + NEXT_USER.incrementAndGet();
        executeAsRoot("CREATE USER '" + user + "' IDENTIFIED BY ''");
        caller = UtFrameUtils.initCtxForNewPrivilege(new UserIdentity(user, "%"));
        caller.setCurrentRoleIds(Set.of());
        caller.setDatabase("ai_authorization");
        executeAsRoot("GRANT CREATE TABLE ON DATABASE ai_authorization TO '" + user + "'");
    }

    @AfterEach
    public void removeCaller() throws Exception {
        executeAsRoot("DROP USER '" + user + "'");
        connectContext.setThreadLocalInfo();
    }

    @Test
    public void testAnalyzerRejectsMissingUsageBeforeAuthorizer() {
        assertAnalysisDenied("SELECT ai_complete('prompt')", caller, "AI FUNCTION");
    }

    @ParameterizedTest
    @CsvSource(value = {
            "ai_complete | ai_complete('prompt')",
            "ai_complete | ai_complete('model', 'prompt', map{'temperature': 0.5})",
            "ai_embed | ai_embed('prompt')",
            "ai_sentiment | ai_sentiment('prompt')",
            "ai_classify | ai_classify('prompt', ['label'])",
            "ai_extract | ai_extract('prompt', ['field'])",
            "ai_fix_grammar | ai_fix_grammar('prompt')",
            "ai_redact | ai_redact('prompt', ['field'])",
            "ai_translate | ai_translate('prompt', 'en', 'zh')",
            "ai_similarity | ai_similarity('prompt', 'other')",
            "ai_summarize | ai_summarize('prompt')",
            "ai_filter | ai_filter('prompt', 'condition')"
    }, delimiter = '|')
    public void testFamilyUsageIsRecheckedForNewStatements(String family, String expression) throws Exception {
        String sql = "SELECT " + expression;
        assertAnalysisDenied(sql, caller, "AI FUNCTION");
        executeAsRoot("GRANT USAGE ON AI FUNCTION " + family + " TO '" + user + "'");
        QueryStatement statement = (QueryStatement) analyze(sql, caller);
        FunctionCallExpr call = (FunctionCallExpr) ((SelectRelation) statement.getQueryRelation())
                .getOutputExpression().get(0);
        Assertions.assertTrue(call.getFn().isAi());
        Assertions.assertEquals(family, call.getFn().functionName());
        executeAsRoot("REVOKE USAGE ON AI FUNCTION " + family + " FROM '" + user + "'");
        assertAnalysisDenied(sql, caller, "AI FUNCTION");
    }

    @Test
    public void testProviderAndFunctionRequireSeparateUsage() throws Exception {
        String sql = "SELECT ai_custom_query('ai_authorization_provider', 'prompt')";
        executeAsRoot("GRANT USAGE ON AI PROVIDER ai_authorization_provider TO '" + user + "'");
        assertAnalysisDenied(sql, caller, "AI FUNCTION");
        executeAsRoot("GRANT USAGE ON AI FUNCTION ai_custom_query TO '" + user + "'");
        analyze(sql, caller);
        executeAsRoot("REVOKE USAGE ON AI PROVIDER ai_authorization_provider FROM '" + user + "'");
        assertAnalysisDenied(sql, caller, "AI PROVIDER");
        executeAsRoot("GRANT USAGE ON AI PROVIDER ai_authorization_provider TO '" + user + "'");
        analyze(sql, caller);
        executeAsRoot("REVOKE USAGE ON AI FUNCTION ai_custom_query FROM '" + user + "'");
        assertAnalysisDenied(sql, caller, "AI FUNCTION");
    }

    @Test
    public void testAnalyzerUsesExplicitContextInsteadOfThreadLocal() {
        String sql = "SELECT ai_complete('prompt')";
        try (var ignored = connectContext.bindScope()) {
            assertAnalysisDenied(sql, caller, "AI FUNCTION");
        }
        try (var ignored = caller.bindScope()) {
            Assertions.assertDoesNotThrow(() -> analyze(sql, connectContext));
        }
    }

    @Test
    public void testExecuteAsUsesTargetGrants() throws Exception {
        executeAsRoot("GRANT USAGE ON AI FUNCTION ai_complete TO '" + user + "'");
        ConnectContext session = UtFrameUtils.createDefaultCtx();
        try (var ignored = session.bindScope()) {
            ExecuteAsStmt executeAs = (ExecuteAsStmt) analyze("EXECUTE AS '" + user + "' WITH NO REVERT", session);
            Authorizer.check(executeAs, session);
            ExecuteAsExecutor.execute(executeAs, session);
            Assertions.assertEquals(caller.getCurrentUserIdentity(), session.getCurrentUserIdentity());
            analyze("SELECT ai_complete('prompt')", session);
            executeAsRoot("REVOKE USAGE ON AI FUNCTION ai_complete FROM '" + user + "'");
            assertAnalysisDenied("SELECT ai_complete('prompt')", session, "AI FUNCTION");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTaskUsesExistingAuthorizationContextWithoutAuthenticationProvenance(boolean useCreator)
            throws Exception {
        boolean previousUseCreator = Config.mv_use_creator_based_authorization;
        Config.mv_use_creator_based_authorization = useCreator;
        try (var ignored = caller.bindScope()) {
            executeAsRoot("GRANT USAGE ON AI FUNCTION ai_complete TO '" + user + "'");
            SubmitTaskStmt submit = (SubmitTaskStmt) analyze("SUBMIT TASK ai_authorization_task AS "
                    + "CREATE TABLE task_answer AS SELECT ai_complete('prompt') AS answer", caller);
            // A normal native-user context is sufficient; no authentication or AI identity envelope is installed.
            Task task = TaskBuilder.buildTask(submit, caller);
            TaskRun run = TaskRunBuilder.newBuilder(task).build();
            run.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
            ConnectContext taskContext = run.buildTaskRunConnectContext();
            Assertions.assertEquals(useCreator ? caller.getCurrentUserIdentity() : UserIdentity.ROOT,
                    taskContext.getCurrentUserIdentity());
            Authorizer.check(analyze(task.getDefinition(), taskContext), taskContext);
            executeAsRoot("REVOKE USAGE ON AI FUNCTION ai_complete FROM '" + user + "'");
            if (useCreator) {
                assertAnalysisDenied(task.getDefinition(), taskContext, "AI FUNCTION");
            } else {
                Authorizer.check(analyze(task.getDefinition(), taskContext), taskContext);
            }
        } finally {
            Config.mv_use_creator_based_authorization = previousUseCreator;
        }
    }

    @Test
    public void testLegacyAIQueryRemainsAnOrdinaryBuiltin() {
        StatementBase statement = analyze("SELECT ai_query('prompt', parse_json('{}'))", caller);
        Assertions.assertDoesNotThrow(() -> Authorizer.check(statement, caller));
        FunctionCallExpr call = (FunctionCallExpr) ((SelectRelation) ((QueryStatement) statement).getQueryRelation())
                .getOutputExpression().get(0);
        Assertions.assertFalse(call.getFn().isAi());
    }

    @Test
    public void testLegacyAIQueryIsNotAGrantableAIFamily() {
        Assertions.assertNull(GlobalStateMgr.getCurrentState().getGrantableAIFunctionFamily("ai_query"));
        Assertions.assertThrows(SemanticException.class,
                () -> analyze("GRANT USAGE ON AI FUNCTION ai_query TO '" + user + "'", connectContext));
    }

    private static void assertAnalysisDenied(String sql, ConnectContext context, String objectType) {
        ErrorReportException error = Assertions.assertThrows(ErrorReportException.class, () -> analyze(sql, context));
        Assertions.assertTrue(error.getMessage().contains(objectType), error.getMessage());
        Assertions.assertTrue(error.getMessage().contains("USAGE"), error.getMessage());
        Assertions.assertFalse(error.getMessage().contains("private-authorization-test-key"));
    }

    private static StatementBase analyze(String sql, ConnectContext context) {
        StatementBase statement = SqlParser.parse(sql, context.getSessionVariable()).get(0);
        Analyzer.analyze(statement, context);
        return statement;
    }

    private static void executeAsRoot(String sql) throws Exception {
        try (var ignored = connectContext.bindScope()) {
            DDLStmtExecutor.execute(analyze(sql, connectContext), connectContext);
        }
    }
}
