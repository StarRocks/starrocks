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

package com.starrocks.sql.plan;

import com.starrocks.authorization.AccessControlProvider;
import com.starrocks.authorization.AccessController;
import com.starrocks.authorization.NativeAccessController;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.ErrorReportException;
import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.planner.SlotId;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.AIProviderMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.AuthorizerStmtVisitor;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.common.AIModelConfigs;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExecPlanAIProviderPrivilegeTest extends PlanTestNoneDBBase {
    @Test
    public void testRecreatedProviderRequiresItsOwnGrantAtBinding() throws Exception {
        String name = "binding_recreated";
        String user = "binding_caller";
        AIProviderMgr manager = GlobalStateMgr.getCurrentState().getAIProviderMgr();
        Map<String, String> parameters = Map.of("endpoint", "https://example.test/v1/chat/completions",
                "model", "before-alter", "api_key", "test-key");
        manager.createProvider(name, AIProviderType.CHAT, parameters, "");
        executeAsRoot("CREATE USER '" + user + "' IDENTIFIED BY ''");
        try {
            executeAsRoot("GRANT USAGE ON AI FUNCTION ai_custom_query TO '" + user + "'");
            executeAsRoot("GRANT USAGE ON AI PROVIDER " + name + " TO '" + user + "'");
            ConnectContext caller = UtFrameUtils.initCtxForNewPrivilege(new UserIdentity(user, "%"));
            caller.setCurrentRoleIds(Set.of());
            QueryStatement statement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(
                    "SELECT ai_custom_query('" + name + "', 'prompt')", caller);
            FunctionCallExpr call = (FunctionCallExpr) ((SelectRelation) statement.getQueryRelation())
                    .getOutputExpression().get(0);
            Authorizer.check(statement, caller);
            ExecPlan captured = new ExecPlan(caller, List.of(), null, List.of(), false);
            AIModelConfigs.ModelConfig original = captured.bindAIModelConfigs(project(call)).get("provider:0");
            String originalId = manager.getProvider(name).getId();

            manager.alterProvider(name, Map.of("model", "after-alter"), false);
            Assertions.assertEquals(originalId, manager.getProvider(name).getId());
            ExecPlan afterAlter = new ExecPlan(caller, List.of(), null, List.of(), false);
            Assertions.assertEquals("after-alter", afterAlter.bindAIModelConfigs(project(call)).get("provider:0").model());

            // Simulate a metadata replacement between semantic authorization and physical binding.
            Authorizer.check(statement, caller);
            manager.dropProvider(name, false);
            manager.createProvider(name, AIProviderType.CHAT, parameters, "");
            Assertions.assertNotEquals(originalId, manager.getProvider(name).getId());
            ExecPlan rebuilt = new ExecPlan(caller, List.of(), null, List.of(), false);
            Assertions.assertThrows(ErrorReportException.class, () -> rebuilt.bindAIModelConfigs(project(call)));
            Assertions.assertSame(original, captured.bindAIModelConfigs(project(call)).get("provider:0"));
            Assertions.assertEquals("before-alter", original.model());

            executeAsRoot("GRANT USAGE ON AI PROVIDER " + name + " TO '" + user + "'");
            Assertions.assertEquals(Set.of("provider:0"), rebuilt.bindAIModelConfigs(project(call)).keySet());
            Assertions.assertNull(call.getAiModelConfigId());
        } finally {
            manager.dropProvider(name, true);
            executeAsRoot("DROP USER '" + user + "'");
            connectContext.setThreadLocalInfo();
        }
    }

    private static Map<SlotId, Expr> project(FunctionCallExpr call) {
        return new HashMap<>(Map.of(new SlotId(1), call));
    }

    private static void executeAsRoot(String sql) throws Exception {
        connectContext.setThreadLocalInfo();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, connectContext), connectContext);
    }

    @Test
    public void testRejectedProviderIsNotCapturedOrCached() throws Exception {
        QueryStatement statement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(
                "select ai_custom_query('private_provider', 'prompt')", connectContext);
        FunctionCallExpr call = (FunctionCallExpr) ((SelectRelation) statement.getQueryRelation())
                .getOutputExpression().get(0);
        Map<SlotId, Expr> project = new HashMap<>(Map.of(new SlotId(1), call));
        AIProvider provider = new AIProvider("private-id", "private_provider", AIProviderType.CHAT,
                Map.of("endpoint", "https://example.test/v1/chat/completions", "model", "test-model",
                        "api_key", "private-test-key"), "");
        AIProviderMgr manager = Mockito.mock(AIProviderMgr.class);
        Mockito.when(manager.getProvider("private_provider")).thenReturn(provider);
        GlobalStateMgr originalState = GlobalStateMgr.getCurrentState();
        GlobalStateMgr state = Mockito.mock(GlobalStateMgr.class);
        AccessControlProvider access = new AccessControlProvider(new AuthorizerStmtVisitor(), new AccessController() { });
        Mockito.when(state.getAIProviderMgr()).thenReturn(manager);
        Mockito.when(state.getAuthorizationMgr()).thenReturn(originalState.getAuthorizationMgr());
        Mockito.when(state.getAuthorizer()).thenReturn(new Authorizer(access));
        ExecPlan plan = new ExecPlan(connectContext, List.of(), null, List.of(), false);
        try (MockedStatic<GlobalStateMgr> mocked = Mockito.mockStatic(GlobalStateMgr.class, Mockito.CALLS_REAL_METHODS)) {
            mocked.when(GlobalStateMgr::getCurrentState).thenReturn(state);
            ErrorReportException denied = Assertions.assertThrows(ErrorReportException.class,
                    () -> plan.bindAIModelConfigs(project));
            Assertions.assertFalse(denied.getMessage().contains("private-test-key"));
            Assertions.assertSame(call, project.get(new SlotId(1)));
            Assertions.assertNull(call.getAiModelConfigId());

            access.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, new NativeAccessController());
            Map<String, AIModelConfigs.ModelConfig> recovered = plan.bindAIModelConfigs(project);
            Assertions.assertEquals(Set.of("provider:0"), recovered.keySet());
            Assertions.assertEquals("test-model", recovered.get("provider:0").model());
            Mockito.verify(manager, Mockito.times(2)).getProvider("private_provider");
        }
    }
}
