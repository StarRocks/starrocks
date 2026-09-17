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

import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.planner.SlotId;
import com.starrocks.server.AIProviderMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.common.AIModelConfigs;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExecPlanAIProviderTest extends PlanTestBase {
    private static FunctionCallExpr chatTemplate;
    private static FunctionCallExpr embeddingTemplate;

    @BeforeAll
    public static void resolveFunctions() throws Exception {
        chatTemplate = analyzedCall("ai_custom_query");
        embeddingTemplate = analyzedCall("ai_custom_embedding");
    }

    @Test
    public void testCachedProviderIsRevalidatedForEachCapability() {
        AIProviderMgr manager = Mockito.mock(AIProviderMgr.class);
        Mockito.when(manager.getProvider("shared")).thenReturn(provider("shared", AIProviderType.CHAT, "chat-model"));
        ExecPlan plan = new ExecPlan(connectContext, List.of(), null, List.of(), false);
        try (MockedStatic<GlobalStateMgr> ignored = mockProviderManager(manager)) {
            Map<String, AIModelConfigs.ModelConfig> first = plan.bindAIModelConfigs(project(call(chatTemplate, "shared")));
            Map<SlotId, Expr> incompatible = project(call(embeddingTemplate, "shared"));
            Expr original = incompatible.get(new SlotId(1));

            SemanticException error = Assertions.assertThrows(SemanticException.class,
                    () -> plan.bindAIModelConfigs(incompatible));

            Assertions.assertEquals("AI provider 'shared' requires type EMBEDDING", error.getDetailMsg());
            Assertions.assertSame(original, incompatible.get(new SlotId(1)));
            Assertions.assertNull(((FunctionCallExpr) original).getAiModelConfigId());
            Assertions.assertSame(first.get("provider:0"), plan.bindAIModelConfigs(project(call(chatTemplate, "shared")))
                    .get("provider:0"));
            Mockito.verify(manager).getProvider("shared");
            Mockito.verifyNoMoreInteractions(manager);
        }
    }

    @Test
    public void testFailedBindingsAreNotCachedAndDoNotConsumeConfigurationIds() {
        AIProviderMgr manager = Mockito.mock(AIProviderMgr.class);
        AIProvider unsupported = provider("recoverable", AIProviderType.CHAT, "unsupported-model");
        unsupported.mergeParams(Map.of("protocol", "anthropic"));
        Mockito.when(manager.getProvider("recoverable")).thenReturn(null,
                provider("recoverable", AIProviderType.EMBEDDING, "embedding-model"), unsupported,
                provider("recoverable", AIProviderType.CHAT, "recovered-model"));
        ExecPlan plan = new ExecPlan(connectContext, List.of(), null, List.of(), false);
        FunctionCallExpr original = call(chatTemplate, "recoverable");
        Map<SlotId, Expr> project = project(original);
        try (MockedStatic<GlobalStateMgr> ignored = mockProviderManager(manager)) {
            for (String message : List.of("does not exist", "requires type CHAT", "not ANTHROPIC")) {
                SemanticException error = Assertions.assertThrows(SemanticException.class,
                        () -> plan.bindAIModelConfigs(project));
                Assertions.assertTrue(error.getDetailMsg().contains(message), error.getDetailMsg());
                Assertions.assertSame(original, project.get(new SlotId(1)));
                Assertions.assertNull(original.getAiModelConfigId());
            }

            Map<String, AIModelConfigs.ModelConfig> recovered = plan.bindAIModelConfigs(project);
            Assertions.assertEquals(Set.of("provider:0"), recovered.keySet());
            Assertions.assertEquals("recovered-model", recovered.get("provider:0").model());
            Assertions.assertEquals("provider:0", ((FunctionCallExpr) project.get(new SlotId(1))).getAiModelConfigId());
            Mockito.verify(manager, Mockito.times(4)).getProvider("recoverable");
            Mockito.verifyNoMoreInteractions(manager);
        }
    }

    @Test
    public void testExactProviderNamesReceiveDistinctOpaqueIdsAndOnlyUsedConfigurations() {
        List<String> names = List.of("named", " named", "named ", "named\nline", "provider:0");
        AIProviderMgr manager = Mockito.mock(AIProviderMgr.class);
        for (int i = 0; i < names.size(); i++) {
            Mockito.when(manager.getProvider(names.get(i)))
                    .thenReturn(provider(names.get(i), AIProviderType.CHAT, "model-" + i));
        }
        ExecPlan plan = new ExecPlan(connectContext, List.of(), null, List.of(), false);
        try (MockedStatic<GlobalStateMgr> ignored = mockProviderManager(manager)) {
            for (int i = 0; i < names.size(); i++) {
                FunctionCallExpr original = call(chatTemplate, names.get(i));
                Map<SlotId, Expr> project = project(original);
                Map<String, AIModelConfigs.ModelConfig> configs = plan.bindAIModelConfigs(project);
                String id = "provider:" + i;
                Assertions.assertEquals(Set.of(id), configs.keySet());
                Assertions.assertEquals("model-" + i, configs.get(id).model());
                FunctionCallExpr bound = (FunctionCallExpr) project.get(new SlotId(1));
                Assertions.assertEquals(id, bound.getAiModelConfigId());
                Assertions.assertEquals(names.get(i), ((StringLiteral) bound.getChild(0)).getValue());
                Assertions.assertNull(original.getAiModelConfigId());
                Assertions.assertThrows(UnsupportedOperationException.class, configs::clear);
            }
            for (String name : names) {
                Mockito.verify(manager).getProvider(name);
            }
            Mockito.verifyNoMoreInteractions(manager);
        }
    }

    @Test
    public void testProviderMetadataIsReadOncePerNamePerPlan() {
        AIProviderMgr manager = Mockito.mock(AIProviderMgr.class);
        AIProvider metadata = provider("once", AIProviderType.CHAT, "original-model");
        Mockito.when(manager.getProvider("once")).thenReturn(metadata);
        ExecPlan plan = new ExecPlan(connectContext, List.of(), null, List.of(), false);
        try (MockedStatic<GlobalStateMgr> ignored = mockProviderManager(manager)) {
            Map<String, AIModelConfigs.ModelConfig> first = plan.bindAIModelConfigs(project(call(chatTemplate, "once")));
            metadata.mergeParams(Map.of("model", "updated-model", "api_key", "updated-key"));
            Map<String, AIModelConfigs.ModelConfig> repeated = plan.bindAIModelConfigs(project(call(chatTemplate, "once")));

            Assertions.assertSame(first.get("provider:0"), repeated.get("provider:0"));
            Assertions.assertEquals("original-model", repeated.get("provider:0").model());
            Assertions.assertEquals("test-key", repeated.get("provider:0").apiKey());
            Mockito.verify(manager).getProvider("once");

            Map<String, AIModelConfigs.ModelConfig> fresh = new ExecPlan(connectContext, List.of(), null, List.of(), false)
                    .bindAIModelConfigs(project(call(chatTemplate, "once")));
            Assertions.assertEquals("updated-model", fresh.get("provider:0").model());
            Assertions.assertEquals("updated-key", fresh.get("provider:0").apiKey());
            Mockito.verify(manager, Mockito.times(2)).getProvider("once");
            Mockito.verifyNoMoreInteractions(manager);
        }
    }

    @Test
    public void testBindingCopiesExecutionExpressionAndConfigurationIdIsFinal() throws Exception {
        AIProviderMgr manager = Mockito.mock(AIProviderMgr.class);
        Mockito.when(manager.getProvider("immutable"))
                .thenReturn(provider("immutable", AIProviderType.CHAT, "immutable-model"));
        FunctionCallExpr original = call(chatTemplate, "immutable");
        String originalSql = AstToSQLBuilder.toSQL(original);
        Map<SlotId, Expr> project = project(original);
        try (MockedStatic<GlobalStateMgr> ignored = mockProviderManager(manager)) {
            new ExecPlan(connectContext, List.of(), null, List.of(), false).bindAIModelConfigs(project);
        }

        FunctionCallExpr bound = (FunctionCallExpr) project.get(new SlotId(1));
        Assertions.assertNotSame(original, bound);
        Assertions.assertNull(original.getAiModelConfigId());
        Assertions.assertEquals(originalSql, AstToSQLBuilder.toSQL(original));
        Assertions.assertEquals(originalSql, AstToSQLBuilder.toSQL(bound));
        Assertions.assertSame(original.getFn(), bound.getFn());
        Assertions.assertEquals(original.getType(), bound.getType());
        Assertions.assertEquals("provider:0", bound.getAiModelConfigId());
        Assertions.assertEquals("provider:0", ((FunctionCallExpr) bound.clone()).getAiModelConfigId());
        Assertions.assertTrue(Modifier.isFinal(FunctionCallExpr.class.getDeclaredField("aiModelConfigId").getModifiers()));
    }

    private static FunctionCallExpr analyzedCall(String function) throws Exception {
        QueryStatement statement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(
                "select " + function + "('template', 'prompt')", connectContext);
        return (FunctionCallExpr) ((SelectRelation) statement.getQueryRelation()).getOutputExpression().get(0);
    }

    private static FunctionCallExpr call(FunctionCallExpr template, String name) {
        FunctionCallExpr call = new FunctionCallExpr(template.getFnRef(), List.of(new StringLiteral(name),
                new StringLiteral("prompt")));
        call.setFn(template.getFn());
        call.setType(template.getType());
        return call;
    }

    private static Map<SlotId, Expr> project(FunctionCallExpr call) {
        return new HashMap<>(Map.of(new SlotId(1), call));
    }

    private static AIProvider provider(String name, AIProviderType type, String model) {
        return new AIProvider("registry-id-" + name, name, type,
                Map.of("endpoint", "https://example.test/v1/inference", "model", model,
                        "api_key", "test-key", "timeout_ms", "1200", "dimensions", "3"), "");
    }

    private static MockedStatic<GlobalStateMgr> mockProviderManager(AIProviderMgr manager) {
        GlobalStateMgr original = GlobalStateMgr.getCurrentState();
        GlobalStateMgr state = Mockito.mock(GlobalStateMgr.class);
        Mockito.when(state.getAIProviderMgr()).thenReturn(manager);
        Mockito.when(state.getAuthorizer()).thenReturn(original.getAuthorizer());
        Mockito.when(state.getAuthorizationMgr()).thenReturn(original.getAuthorizationMgr());
        MockedStatic<GlobalStateMgr> mocked = Mockito.mockStatic(GlobalStateMgr.class, Mockito.CALLS_REAL_METHODS);
        mocked.when(GlobalStateMgr::getCurrentState).thenReturn(state);
        return mocked;
    }
}
