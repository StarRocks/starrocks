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

import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.planner.AIProjectNode;
import com.starrocks.planner.EmptySetNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.SlotId;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.planner.TupleId;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.AIProviderMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.thrift.TAIProjectNode;
import com.starrocks.thrift.TAIProviderProtocol;
import com.starrocks.thrift.TPlan;
import com.starrocks.type.JsonType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExecPlanAIProviderTest {
    private ConnectContext connectContext;
    private FunctionSet functions;
    private MockedStatic<Authorizer> authorizer;
    private static FunctionCallExpr chatTemplate;
    private static FunctionCallExpr embeddingTemplate;

    @BeforeEach
    public void setUp() {
        functions = new FunctionSet();
        functions.init();
        connectContext = Mockito.mock(ConnectContext.class);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(new SessionVariable());
        ConnectContext.set(connectContext);
        authorizer = Mockito.mockStatic(Authorizer.class);
        chatTemplate = template("ai_complete");
        embeddingTemplate = template("ai_embed");
    }

    @AfterEach
    public void tearDown() {
        authorizer.close();
        ConnectContext.remove();
    }

    private FunctionCallExpr template(String name) {
        Type[] types = {VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR, JsonType.JSON};
        Function function = functions.getFunction(new Function(new FunctionName(name), types, VarcharType.VARCHAR, false),
                Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNotNull(function);
        FunctionCallExpr call = new FunctionCallExpr(name, List.of(new StringLiteral("prompt"),
                new StringLiteral("provider"), new StringLiteral("model"),
                NullLiteral.create(JsonType.JSON)));
        call.setFn(function);
        call.setType(function.getReturnType());
        return call;
    }

    @Test
    public void testProviderIsValidatedForEachCapability() {
        AIProviderMgr manager = Mockito.mock(AIProviderMgr.class);
        Mockito.when(manager.getProvider("shared")).thenReturn(provider("shared", AIProviderType.CHAT, "chat-model"));
        ExecPlan plan = new ExecPlan(connectContext, List.of(), null, List.of(), false);
        try (MockedStatic<GlobalStateMgr> ignored = mockProviderManager(manager)) {
            Map<String, AIProvider> first = plan.bindAIProviders(project(call(chatTemplate, "shared")));
            Map<SlotId, Expr> incompatible = project(call(embeddingTemplate, "shared"));
            Expr original = incompatible.get(new SlotId(1));

            SemanticException error = Assertions.assertThrows(SemanticException.class,
                    () -> plan.bindAIProviders(incompatible));

            Assertions.assertEquals("AI provider 'shared' requires type EMBEDDING", error.getDetailMsg());
            Assertions.assertSame(original, incompatible.get(new SlotId(1)));
            Assertions.assertNull(((FunctionCallExpr) original).getAIProviderId());
            Assertions.assertNotSame(first.get("registry-id-shared"), plan.bindAIProviders(project(call(chatTemplate, "shared")))
                    .get("registry-id-shared"));
            Mockito.verify(manager, Mockito.times(3)).getProvider("shared");
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
                        () -> plan.bindAIProviders(project));
                Assertions.assertTrue(error.getDetailMsg().contains(message), error.getDetailMsg());
                Assertions.assertSame(original, project.get(new SlotId(1)));
                Assertions.assertNull(original.getAIProviderId());
            }

            Map<String, AIProvider> recovered = plan.bindAIProviders(project);
            Assertions.assertEquals(Set.of("registry-id-recoverable"), recovered.keySet());
            Assertions.assertEquals("recovered-model", recovered.get("registry-id-recoverable").getModel());
            Assertions.assertEquals("registry-id-recoverable", ((FunctionCallExpr) project.get(new SlotId(1))).getAIProviderId());
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
                Map<String, AIProvider> configs = plan.bindAIProviders(project);
                String id = "registry-id-" + names.get(i);
                Assertions.assertEquals(Set.of(id), configs.keySet());
                Assertions.assertEquals("model-" + i, configs.get(id).getModel());
                FunctionCallExpr bound = (FunctionCallExpr) project.get(new SlotId(1));
                Assertions.assertEquals(id, bound.getAIProviderId());
                Assertions.assertEquals(names.get(i), ((StringLiteral) bound.getChild(1)).getValue());
                Assertions.assertNull(original.getAIProviderId());
                Assertions.assertThrows(UnsupportedOperationException.class, configs::clear);
            }
            for (String name : names) {
                Mockito.verify(manager).getProvider(name);
            }
            Mockito.verifyNoMoreInteractions(manager);
        }
    }

    @Test
    public void testEachBindingCapturesIndependentProviderMetadata() {
        AIProviderMgr manager = Mockito.mock(AIProviderMgr.class);
        AIProvider metadata = provider("once", AIProviderType.CHAT, "original-model");
        Mockito.when(manager.getProvider("once")).thenReturn(metadata);
        ExecPlan plan = new ExecPlan(connectContext, List.of(), null, List.of(), false);
        try (MockedStatic<GlobalStateMgr> ignored = mockProviderManager(manager)) {
            Map<String, AIProvider> first = plan.bindAIProviders(project(call(chatTemplate, "once")));
            metadata.mergeParams(Map.of("model", "updated-model", "api_key", "updated-key"));
            Map<String, AIProvider> repeated = plan.bindAIProviders(project(call(chatTemplate, "once")));

            Assertions.assertNotSame(first.get("registry-id-shared"), repeated.get("registry-id-once"));
            Assertions.assertEquals("updated-model", repeated.get("registry-id-once").getModel());
            Assertions.assertEquals("updated-key", repeated.get("registry-id-once").getApiKey());
            Assertions.assertEquals("original-model", first.get("registry-id-once").getModel());
            Mockito.verify(manager, Mockito.times(2)).getProvider("once");

            Map<String, AIProvider> fresh = new ExecPlan(connectContext, List.of(), null, List.of(), false)
                    .bindAIProviders(project(call(chatTemplate, "once")));
            Assertions.assertEquals("updated-model", fresh.get("registry-id-once").getModel());
            Assertions.assertEquals("updated-key", fresh.get("registry-id-once").getApiKey());
            Mockito.verify(manager, Mockito.times(3)).getProvider("once");
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
            new ExecPlan(connectContext, List.of(), null, List.of(), false).bindAIProviders(project);
        }

        FunctionCallExpr bound = (FunctionCallExpr) project.get(new SlotId(1));
        Assertions.assertNotSame(original, bound);
        Assertions.assertNull(original.getAIProviderId());
        Assertions.assertEquals(originalSql, AstToSQLBuilder.toSQL(original));
        Assertions.assertEquals(originalSql, AstToSQLBuilder.toSQL(bound));
        Assertions.assertSame(original.getFn(), bound.getFn());
        Assertions.assertEquals(original.getType(), bound.getType());
        Assertions.assertEquals("registry-id-immutable", bound.getAIProviderId());
        Assertions.assertEquals("registry-id-immutable", ((FunctionCallExpr) bound.clone()).getAIProviderId());
        Assertions.assertTrue(Modifier.isFinal(FunctionCallExpr.class.getDeclaredField("aiProviderId").getModifiers()));
    }

    @Test
    public void testSameProviderIsDeduplicatedWithoutMergingCallArguments() {
        AIProviderMgr manager = Mockito.mock(AIProviderMgr.class);
        AIProvider metadata = provider("shared", AIProviderType.CHAT, "default-model");
        Mockito.when(manager.getProvider("shared")).thenReturn(metadata);
        FunctionCallExpr first = call(chatTemplate, "shared");
        FunctionCallExpr second = new FunctionCallExpr(chatTemplate.getFnRef(), List.of(new StringLiteral("prompt"),
                new StringLiteral("shared"), new StringLiteral("second-model"), chatTemplate.getChild(3).clone()));
        second.setFn(chatTemplate.getFn());
        second.setType(chatTemplate.getType());
        Map<SlotId, Expr> project = new HashMap<>(Map.of(new SlotId(1), first, new SlotId(2), second));
        try (MockedStatic<GlobalStateMgr> ignored = mockProviderManager(manager)) {
            ExecPlan plan = new ExecPlan(connectContext, List.of(), null, List.of(), false);
            Map<String, AIProvider> mapping = plan.bindAIProviders(project);
            Assertions.assertEquals(Set.of(metadata.getId()), mapping.keySet());
            Assertions.assertEquals("default-model", mapping.get(metadata.getId()).getModel());
            Assertions.assertEquals("explicit-model", ((StringLiteral) project.get(new SlotId(1)).getChild(2)).getValue());
            Assertions.assertEquals("second-model", ((StringLiteral) project.get(new SlotId(2)).getChild(2)).getValue());
            Assertions.assertEquals(metadata.getId(), ((FunctionCallExpr) project.get(new SlotId(1))).getAIProviderId());
            Assertions.assertEquals(metadata.getId(), ((FunctionCallExpr) project.get(new SlotId(2))).getAIProviderId());
            Assertions.assertNotSame(metadata, mapping.get(metadata.getId()));
        }
    }

    @Test
    public void testEachAIProjectSerializesItsProviderAndOwnsACopy() throws Exception {
        AIProviderMgr manager = Mockito.mock(AIProviderMgr.class);
        AIProvider metadata = provider("shared", AIProviderType.CHAT, "default-model");
        Mockito.when(manager.getProvider("shared")).thenReturn(metadata);
        try (MockedStatic<GlobalStateMgr> ignored = mockProviderManager(manager)) {
            ExecPlan plan = new ExecPlan(connectContext, List.of(), null, List.of(), false);
            Map<SlotId, Expr> innerCalls = project(call(chatTemplate, "shared"));
            Map<SlotId, Expr> outerCalls = project(call(chatTemplate, "shared"));
            TupleDescriptor tuple = new TupleDescriptor(new TupleId(0));
            Map<String, AIProvider> innerProviders = plan.bindAIProviders(innerCalls);
            AIProjectNode inner = new AIProjectNode(new PlanNodeId(1), tuple,
                    new EmptySetNode(new PlanNodeId(0), new ArrayList<>(List.of(tuple.getId()))),
                    innerCalls, Map.of(), innerProviders);
            AIProjectNode outer = new AIProjectNode(new PlanNodeId(2), tuple, inner,
                    outerCalls, Map.of(), plan.bindAIProviders(outerCalls));
            metadata.mergeParams(Map.of("model", "changed-metadata"));
            innerProviders.get(metadata.getId()).mergeParams(Map.of("model", "changed-caller-copy"));
            TPlan decoded = new TPlan();
            new TDeserializer(new TBinaryProtocol.Factory()).deserialize(decoded,
                    new TSerializer(new TBinaryProtocol.Factory()).serialize(outer.treeToThrift()));
            for (int index = 0; index < 2; index++) {
                TAIProjectNode project = decoded.getNodes().get(index).getAi_project_node();
                Assertions.assertEquals(Set.of(metadata.getId()), project.getAi_provider_mapping().keySet());
                Assertions.assertEquals("default-model",
                        project.getAi_provider_mapping().get(metadata.getId()).getParams().get("model"));
                Assertions.assertEquals(TAIProviderProtocol.OPENAI,
                        project.getAi_provider_mapping().get(metadata.getId()).getProtocol());
                Assertions.assertFalse(project.getAi_provider_mapping().get(metadata.getId())
                        .getParams().containsKey("protocol"));
                Assertions.assertEquals(metadata.getId(),
                        project.getSlot_map().get(1).getNodes().get(0).getAi_provider_id());
            }
        }
    }

    private static FunctionCallExpr call(FunctionCallExpr template, String name) {
        FunctionCallExpr call = new FunctionCallExpr(template.getFnRef(), List.of(new StringLiteral("prompt"),
                new StringLiteral(name), new StringLiteral("explicit-model"), template.getChild(3).clone()));
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

    private MockedStatic<GlobalStateMgr> mockProviderManager(AIProviderMgr manager) {
        GlobalStateMgr state = Mockito.mock(GlobalStateMgr.class);
        Mockito.when(state.getAIProviderMgr()).thenReturn(manager);
        Mockito.when(state.getBuiltinFunctions()).thenReturn(functions.getBuiltinFunctions());
        Mockito.when(state.getFunction(Mockito.any(Function.class), Mockito.any(Function.CompareMode.class)))
                .thenAnswer(call -> functions.getFunction(call.getArgument(0), call.getArgument(1)));
        Mockito.when(connectContext.getGlobalStateMgr()).thenReturn(state);
        MockedStatic<GlobalStateMgr> mocked = Mockito.mockStatic(GlobalStateMgr.class, Mockito.CALLS_REAL_METHODS);
        mocked.when(GlobalStateMgr::getCurrentState).thenReturn(state);
        return mocked;
    }
}
