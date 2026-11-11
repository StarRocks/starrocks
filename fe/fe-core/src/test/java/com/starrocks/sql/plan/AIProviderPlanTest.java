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
import com.starrocks.planner.AIProjectNode;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.AIProviderMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.OptimisticVersion;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.thrift.TAIProjectNode;
import com.starrocks.thrift.TAIProvider;
import com.starrocks.thrift.TAIProviderProtocol;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.utframe.AIProviderTestUtil;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class AIProviderPlanTest extends PlanTestBase {
    private static String oldChatEndpoint;
    private static String oldChatModel;
    private static String oldChatProvider;

    @BeforeAll
    public static void createProviders() throws Exception {
        oldChatEndpoint = AIProviderTestUtil.chatProperty("endpoint");
        oldChatModel = AIProviderTestUtil.chatProperty("model");
        oldChatProvider = AIProviderTestUtil.chatProperty("protocol");
        AIProviderTestUtil.setChatProperty("endpoint", "https://models.example.test/v1/chat/completions");
        AIProviderTestUtil.setChatProperty("model", "system-chat-model");
        AIProviderTestUtil.setChatProperty("protocol", "openai");
        manager().createProvider("text_plan_chat", AIProviderType.CHAT, properties("original-model"), "");
        manager().createProvider("text_plan_embedding", AIProviderType.EMBEDDING,
                Map.of("endpoint", "https://models.example.test/v1/embeddings", "model", "embedding-model",
                        "api_key", "test-embedding-key", "timeout_ms", "1500", "dimensions", "6"), "");
    }

    @AfterAll
    public static void restoreChatConfiguration() {
        AIProviderTestUtil.setChatProperty("endpoint", oldChatEndpoint);
        AIProviderTestUtil.setChatProperty("model", oldChatModel);
        AIProviderTestUtil.setChatProperty("protocol", oldChatProvider);
    }

    @BeforeEach
    public void resetDefaultProvider() {
        AIProviderTestUtil.setChatProperty("endpoint", "https://models.example.test/v1/chat/completions");
        AIProviderTestUtil.setChatProperty("model", "default-chat-model");
        AIProviderTestUtil.setChatProperty("protocol", "openai");
    }

    @AfterEach
    public void clearEmbeddingConfig() {
        AIProviderTestUtil.setEmbeddingProperty("endpoint", "");
        AIProviderTestUtil.setEmbeddingProperty("model", "");
        AIProviderTestUtil.setEmbeddingProperty("protocol", "");
    }

    @Test
    public void testDirectPhysicalPlanCapturesCurrentProviderWithoutBindings() throws Exception {
        String name = "direct_physical_chat";
        manager().createProvider(name, AIProviderType.CHAT, properties("original-model"), "");
        try {
            ExecPlan original = getExecPlan(
                    "select ai_complete(provider => '" + name + "', model => 'explicit-model', prompt => "
                            + "ai_complete(provider => '" + name + "', model => 'explicit-model', prompt => k1)) from t7");
            manager().alterProvider(name, properties("current-model"), false);
            ExecPlan rebuilt = PlanFragmentBuilder.createPhysicalPlan(original.getPhysicalPlan(), connectContext,
                    original.getOutputColumns(), original.getColumnRefFactory(), original.getColNames(),
                    TResultSinkType.MYSQL_PROTOCAL, true, false);
            assertSnapshot(original, properties("original-model"));
            assertSnapshot(rebuilt, properties("current-model"));
        } finally {
            manager().dropProvider(name, true);
        }
    }

    @Test
    public void testEachBindingRefreshesProvider() throws Exception {
        String name = "direct_exec_plan_chat";
        manager().createProvider(name, AIProviderType.CHAT, properties("original-model"), "");
        try {
            ExecPlan template = getExecPlan("select ai_complete(provider => '" + name + "', model => "
                    + "'explicit-model', prompt => k1) from t7");
            AIProjectNode node = (AIProjectNode) template.getFragments().stream()
                    .flatMap(fragment -> fragment.collectNodes().stream())
                    .filter(AIProjectNode.class::isInstance).findFirst().orElseThrow();
            ExecPlan plan = new ExecPlan(connectContext, List.of(), null, List.of(), false);
            Map<String, AIProvider> first = plan.bindAIProviders(new HashMap<>(node.getSlotMap()));
            manager().alterProvider(name, properties("current-model"), false);
            Map<String, AIProvider> repeated = plan.bindAIProviders(new HashMap<>(node.getSlotMap()));
            Map<String, AIProvider> fresh =
                    new ExecPlan(connectContext, List.of(), null, List.of(), false)
                    .bindAIProviders(new HashMap<>(node.getSlotMap()));
            Assertions.assertNotSame(first.values().iterator().next(), repeated.values().iterator().next());
            Assertions.assertEquals("current-model", repeated.values().iterator().next().getModel());
            Assertions.assertEquals("current-model", fresh.values().iterator().next().getModel());
        } finally {
            manager().dropProvider(name, true);
        }
    }

    @Test
    public void testEmbeddingWireRouteAndOptions() throws Exception {
        AIProviderTestUtil.setEmbeddingProperty("endpoint", "https://models.example.test/v1/embeddings");
        AIProviderTestUtil.setEmbeddingProperty("model", "embedding-model");
        AIProviderTestUtil.setEmbeddingProperty("protocol", "openai");
        TAIProjectNode node = aiNodes(getExecPlan("select ai_embed(input => k1, options => "
                + "json_object('dimensions', 3)) from t7")).get(0);
        Assertions.assertEquals(1, node.getAi_provider_mapping().size());
        TAIProvider config = node.getAi_provider_mapping().values().iterator().next();
        Assertions.assertFalse(config.getType() == com.starrocks.thrift.TAIProviderType.CHAT);
        Assertions.assertEquals("embedding-model", config.getParams().get("model"));
        Assertions.assertFalse(config.getParams().containsKey("api_key"));
        Assertions.assertFalse(config.getParams().containsKey("timeout_ms"));
        Assertions.assertFalse(config.getParams().containsKey("dimensions"));
        Assertions.assertEquals(4, node.getSlot_map().values().iterator().next().getNodes().get(0).getNum_children());
    }

    @Test
    public void testExplicitProviderPlanUsesProviderConfiguration() throws Exception {
        ExecPlan plan = getExecPlan("select ai_complete(prompt => k1, provider => concat('text_plan_', 'chat'), "
                + "model => 'explicit-model') from t7");
        TAIProjectNode chat = aiNodes(plan).get(0);
        String configId = manager().getProvider("text_plan_chat").getId();
        Assertions.assertEquals(Set.of(configId), chat.getAi_provider_mapping().keySet());
        TAIProvider configuration = chat.getAi_provider_mapping().get(configId);
        Assertions.assertEquals("test-key-original-model", configuration.getParams().get("api_key"));
        Assertions.assertEquals("1200", configuration.getParams().get("timeout_ms"));
        Assertions.assertEquals("text_plan_chat",
                chat.getSlot_map().values().iterator().next().getNodes().get(2).getString_literal().getValue());
        ExecPlan embedding = getExecPlan(
                "select ai_embed(provider => 'text_plan_embedding', model => 'explicit-model', input => k1, "
                        + "options => json_object('dimensions', 3)) from t7");
        TAIProvider embeddingConfig = aiNodes(embedding).get(0).getAi_provider_mapping()
                .values().iterator().next();
        Assertions.assertTrue(embeddingConfig.getType() == com.starrocks.thrift.TAIProviderType.EMBEDDING);
        Assertions.assertFalse(embeddingConfig.getType() == com.starrocks.thrift.TAIProviderType.CHAT);
        Assertions.assertEquals("test-embedding-key", embeddingConfig.getParams().get("api_key"));
        Assertions.assertEquals("1500", embeddingConfig.getParams().get("timeout_ms"));
        Assertions.assertEquals("6", embeddingConfig.getParams().get("dimensions"));
        Assertions.assertThrows(Exception.class, () -> getExecPlan("select ai_embed(provider => 'text_plan_chat', "
                + "model => 'explicit-model', input => 'p')"));
        Assertions.assertThrows(Exception.class, () -> getExecPlan("select ai_complete(provider => "
                + "'text_plan_embedding', model => 'explicit-model', prompt => 'p')"));
        Assertions.assertThrows(Exception.class, () -> getExecPlan("select ai_complete(provider => "
                + "'missing_provider', model => 'explicit-model', prompt => 'p')"));
    }

    @Test
    public void testSqlCreatedProviderBindsItsExecutionConfiguration() throws Exception {
        String name = "ddl_plan_chat";
        String sql = "CREATE AI PROVIDER " + name + " TYPE chat PROPERTIES ("
                + "'endpoint' = 'https://models.example.test/v1/inference', "
                + "'model' = 'ddl-model', 'api_key' = 'test-ddl-key', 'timeout_ms' = '1200')";
        try {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, connectContext), connectContext);
            ExecPlan plan = getExecPlan("select ai_complete(provider => '" + name + "', model => 'explicit-model', "
                    + "prompt => k1) from t7");
            List<TAIProjectNode> nodes = aiNodes(plan);
            Assertions.assertEquals(1, nodes.size());
            String configId = manager().getProvider(name).getId();
            Assertions.assertEquals(Set.of(configId), nodes.get(0).getAi_provider_mapping().keySet());
            TAIProvider configuration = nodes.get(0).getAi_provider_mapping().get(configId);
            Assertions.assertTrue(configuration.getType() == com.starrocks.thrift.TAIProviderType.CHAT);
            Assertions.assertFalse(configuration.getType() == com.starrocks.thrift.TAIProviderType.EMBEDDING);
            Assertions.assertEquals("https://models.example.test/v1/inference", configuration.getParams().get("endpoint"));
            Assertions.assertEquals("ddl-model", configuration.getParams().get("model"));
            Assertions.assertEquals(TAIProviderProtocol.OPENAI, configuration.getProtocol());
            Assertions.assertEquals("test-ddl-key", configuration.getParams().get("api_key"));
            Assertions.assertEquals("1200", configuration.getParams().get("timeout_ms"));
        } finally {
            manager().dropProvider(name, true);
        }
    }

    @Test
    public void testPlanSnapshotSurvivesAlterAndRecreationWhileRebuiltPlansRefresh() throws Exception {
        String name = "snapshot_plan_chat";
        Map<String, String> originalProperties = properties("original-model");
        String originalId = manager().createProvider(name, AIProviderType.CHAT, originalProperties, "");
        ExecPlan template = getExecPlan(
                "select ai_complete(provider => '" + name + "', model => 'explicit-model', prompt => "
                        + "ai_complete(provider => '" + name + "', model => 'explicit-model', prompt => k1)) from t7");
        Map<String, String> alteredProperties = Map.of("model", "altered-model", "api_key", "altered-key",
                "endpoint", "https://altered.example.test/v1/chat/completions", "timeout_ms", "2400");
        manager().alterProvider(name, alteredProperties, false);
        Assertions.assertEquals(originalId, manager().getProvider(name).getId());
        assertSnapshot(template, originalProperties);
        assertSnapshot(rebuild(template), alteredProperties);
        ExecPlan afterAlter = getExecPlan(
                "select ai_complete(provider => '" + name + "', model => 'explicit-model', prompt => "
                        + "ai_complete(provider => '" + name + "', model => 'explicit-model', prompt => k1)) from t7");
        assertSnapshot(afterAlter, alteredProperties);
        manager().dropProvider(name, false);
        Assertions.assertThrows(SemanticException.class, () -> rebuild(template));
        String recreatedId = manager().createProvider(name, AIProviderType.CHAT, properties("recreated-model"), "");
        Assertions.assertNotEquals(originalId, recreatedId);
        assertSnapshot(template, originalProperties);
        assertSnapshot(rebuild(template), properties("recreated-model"));
        ExecPlan next = getExecPlan(
                "select ai_complete(provider => '" + name + "', model => 'explicit-model', prompt => "
                        + "ai_complete(provider => '" + name + "', model => 'explicit-model', prompt => k1)) from t7");
        assertSnapshot(next, properties("recreated-model"));
        manager().dropProvider(name, true);
    }

    @Test
    public void testPlanSnapshotSurvivesSqlProtocolAlterWhileNewPlanningFails() throws Exception {
        String name = "protocol_alter_plan_chat";
        Map<String, String> originalProperties = properties("original-model");
        manager().createProvider(name, AIProviderType.CHAT, originalProperties, "");
        String sql = "select ai_complete(provider => '" + name + "', model => 'explicit-model', prompt => "
                + "ai_complete(provider => '" + name + "', model => 'explicit-model', prompt => k1)) from t7";
        try {
            ExecPlan template = getExecPlan(sql);
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "ALTER AI PROVIDER " + name + " SET ('protocol' = 'anthropic')", connectContext), connectContext);

            assertSnapshot(template, originalProperties);
            Assertions.assertThrows(SemanticException.class, () -> rebuild(template));
            SemanticException error = Assertions.assertThrows(SemanticException.class, () -> getExecPlan(sql));
            Assertions.assertEquals("AI functions only support an OPENAI provider protocol now, not ANTHROPIC",
                    error.getDetailMsg());
        } finally {
            manager().dropProvider(name, true);
        }
    }

    @Test
    public void testOrdinaryQueryHasNoProviderDependencies() throws Exception {
        new MockUp<AIProviderMgr>() {
            @Mock
            public AIProvider getProvider(String name) {
                throw new AssertionError("An ordinary query must not access the Provider registry");
            }
        };
        Assertions.assertTrue(aiNodes(getExecPlan("select k1 from t7")).isEmpty());
    }

    @Test
    public void testOptimizedAwayProviderCallsAreAuthorizedWithoutCapturingConfiguration() throws Exception {
        Assertions.assertThrows(SemanticException.class, () -> getExecPlan("select k1 from (select k1, "
                + "ai_complete(provider => 'missing_provider', model => 'explicit-model', prompt => k1) as unused from t7) t"));
        Assertions.assertThrows(SemanticException.class, () -> getExecPlan(
                "select ai_embed(provider => 'missing_provider', model => 'explicit-model', input => k1) from t7 where false"));
        new MockUp<AIProvider>() {
            @Mock
            public TAIProvider toThrift() {
                throw new AssertionError("An eliminated call must not capture execution configuration");
            }
        };
        Assertions.assertTrue(aiNodes(getExecPlan("select k1 from (select k1, ai_complete(provider => "
                + "'text_plan_chat', model => 'explicit-model', prompt => k1) as unused from t7) t")).isEmpty());
        Assertions.assertTrue(aiNodes(getExecPlan("select ai_embed(provider => 'text_plan_embedding', model => "
                + "'explicit-model', input => k1) from t7 where false")).isEmpty());
    }

    @Test
    public void testExplicitCallsDoNotRequireDefaultProviders() throws Exception {
        String chatEndpoint = AIProviderTestUtil.chatProperty("endpoint");
        String embeddingEndpoint = AIProviderTestUtil.embeddingProperty("endpoint");
        AIProviderTestUtil.setChatProperty("endpoint", "");
        AIProviderTestUtil.setEmbeddingProperty("endpoint", "");
        try {
            Assertions.assertFalse(aiNodes(getExecPlan("select ai_complete(provider => 'text_plan_chat', model => "
                    + "'explicit-model', prompt => k1), ai_embed(provider => 'text_plan_embedding', model => "
                    + "'explicit-model', input => k1) from t7")).isEmpty());
        } finally {
            AIProviderTestUtil.setChatProperty("endpoint", chatEndpoint);
            AIProviderTestUtil.setEmbeddingProperty("endpoint", embeddingEndpoint);
        }
    }

    @Test
    public void testDefaultAndExplicitCallsShareProviderSnapshot() throws Exception {
        GlobalStateMgr.getCurrentState().getAIProviderMgr().setDefaultProvider("text_plan_chat");
        List<TAIProjectNode> nodes = aiNodes(getExecPlan("select ai_complete(k1), ai_complete(provider => "
                + "'text_plan_chat', model => 'explicit-model', prompt => k1) from t7"));
        Set<String> ids = nodes.stream().flatMap(node -> node.getAi_provider_mapping().keySet().stream())
                .collect(java.util.stream.Collectors.toSet());
        Assertions.assertEquals(Set.of(manager().getProvider("text_plan_chat").getId()), ids);
    }

    @Test
    public void testViewExpansionUsesCurrentProviderConfiguration() throws Exception {
        String name = "view_plan_chat";
        String replacement = "view_replacement_chat";
        String originalId = manager().createProvider(name, AIProviderType.CHAT, properties("view-model"), "");
        manager().createProvider(replacement, AIProviderType.CHAT, properties("replacement-model"), "");
        try {
            starRocksAssert.withView("create view provider_view as select ai_complete(provider => '" + name
                    + "', model => 'explicit-model', prompt => ai_complete(provider => '" + name + "', model => "
                    + "'explicit-model', prompt => k1)) as result from t7", () -> {
                        ExecPlan first = getExecPlan("select result from provider_view");
                        assertSnapshot(first, properties("view-model"));

                        manager().alterProvider(name, properties("altered-view-model"), false);
                        assertSnapshot(getExecPlan("select result from provider_view"),
                                properties("altered-view-model"));
                        assertSnapshot(first, properties("view-model"));

                        manager().dropProvider(name, false);
                        Assertions.assertThrows(SemanticException.class,
                                () -> getExecPlan("select result from provider_view"));
                        String recreatedId = manager().createProvider(
                                name, AIProviderType.CHAT, properties("recreated-view-model"), "");
                        Assertions.assertNotEquals(originalId, recreatedId);
                        assertSnapshot(getExecPlan("select result from provider_view"),
                                properties("recreated-view-model"));

                        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                                "alter view provider_view as select ai_complete(provider => '" + replacement
                                        + "', model => 'explicit-model', prompt => ai_complete(provider => '"
                                        + replacement + "', model => 'explicit-model', prompt => k1)) as result from t7",
                                connectContext), connectContext);
                        ExecPlan alteredView = getExecPlan("select result from provider_view");
                        assertSnapshot(alteredView, properties("replacement-model"));
                        Assertions.assertEquals(Set.of(manager().getProvider(replacement).getId()), aiNodes(alteredView).get(0)
                                .getAi_provider_mapping().keySet());
                    });
        } finally {
            manager().dropProvider(name, true);
            manager().dropProvider(replacement, true);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"ai_complete", "ai_embed"})
    public void testQueryDumpCannotRebindAnUncapturedProvider(String function) {
        QueryDumpInfo dump = new QueryDumpInfo(connectContext);
        String name = function.equals("ai_complete") ? "text_plan_chat" : "text_plan_embedding";
        dump.setOriginStmt("select " + function + "('prompt', '" + name + "', 'explicit-model')");
        String previousDatabase = connectContext.getDatabase();
        try {
            SemanticException error = Assertions.assertThrows(SemanticException.class,
                    () -> UtFrameUtils.getPlanFragmentFromQueryDump(connectContext, dump));
            Assertions.assertTrue(error.getMessage().contains("Query dump replay does not support AI providers"));
            error = Assertions.assertThrows(SemanticException.class,
                    () -> UtFrameUtils.getNewPlanAndFragmentFromDump(connectContext, dump));
            Assertions.assertTrue(error.getMessage().contains("Query dump replay does not support AI providers"));
            error = Assertions.assertThrows(SemanticException.class,
                    () -> UtFrameUtils.replaySql(connectContext, dump.getOriginStmt()));
            Assertions.assertTrue(error.getMessage().contains("Query dump replay does not support AI providers"));
        } finally {
            connectContext.setDatabase(previousDatabase);
        }
    }

    @Test
    public void testNormalQueryDumpCollectionDoesNotCaptureProviderCredentials() throws Exception {
        boolean previous = connectContext.getSessionVariable().getEnableQueryDump();
        connectContext.getSessionVariable().setEnableQueryDump(true);
        try {
            String dump = getDumpString("select ai_complete(provider => 'text_plan_chat', model => "
                    + "'explicit-model', prompt => k1) from t7");
            Assertions.assertTrue(dump.contains("ai_complete"));
            Assertions.assertFalse(dump.contains("test-key-original-model"));
            Assertions.assertFalse(dump.contains("ai_provider_mapping"));
        } finally {
            connectContext.getSessionVariable().setEnableQueryDump(previous);
        }
    }

    @Test
    public void testReplayRejectsProviderCallsAfterViewExpansion() throws Exception {
        starRocksAssert.withView("create view replay_provider_view as select ai_complete(provider => "
                + "'text_plan_chat', model => 'explicit-model', prompt => k1) as result from t7", () -> {
                    SemanticException error = Assertions.assertThrows(SemanticException.class,
                            () -> UtFrameUtils.replaySql(connectContext, "select * from replay_provider_view"));
                    Assertions.assertTrue(error.getMessage()
                            .contains("Query dump replay does not support AI providers"));
                });
    }

    @Test
    public void testSchemaRetryCapturesRecreatedProvider() throws Exception {
        String name = "retry_plan_chat";
        String originalId = manager().createProvider(name, AIProviderType.CHAT, properties("original-model"), "");
        String sql = "select ai_complete(provider => '" + name + "', model => 'explicit-model', prompt => "
                + "ai_complete(provider => '" + name + "', model => 'explicit-model', prompt => k1)) from t7";
        boolean previousLock = connectContext.getSessionVariable().isCboUseDBLock();
        connectContext.getSessionVariable().setCboUseDBLock(false);
        AtomicInteger validations = new AtomicInteger();
        try (MockedStatic<OptimisticVersion> versions = Mockito.mockStatic(
                OptimisticVersion.class, Mockito.CALLS_REAL_METHODS)) {
            versions.when(() -> OptimisticVersion.validateTableUpdate(Mockito.any(), Mockito.anyLong()))
                    .thenAnswer(invocation -> {
                        if (validations.incrementAndGet() == 1) {
                            manager().dropProvider(name, false);
                            manager().createProvider(name, AIProviderType.CHAT, properties("recreated-model"), "");
                            return false;
                        }
                        return invocation.callRealMethod();
                    });
            ExecPlan plan = getExecPlan(sql);
            Assertions.assertTrue(validations.get() >= 2, "The actual optimistic planning retry must run");
            Assertions.assertNotEquals(originalId, manager().getProvider(name).getId());
            assertSnapshot(plan, properties("recreated-model"));
        } finally {
            connectContext.getSessionVariable().setCboUseDBLock(previousLock);
        }
        ExecPlan next = getExecPlan(sql);
        assertSnapshot(next, properties("recreated-model"));
        manager().dropProvider(name, true);
    }

    private static ExecPlan rebuild(ExecPlan template) {
        return PlanFragmentBuilder.createPhysicalPlan(template.getPhysicalPlan(), connectContext,
                template.getOutputColumns(), template.getColumnRefFactory(), template.getColNames(),
                TResultSinkType.MYSQL_PROTOCAL, true, false);
    }

    private static void assertSnapshot(ExecPlan plan, Map<String, String> properties) {
        List<TAIProjectNode> nodes = aiNodes(plan);
        Assertions.assertEquals(2, nodes.size());
        for (TAIProjectNode node : nodes) {
            Assertions.assertEquals(1, node.getAi_provider_mapping().size());
            String key = node.getAi_provider_mapping().keySet().iterator().next();
            TAIProvider config = node.getAi_provider_mapping().get(key);
            Assertions.assertEquals(config.getId(), key);
            Assertions.assertEquals(properties.get("model"), config.getParams().get("model"));
            Assertions.assertEquals(properties.get("endpoint"), config.getParams().get("endpoint"));
            Assertions.assertEquals(properties.get("api_key"), config.getParams().get("api_key"));
            Assertions.assertEquals(properties.get("timeout_ms"), config.getParams().get("timeout_ms"));
        }
    }

    private static AIProviderMgr manager() {
        return GlobalStateMgr.getCurrentState().getAIProviderMgr();
    }

    private static Map<String, String> properties(String model) {
        return Map.of("endpoint", "https://models.example.test/v1/inference", "model", model,
                "api_key", "test-key-" + model, "timeout_ms", "1200");
    }

    private static List<TAIProjectNode> aiNodes(ExecPlan plan) {
        return plan.getFragments().stream().flatMap(fragment -> fragment.getPlanRoot().treeToThrift().getNodes().stream())
                .filter(node -> node.getNode_type() == TPlanNodeType.AI_PROJECT_NODE)
                .map(node -> node.getAi_project_node()).toList();
    }
    @Test
    public void testDefaultSelectionRefreshesAtEachBinding() throws Exception {
        String replacement = "replacement_default_chat";
        manager().createProvider(replacement, AIProviderType.CHAT, properties("replacement-model"), "");
        manager().setDefaultProvider("text_plan_chat");
        try {
            ExecPlan template = getExecPlan("select ai_complete(k1) from t7");
            AIProjectNode node = (AIProjectNode) template.getFragments().stream()
                    .flatMap(fragment -> fragment.collectNodes().stream())
                    .filter(AIProjectNode.class::isInstance).findFirst().orElseThrow();
            ExecPlan plan = new ExecPlan(connectContext, List.of(), null, List.of(), false);
            Map<String, AIProvider> first = plan.bindAIProviders(new HashMap<>(node.getSlotMap()));
            manager().alterProvider("text_plan_chat", properties("altered-model"), false);
            manager().setDefaultProvider(replacement);
            Map<String, AIProvider> repeated = plan.bindAIProviders(new HashMap<>(node.getSlotMap()));
            Assertions.assertNotSame(first.values().iterator().next(), repeated.values().iterator().next());
            Assertions.assertEquals("replacement-model", repeated.values().iterator().next().getModel());
            Map<String, AIProvider> fresh =
                    new ExecPlan(connectContext, List.of(), null, List.of(), false)
                            .bindAIProviders(new HashMap<>(node.getSlotMap()));
            Assertions.assertEquals("replacement-model", fresh.values().iterator().next().getModel());
        } finally {
            manager().alterProvider("text_plan_chat", properties("original-model"), false);
            manager().setDefaultProvider(AIProviderTestUtil.defaultName(AIProviderType.CHAT));
            manager().dropProvider(replacement, true);
        }
    }

    @Test
    public void testMissingDefaultFailsEvenInExplainAndPrunedCalls() throws Exception {
        AIProviderTestUtil.clearDefaults();
        for (String expression : List.of("ai_complete('p')", "ai_complete(prompt => 'p', model => 'model')", "ai_embed('p')",
                "ai_embed(input => 'p', model => 'model')", "ai_translate('hello', '', 'zh')")) {
            SemanticException error = Assertions.assertThrows(SemanticException.class,
                    () -> getExecPlan("explain select " + expression));
            Assertions.assertTrue(error.getMessage().contains("SET <provider_name> AS DEFAULT AI PROVIDER"));
        }
        Assertions.assertThrows(SemanticException.class,
                () -> getExecPlan("select k1 from (select k1, ai_complete(k1) as unused from t7) t"));
        Assertions.assertFalse(aiNodes(getExecPlan(
                "select ai_complete(k1, 'text_plan_chat', 'model') from t7")).isEmpty());
    }
}
