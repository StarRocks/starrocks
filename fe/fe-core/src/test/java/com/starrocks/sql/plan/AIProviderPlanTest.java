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

import com.starrocks.common.Config;
import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.planner.AIProjectNode;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.AIProviderMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.OptimisticVersion;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.AIModelConfigs;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.thrift.TAIModelConfiguration;
import com.starrocks.thrift.TAIModelSource;
import com.starrocks.thrift.TAIProjectNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
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
        oldChatEndpoint = Config.ai_default_chat_endpoint;
        oldChatModel = Config.ai_default_chat_model;
        oldChatProvider = Config.ai_default_chat_provider;
        Config.ai_default_chat_endpoint = "https://models.example.test/v1/chat/completions";
        Config.ai_default_chat_model = "system-chat-model";
        Config.ai_default_chat_provider = AIModelConfigs.OPENAI_COMPATIBLE_PROVIDER;
        manager().createProvider("text_plan_chat", AIProviderType.CHAT, properties("original-model"), "");
        manager().createProvider("text_plan_embedding", AIProviderType.EMBEDDING,
                Map.of("endpoint", "https://models.example.test/v1/embeddings", "model", "embedding-model",
                        "api_key", "test-embedding-key", "timeout_ms", "1500", "dimensions", "6"), "");
    }

    @AfterAll
    public static void restoreChatConfiguration() {
        Config.ai_default_chat_endpoint = oldChatEndpoint;
        Config.ai_default_chat_model = oldChatModel;
        Config.ai_default_chat_provider = oldChatProvider;
    }

    @AfterEach
    public void clearEmbeddingConfig() {
        Config.ai_default_embedding_endpoint = "";
        Config.ai_default_embedding_model = "";
        Config.ai_default_embedding_provider = "";
    }

    @Test
    public void testDirectPhysicalPlanCapturesCurrentProviderWithoutBindings() throws Exception {
        String name = "direct_physical_chat";
        manager().createProvider(name, AIProviderType.CHAT, properties("original-model"), "");
        try {
            ExecPlan original = getExecPlan(
                    "select ai_custom_query('" + name + "', ai_custom_query('" + name + "', k1)) from t7");
            manager().alterProvider(name, properties("current-model"), false);
            ExecPlan rebuilt = PlanFragmentBuilder.createPhysicalPlan(original.getPhysicalPlan(), connectContext,
                    original.getOutputColumns(), original.getColumnRefFactory(), original.getColNames(),
                    TResultSinkType.MYSQL_PROTOCAL, true, false);
            assertSnapshot(original, "provider:0", properties("original-model"));
            assertSnapshot(rebuilt, "provider:0", properties("current-model"));
        } finally {
            manager().dropProvider(name, true);
        }
    }

    @Test
    public void testExecPlanCachesProviderAndFreshPlanRefreshes() throws Exception {
        String name = "direct_exec_plan_chat";
        manager().createProvider(name, AIProviderType.CHAT, properties("original-model"), "");
        try {
            ExecPlan template = getExecPlan("select ai_custom_query('" + name + "', k1) from t7");
            AIProjectNode node = (AIProjectNode) template.getFragments().stream()
                    .flatMap(fragment -> fragment.collectNodes().stream())
                    .filter(AIProjectNode.class::isInstance).findFirst().orElseThrow();
            ExecPlan plan = new ExecPlan(connectContext, List.of(), null, List.of(), false);
            Map<String, AIModelConfigs.ModelConfig> first = plan.bindAIModelConfigs(new HashMap<>(node.getSlotMap()));
            manager().alterProvider(name, properties("current-model"), false);
            Map<String, AIModelConfigs.ModelConfig> repeated = plan.bindAIModelConfigs(new HashMap<>(node.getSlotMap()));
            Map<String, AIModelConfigs.ModelConfig> fresh =
                    new ExecPlan(connectContext, List.of(), null, List.of(), false)
                    .bindAIModelConfigs(new HashMap<>(node.getSlotMap()));
            Assertions.assertSame(first.get("provider:0"), repeated.get("provider:0"));
            Assertions.assertEquals("original-model", repeated.get("provider:0").model());
            Assertions.assertEquals("current-model", fresh.get("provider:0").model());
        } finally {
            manager().dropProvider(name, true);
        }
    }

    @Test
    public void testEmbeddingWireRouteAndOptions() throws Exception {
        Config.ai_default_embedding_endpoint = "https://models.example.test/v1/embeddings";
        Config.ai_default_embedding_model = "embedding-model";
        Config.ai_default_embedding_provider = "openai_compatible";
        TAIProjectNode node = aiNodes(getExecPlan("select ai_embed(k1, map{'dimensions': 3}) from t7")).get(0);
        Assertions.assertEquals(Set.of("__system_embedding__"), node.getAi_model_configs().keySet());
        TAIModelConfiguration config = node.getAi_model_configs().get("__system_embedding__");
        Assertions.assertFalse(config.isSetChat());
        Assertions.assertEquals("embedding-model", config.getEmbedding().getModel());
        Assertions.assertFalse(config.getEmbedding().isSetApi_key());
        Assertions.assertFalse(config.getEmbedding().isSetTimeout_ms());
        Assertions.assertFalse(config.getEmbedding().isSetDimensions());
        Assertions.assertFalse(config.isSetSource());
        Assertions.assertEquals(2, node.getSlot_map().values().iterator().next().getNodes().get(0).getNum_children());
    }

    @Test
    public void testProviderOnlyPlanDoesNotReadSystemDefaults() throws Exception {
        ExecPlan plan = getExecPlan("select ai_custom_query(concat('text_plan_', 'chat'), k1) from t7");
        TAIProjectNode chat = aiNodes(plan).get(0);
        String configId = "provider:0";
        Assertions.assertEquals(Set.of(configId), chat.getAi_model_configs().keySet());
        TAIModelConfiguration configuration = chat.getAi_model_configs().get(configId);
        Assertions.assertEquals("test-key-original-model", configuration.getChat().getApi_key());
        Assertions.assertEquals(1200, configuration.getChat().getTimeout_ms());
        Assertions.assertEquals(TAIModelSource.PROVIDER, configuration.getSource());
        Assertions.assertEquals("text_plan_chat",
                chat.getSlot_map().values().iterator().next().getNodes().get(1).getString_literal().getValue());
        ExecPlan embedding = getExecPlan(
                "select ai_custom_embedding('text_plan_embedding', k1, map{'dimensions': 3}) from t7");
        TAIModelConfiguration embeddingConfig = aiNodes(embedding).get(0).getAi_model_configs()
                .get("provider:0");
        Assertions.assertTrue(embeddingConfig.isSetEmbedding());
        Assertions.assertFalse(embeddingConfig.isSetChat());
        Assertions.assertEquals(TAIModelSource.PROVIDER, embeddingConfig.getSource());
        Assertions.assertEquals("test-embedding-key", embeddingConfig.getEmbedding().getApi_key());
        Assertions.assertEquals(1500, embeddingConfig.getEmbedding().getTimeout_ms());
        Assertions.assertEquals(6, embeddingConfig.getEmbedding().getDimensions());
        Assertions.assertThrows(Exception.class, () -> getExecPlan("select ai_custom_embedding('text_plan_chat', 'p')"));
        Assertions.assertThrows(Exception.class, () -> getExecPlan("select ai_custom_query('text_plan_embedding', 'p')"));
        Assertions.assertThrows(Exception.class, () -> getExecPlan("select ai_custom_query('missing_provider', 'p')"));
    }

    @Test
    public void testSqlCreatedProviderBindsItsExecutionConfiguration() throws Exception {
        String name = "ddl_plan_chat";
        String sql = "CREATE AI PROVIDER " + name + " TYPE chat PROPERTIES ("
                + "'endpoint' = 'https://models.example.test/v1/inference', "
                + "'model' = 'ddl-model', 'api_key' = 'test-ddl-key', 'timeout_ms' = '1200')";
        try {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, connectContext), connectContext);
            ExecPlan plan = getExecPlan("select ai_custom_query('" + name + "', k1) from t7");
            List<TAIProjectNode> nodes = aiNodes(plan);
            Assertions.assertEquals(1, nodes.size());
            String configId = "provider:0";
            Assertions.assertEquals(Set.of(configId), nodes.get(0).getAi_model_configs().keySet());
            TAIModelConfiguration configuration = nodes.get(0).getAi_model_configs().get(configId);
            Assertions.assertEquals(TAIModelSource.PROVIDER, configuration.getSource());
            Assertions.assertTrue(configuration.isSetChat());
            Assertions.assertFalse(configuration.isSetEmbedding());
            Assertions.assertEquals("https://models.example.test/v1/inference", configuration.getChat().getEndpoint());
            Assertions.assertEquals("ddl-model", configuration.getChat().getModel());
            Assertions.assertEquals("openai_compatible", configuration.getChat().getProvider());
            Assertions.assertEquals("test-ddl-key", configuration.getChat().getApi_key());
            Assertions.assertEquals(1200, configuration.getChat().getTimeout_ms());
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
                "select ai_custom_query('" + name + "', ai_custom_query('" + name + "', k1)) from t7");
        Map<String, String> alteredProperties = Map.of("model", "altered-model", "api_key", "altered-key",
                "endpoint", "https://altered.example.test/v1/chat/completions", "timeout_ms", "2400");
        manager().alterProvider(name, alteredProperties, false);
        Assertions.assertEquals(originalId, manager().getProvider(name).getId());
        assertSnapshot(template, "provider:0", originalProperties);
        assertSnapshot(rebuild(template), "provider:0", alteredProperties);
        ExecPlan afterAlter = getExecPlan(
                "select ai_custom_query('" + name + "', ai_custom_query('" + name + "', k1)) from t7");
        assertSnapshot(afterAlter, "provider:0", alteredProperties);
        manager().dropProvider(name, false);
        Assertions.assertThrows(SemanticException.class, () -> rebuild(template));
        String recreatedId = manager().createProvider(name, AIProviderType.CHAT, properties("recreated-model"), "");
        Assertions.assertNotEquals(originalId, recreatedId);
        assertSnapshot(template, "provider:0", originalProperties);
        assertSnapshot(rebuild(template), "provider:0", properties("recreated-model"));
        ExecPlan next = getExecPlan(
                "select ai_custom_query('" + name + "', ai_custom_query('" + name + "', k1)) from t7");
        assertSnapshot(next, "provider:0", properties("recreated-model"));
        manager().dropProvider(name, true);
    }

    @Test
    public void testPlanSnapshotSurvivesSqlProtocolAlterWhileNewPlanningFails() throws Exception {
        String name = "protocol_alter_plan_chat";
        Map<String, String> originalProperties = properties("original-model");
        manager().createProvider(name, AIProviderType.CHAT, originalProperties, "");
        String sql = "select ai_custom_query('" + name + "', ai_custom_query('" + name + "', k1)) from t7";
        try {
            ExecPlan template = getExecPlan(sql);
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "ALTER AI PROVIDER " + name + " SET ('protocol' = 'anthropic')", connectContext), connectContext);

            assertSnapshot(template, "provider:0", originalProperties);
            Assertions.assertThrows(SemanticException.class, () -> rebuild(template));
            SemanticException error = Assertions.assertThrows(SemanticException.class, () -> getExecPlan(sql));
            Assertions.assertEquals("AI functions require an OPENAI provider protocol, not ANTHROPIC", error.getDetailMsg());
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
                + "ai_custom_query('missing_provider', k1) as unused from t7) t"));
        Assertions.assertThrows(SemanticException.class, () -> getExecPlan(
                "select ai_custom_embedding('missing_provider', k1) from t7 where false"));
        new MockUp<AIModelConfigs>() {
            @Mock
            public AIModelConfigs.ModelConfig fromProvider(AIProvider provider) {
                throw new AssertionError("An eliminated call must not capture execution configuration");
            }
        };
        Assertions.assertTrue(aiNodes(getExecPlan("select k1 from (select k1, "
                + "ai_custom_query('text_plan_chat', k1) as unused from t7) t")).isEmpty());
        Assertions.assertTrue(aiNodes(getExecPlan("select ai_custom_embedding('text_plan_embedding', k1) "
                + "from t7 where false")).isEmpty());
    }

    @Test
    public void testProviderOnlyCallsIgnoreInvalidSystemConfiguration() throws Exception {
        String chatEndpoint = Config.ai_default_chat_endpoint;
        String embeddingEndpoint = Config.ai_default_embedding_endpoint;
        Config.ai_default_chat_endpoint = "invalid-system-chat-endpoint";
        Config.ai_default_embedding_endpoint = "invalid-system-embedding-endpoint";
        try {
            Assertions.assertFalse(aiNodes(getExecPlan("select ai_custom_query('text_plan_chat', k1), "
                    + "ai_custom_embedding('text_plan_embedding', k1) from t7")).isEmpty());
        } finally {
            Config.ai_default_chat_endpoint = chatEndpoint;
            Config.ai_default_embedding_endpoint = embeddingEndpoint;
        }
    }

    @Test
    public void testSystemAndNamedCallsDoNotUseRegistryDefaults() throws Exception {
        new MockUp<AIProviderMgr>() {
            @Mock
            public AIProvider getDefaultProvider(AIProviderType type) {
                throw new AssertionError("SYSTEM and named calls must not select a registry default");
            }
        };
        List<TAIProjectNode> nodes = aiNodes(getExecPlan("select ai_complete(k1), "
                + "ai_custom_query('text_plan_chat', k1) from t7"));
        Set<String> ids = nodes.stream().flatMap(node -> node.getAi_model_configs().keySet().stream())
                .collect(java.util.stream.Collectors.toSet());
        Assertions.assertEquals(Set.of(AIModelConfigs.SYSTEM_CHAT_CONFIG_ID, "provider:0"), ids);
    }

    @Test
    public void testViewExpansionUsesCurrentProviderConfiguration() throws Exception {
        String name = "view_plan_chat";
        String replacement = "view_replacement_chat";
        String originalId = manager().createProvider(name, AIProviderType.CHAT, properties("view-model"), "");
        manager().createProvider(replacement, AIProviderType.CHAT, properties("replacement-model"), "");
        try {
            starRocksAssert.withView("create view provider_view as select ai_custom_query('" + name
                    + "', ai_custom_query('" + name + "', k1)) as result from t7", () -> {
                        ExecPlan first = getExecPlan("select result from provider_view");
                        assertSnapshot(first, "provider:0", properties("view-model"));

                        manager().alterProvider(name, properties("altered-view-model"), false);
                        assertSnapshot(getExecPlan("select result from provider_view"), "provider:0",
                                properties("altered-view-model"));
                        assertSnapshot(first, "provider:0", properties("view-model"));

                        manager().dropProvider(name, false);
                        Assertions.assertThrows(SemanticException.class,
                                () -> getExecPlan("select result from provider_view"));
                        String recreatedId = manager().createProvider(
                                name, AIProviderType.CHAT, properties("recreated-view-model"), "");
                        Assertions.assertNotEquals(originalId, recreatedId);
                        assertSnapshot(getExecPlan("select result from provider_view"), "provider:0",
                                properties("recreated-view-model"));

                        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                                "alter view provider_view as select ai_custom_query('" + replacement
                                        + "', ai_custom_query('" + replacement + "', k1)) as result from t7",
                                connectContext), connectContext);
                        ExecPlan alteredView = getExecPlan("select result from provider_view");
                        assertSnapshot(alteredView, "provider:0", properties("replacement-model"));
                        Assertions.assertEquals(Set.of("provider:0"), aiNodes(alteredView).get(0)
                                .getAi_model_configs().keySet());
                    });
        } finally {
            manager().dropProvider(name, true);
            manager().dropProvider(replacement, true);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"ai_custom_query", "ai_custom_embedding"})
    public void testQueryDumpCannotRebindAnUncapturedProvider(String function) {
        QueryDumpInfo dump = new QueryDumpInfo(connectContext);
        String name = function.equals("ai_custom_query") ? "text_plan_chat" : "text_plan_embedding";
        dump.setOriginStmt("select " + function + "('" + name + "', 'prompt')");
        String previousDatabase = connectContext.getDatabase();
        try {
            SemanticException error = Assertions.assertThrows(SemanticException.class,
                    () -> UtFrameUtils.getPlanFragmentFromQueryDump(connectContext, dump));
            Assertions.assertTrue(error.getMessage().contains("Query dump replay does not support named AI providers"));
            error = Assertions.assertThrows(SemanticException.class,
                    () -> UtFrameUtils.getNewPlanAndFragmentFromDump(connectContext, dump));
            Assertions.assertTrue(error.getMessage().contains("Query dump replay does not support named AI providers"));
            error = Assertions.assertThrows(SemanticException.class,
                    () -> UtFrameUtils.replaySql(connectContext, dump.getOriginStmt()));
            Assertions.assertTrue(error.getMessage().contains("Query dump replay does not support named AI providers"));
        } finally {
            connectContext.setDatabase(previousDatabase);
        }
    }

    @Test
    public void testNormalQueryDumpCollectionDoesNotCaptureProviderCredentials() throws Exception {
        boolean previous = connectContext.getSessionVariable().getEnableQueryDump();
        connectContext.getSessionVariable().setEnableQueryDump(true);
        try {
            String dump = getDumpString("select ai_custom_query('text_plan_chat', k1) from t7");
            Assertions.assertTrue(dump.contains("ai_custom_query"));
            Assertions.assertFalse(dump.contains("test-key-original-model"));
            Assertions.assertFalse(dump.contains("ai_model_configs"));
        } finally {
            connectContext.getSessionVariable().setEnableQueryDump(previous);
        }
    }

    @Test
    public void testReplayRejectsProviderCallsAfterViewExpansion() throws Exception {
        starRocksAssert.withView("create view replay_provider_view as "
                + "select ai_custom_query('text_plan_chat', k1) as result from t7", () -> {
                    SemanticException error = Assertions.assertThrows(SemanticException.class,
                            () -> UtFrameUtils.replaySql(connectContext, "select * from replay_provider_view"));
                    Assertions.assertTrue(error.getMessage()
                            .contains("Query dump replay does not support named AI providers"));
                });
    }

    @Test
    public void testSystemCallsRemainReplayable() throws Exception {
        QueryDumpInfo dump = new QueryDumpInfo(connectContext);
        dump.setOriginStmt("select ai_complete('prompt')");
        String previousDatabase = connectContext.getDatabase();
        try {
            for (ExecPlan plan : List.of(UtFrameUtils.getPlanFragmentFromQueryDump(connectContext, dump),
                    UtFrameUtils.getNewPlanAndFragmentFromDump(connectContext, dump).second,
                    UtFrameUtils.replaySql(connectContext, dump.getOriginStmt()).second)) {
                TAIModelConfiguration config = aiNodes(plan).get(0).getAi_model_configs()
                        .get(AIModelConfigs.SYSTEM_CHAT_CONFIG_ID);
                Assertions.assertTrue(config.isSetChat());
                Assertions.assertFalse(config.isSetSource());
            }
        } finally {
            connectContext.setDatabase(previousDatabase);
        }
    }

    @Test
    public void testSchemaRetryCapturesRecreatedProvider() throws Exception {
        String name = "retry_plan_chat";
        String originalId = manager().createProvider(name, AIProviderType.CHAT, properties("original-model"), "");
        String sql = "select ai_custom_query('" + name + "', ai_custom_query('" + name + "', k1)) from t7";
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
            assertSnapshot(plan, "provider:0", properties("recreated-model"));
        } finally {
            connectContext.getSessionVariable().setCboUseDBLock(previousLock);
        }
        ExecPlan next = getExecPlan(sql);
        assertSnapshot(next, "provider:0", properties("recreated-model"));
        manager().dropProvider(name, true);
    }

    private static ExecPlan rebuild(ExecPlan template) {
        return PlanFragmentBuilder.createPhysicalPlan(template.getPhysicalPlan(), connectContext,
                template.getOutputColumns(), template.getColumnRefFactory(), template.getColNames(),
                TResultSinkType.MYSQL_PROTOCAL, true, false);
    }

    private static void assertSnapshot(ExecPlan plan, String key, Map<String, String> properties) {
        List<TAIProjectNode> nodes = aiNodes(plan);
        Assertions.assertEquals(2, nodes.size());
        for (TAIProjectNode node : nodes) {
            Assertions.assertEquals(Set.of(key), node.getAi_model_configs().keySet());
            TAIModelConfiguration config = node.getAi_model_configs().get(key);
            Assertions.assertEquals("provider:0", key);
            Assertions.assertEquals(TAIModelSource.PROVIDER, config.getSource());
            Assertions.assertEquals(properties.get("model"), config.getChat().getModel());
            Assertions.assertEquals(properties.get("endpoint"), config.getChat().getEndpoint());
            Assertions.assertEquals(properties.get("api_key"), config.getChat().getApi_key());
            Assertions.assertEquals(Long.parseLong(properties.get("timeout_ms")), config.getChat().getTimeout_ms());
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
}
