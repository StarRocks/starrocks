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
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.AIProviderMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.OptimisticVersion;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.AIModelConfigs;
import com.starrocks.sql.common.AIProviderBindings;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.thrift.TAIModelConfiguration;
import com.starrocks.thrift.TAIModelSource;
import com.starrocks.thrift.TAIProjectNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class AIProviderPlanTest extends PlanTestBase {
    @BeforeAll
    public static void createProviders() throws Exception {
        manager().createProvider("text_plan_chat", AIProviderType.CHAT, properties("original-model"), "");
        manager().createProvider("text_plan_embedding", AIProviderType.EMBEDDING,
                Map.of("endpoint", "https://models.example.test/v1/embeddings", "model", "embedding-model",
                        "api_key", "test-embedding-key", "timeout_ms", "1500", "dimensions", "6"), "");
    }

    @AfterEach
    public void clearEmbeddingConfig() {
        Config.ai_default_embedding_endpoint = "";
        Config.ai_default_embedding_model = "";
        Config.ai_default_embedding_provider = "";
    }

    @Test
    public void testEmbeddingWireRouteAndOptions() throws Exception {
        Config.ai_default_embedding_endpoint = "https://models.example.test/v1/embeddings";
        Config.ai_default_embedding_model = "embedding-model";
        Config.ai_default_embedding_provider = "openai_compatible";
        TAIProjectNode node = aiNodes(getExecPlan("select ai_embed(k1, map{'dimensions': 3}) from t7")).get(0);
        Assertions.assertEquals(Set.of("__system_text_embedding__"), node.getAi_model_configs().keySet());
        TAIModelConfiguration config = node.getAi_model_configs().get("__system_text_embedding__");
        Assertions.assertFalse(config.isSetChat());
        Assertions.assertEquals("embedding-model", config.getText_embedding().getModel());
        Assertions.assertFalse(config.getText_embedding().isSetApi_key());
        Assertions.assertFalse(config.getText_embedding().isSetTimeout_ms());
        Assertions.assertFalse(config.getText_embedding().isSetDimensions());
        Assertions.assertFalse(config.isSetSource());
        Assertions.assertFalse(config.isSetProvider_id());
        Assertions.assertEquals(2, node.getSlot_map().values().iterator().next().getNodes().get(0).getNum_children());
    }

    @Test
    public void testProviderOnlyPlanDoesNotReadSystemDefaults() throws Exception {
        ExecPlan plan = getExecPlan("select ai_custom_query(concat('text_plan_', 'chat'), k1) from t7");
        TAIProjectNode chat = aiNodes(plan).get(0);
        String configId = plan.getAIProviderBindings().configurationId("text_plan_chat");
        Assertions.assertEquals(Set.of(configId), chat.getAi_model_configs().keySet());
        TAIModelConfiguration configuration = chat.getAi_model_configs().get(configId);
        Assertions.assertEquals("test-key-original-model", configuration.getChat().getApi_key());
        Assertions.assertEquals(1200, configuration.getChat().getTimeout_ms());
        Assertions.assertEquals(TAIModelSource.PROVIDER, configuration.getSource());
        Assertions.assertEquals(manager().getProvider("text_plan_chat").getId(), configuration.getProvider_id());
        Assertions.assertEquals("text_plan_chat",
                chat.getSlot_map().values().iterator().next().getNodes().get(1).getString_literal().getValue());
        ExecPlan embedding = getExecPlan(
                "select ai_custom_embedding('text_plan_embedding', k1, map{'dimensions': 3}) from t7");
        TAIModelConfiguration embeddingConfig = aiNodes(embedding).get(0).getAi_model_configs()
                .get(embedding.getAIProviderBindings().configurationId("text_plan_embedding"));
        Assertions.assertTrue(embeddingConfig.isSetText_embedding());
        Assertions.assertFalse(embeddingConfig.isSetChat());
        Assertions.assertEquals(TAIModelSource.PROVIDER, embeddingConfig.getSource());
        Assertions.assertEquals(manager().getProvider("text_plan_embedding").getId(), embeddingConfig.getProvider_id());
        Assertions.assertEquals("test-embedding-key", embeddingConfig.getText_embedding().getApi_key());
        Assertions.assertEquals(1500, embeddingConfig.getText_embedding().getTimeout_ms());
        Assertions.assertEquals(6, embeddingConfig.getText_embedding().getDimensions());
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
            String providerId = manager().getProvider(name).getId();
            ExecPlan plan = getExecPlan("select ai_custom_query('" + name + "', k1) from t7");
            List<TAIProjectNode> nodes = aiNodes(plan);
            Assertions.assertEquals(1, nodes.size());
            String configId = "provider:" + providerId;
            Assertions.assertEquals(configId, plan.getAIProviderBindings().configurationId(name));
            Assertions.assertEquals(Set.of(configId), nodes.get(0).getAi_model_configs().keySet());
            TAIModelConfiguration configuration = nodes.get(0).getAi_model_configs().get(configId);
            Assertions.assertEquals(TAIModelSource.PROVIDER, configuration.getSource());
            Assertions.assertEquals(providerId, configuration.getProvider_id());
            Assertions.assertTrue(configuration.isSetChat());
            Assertions.assertFalse(configuration.isSetText_embedding());
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
    public void testBoundSnapshotSurvivesAlterAndSameNameRecreationAcrossAIProjects() throws Exception {
        String name = "snapshot_plan_chat";
        Map<String, String> originalProperties = properties("original-model");
        String originalId = manager().createProvider(name, AIProviderType.CHAT, originalProperties, "");
        ExecPlan template = getExecPlan(
                "select ai_custom_query('" + name + "', ai_custom_query('" + name + "', k1)) from t7");
        AIProviderBindings bindings = template.getAIProviderBindings();
        AIModelConfigs.ModelConfig original = bindings.getRequiredConfig(name);
        Map<String, String> alteredProperties = Map.of("model", "altered-model", "api_key", "altered-key",
                "endpoint", "https://altered.example.test/v1/chat/completions", "timeout_ms", "2400");
        manager().alterProvider(name, alteredProperties, false);
        Assertions.assertEquals(originalId, manager().getProvider(name).getId());
        assertSnapshot(rebuild(template, bindings), bindings.configurationId(name), originalId, originalProperties);
        ExecPlan afterAlter = getExecPlan(
                "select ai_custom_query('" + name + "', ai_custom_query('" + name + "', k1)) from t7");
        Assertions.assertEquals(bindings.configurationId(name), afterAlter.getAIProviderBindings().configurationId(name));
        assertSnapshot(afterAlter, bindings.configurationId(name), originalId, alteredProperties);
        manager().dropProvider(name, false);
        String recreatedId = manager().createProvider(name, AIProviderType.CHAT, properties("recreated-model"), "");
        Assertions.assertNotEquals(originalId, recreatedId);
        Assertions.assertEquals("original-model", original.model());
        Assertions.assertEquals("test-key-original-model", original.apiKey());
        assertSnapshot(rebuild(template, bindings), bindings.configurationId(name), originalId, originalProperties);
        ExecPlan next = getExecPlan(
                "select ai_custom_query('" + name + "', ai_custom_query('" + name + "', k1)) from t7");
        Assertions.assertNotEquals(bindings.configurationId(name), next.getAIProviderBindings().configurationId(name));
        assertSnapshot(next, next.getAIProviderBindings().configurationId(name), recreatedId, properties("recreated-model"));
        Assertions.assertThrows(Exception.class, () -> rebuild(template, AIProviderBindings.EMPTY),
                "Physical planning must not recover a missing binding from mutable global metadata");
    }

    @Test
    public void testBoundSnapshotSurvivesSqlProtocolAlterWhileNewPlanningFails() throws Exception {
        String name = "protocol_alter_plan_chat";
        Map<String, String> originalProperties = properties("original-model");
        String originalId = manager().createProvider(name, AIProviderType.CHAT, originalProperties, "");
        String sql = "select ai_custom_query('" + name + "', ai_custom_query('" + name + "', k1)) from t7";
        try {
            ExecPlan template = getExecPlan(sql);
            AIProviderBindings bindings = template.getAIProviderBindings();
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "ALTER AI PROVIDER " + name + " SET ('protocol' = 'anthropic')", connectContext), connectContext);

            assertSnapshot(rebuild(template, bindings), bindings.configurationId(name), originalId, originalProperties);
            Assertions.assertEquals("openai_compatible", bindings.getRequiredConfig(name).provider());
            SemanticException error = Assertions.assertThrows(SemanticException.class, () -> getExecPlan(sql));
            Assertions.assertEquals("AI functions require an OPENAI provider protocol, not ANTHROPIC", error.getDetailMsg());
        } finally {
            manager().dropProvider(name, true);
        }
    }

    @Test
    public void testOrdinaryQueryHasNoProviderDependencies() throws Exception {
        Assertions.assertSame(AIProviderBindings.EMPTY, getExecPlan("select k1 from t7").getAIProviderBindings());
    }

    @Test
    public void testViewExpansionUsesCurrentProviderBindings() throws Exception {
        String name = "view_plan_chat";
        String replacement = "view_replacement_chat";
        String originalId = manager().createProvider(name, AIProviderType.CHAT, properties("view-model"), "");
        String replacementId = manager().createProvider(
                replacement, AIProviderType.CHAT, properties("replacement-model"), "");
        try {
            starRocksAssert.withView("create view provider_view as select ai_custom_query('" + name
                    + "', ai_custom_query('" + name + "', k1)) as result from t7", () -> {
                        ExecPlan first = getExecPlan("select result from provider_view");
                        assertSnapshot(first, "provider:" + originalId, originalId, properties("view-model"));

                        manager().alterProvider(name, properties("altered-view-model"), false);
                        assertSnapshot(getExecPlan("select result from provider_view"), "provider:" + originalId,
                                originalId, properties("altered-view-model"));
                        assertSnapshot(first, "provider:" + originalId, originalId, properties("view-model"));

                        manager().dropProvider(name, false);
                        Assertions.assertThrows(SemanticException.class,
                                () -> getExecPlan("select result from provider_view"));
                        String recreatedId = manager().createProvider(
                                name, AIProviderType.CHAT, properties("recreated-view-model"), "");
                        Assertions.assertNotEquals(originalId, recreatedId);
                        assertSnapshot(getExecPlan("select result from provider_view"), "provider:" + recreatedId,
                                recreatedId, properties("recreated-view-model"));

                        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                                "alter view provider_view as select ai_custom_query('" + replacement
                                        + "', ai_custom_query('" + replacement + "', k1)) as result from t7",
                                connectContext), connectContext);
                        ExecPlan alteredView = getExecPlan("select result from provider_view");
                        assertSnapshot(alteredView, "provider:" + replacementId, replacementId,
                                properties("replacement-model"));
                        Assertions.assertThrows(SemanticException.class,
                                () -> alteredView.getAIProviderBindings().getRequiredConfig(name));
                    });
        } finally {
            manager().dropProvider(name, true);
            manager().dropProvider(replacement, true);
        }
    }

    @Test
    public void testQueryDumpCannotRebindAnUncapturedProvider() {
        QueryDumpInfo dump = new QueryDumpInfo(connectContext);
        dump.setOriginStmt("select ai_custom_query('text_plan_chat', 'prompt')");
        String previousDatabase = connectContext.getDatabase();
        try {
            SemanticException error = Assertions.assertThrows(SemanticException.class,
                    () -> UtFrameUtils.getPlanFragmentFromQueryDump(connectContext, dump));
            Assertions.assertTrue(error.getMessage().contains("not bound to this statement"));
        } finally {
            connectContext.setDatabase(previousDatabase);
        }
    }

    @Test
    public void testSchemaRetryDoesNotRebindARecreatedProvider() throws Exception {
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
            Assertions.assertEquals(originalId, plan.getAIProviderBindings().getRequiredConfig(name).providerId());
            assertSnapshot(plan, plan.getAIProviderBindings().configurationId(name), originalId, properties("original-model"));
        } finally {
            connectContext.getSessionVariable().setCboUseDBLock(previousLock);
        }
        ExecPlan next = getExecPlan(sql);
        Assertions.assertNotEquals(originalId, next.getAIProviderBindings().getRequiredConfig(name).providerId());
        assertSnapshot(next, next.getAIProviderBindings().configurationId(name),
                manager().getProvider(name).getId(), properties("recreated-model"));
    }

    private static ExecPlan rebuild(ExecPlan template, AIProviderBindings bindings) {
        return PlanFragmentBuilder.createPhysicalPlan(template.getPhysicalPlan(), connectContext,
                template.getOutputColumns(), template.getColumnRefFactory(), template.getColNames(),
                TResultSinkType.MYSQL_PROTOCAL, true, false, bindings);
    }

    private static void assertSnapshot(ExecPlan plan, String key, String id, Map<String, String> properties) {
        List<TAIProjectNode> nodes = aiNodes(plan);
        Assertions.assertEquals(2, nodes.size());
        for (TAIProjectNode node : nodes) {
            Assertions.assertEquals(Set.of(key), node.getAi_model_configs().keySet());
            TAIModelConfiguration config = node.getAi_model_configs().get(key);
            Assertions.assertEquals("provider:" + id, key);
            Assertions.assertEquals(TAIModelSource.PROVIDER, config.getSource());
            Assertions.assertEquals(id, config.getProvider_id());
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
