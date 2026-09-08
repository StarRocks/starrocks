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

import com.starrocks.catalog.AIModel;
import com.starrocks.common.Config;
import com.starrocks.server.AIModelMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.OptimisticVersion;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.AIModelBindings;
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

public class AITextModelPlanTest extends PlanTestBase {
    @BeforeAll
    public static void createModels() throws Exception {
        for (String capability : List.of("CHAT", "TEXT_EMBEDDING")) {
            String name = capability.equals("CHAT") ? "text_plan_chat" : "text_plan_embedding";
            manager().createModel(name, properties(capability, "original-model"), "", true);
        }
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
        Assertions.assertFalse(config.getText_embedding().isSetCredential_ref());
        Assertions.assertFalse(config.isSetSource());
        Assertions.assertFalse(config.isSetModel_id());
        Assertions.assertEquals(2, node.getSlot_map().values().iterator().next().getNodes().get(0).getNum_children());
    }

    @Test
    public void testNamedModelOnlyPlanDoesNotReadSystemDefaults() throws Exception {
        ExecPlan plan = getExecPlan("select ai_custom_query(concat('text_plan_', 'chat'), k1) from t7");
        TAIProjectNode chat = aiNodes(plan).get(0);
        String configId = plan.getAIModelBindings().configurationId("text_plan_chat");
        Assertions.assertEquals(Set.of(configId), chat.getAi_model_configs().keySet());
        TAIModelConfiguration configuration = chat.getAi_model_configs().get(configId);
        Assertions.assertEquals("TEST_MODEL", configuration.getChat().getCredential_ref());
        Assertions.assertEquals(TAIModelSource.AI_MODEL, configuration.getSource());
        Assertions.assertEquals(manager().getByName("text_plan_chat").getId(), configuration.getModel_id());
        Assertions.assertEquals("text_plan_chat",
                chat.getSlot_map().values().iterator().next().getNodes().get(1).getString_literal().getValue());
        ExecPlan embedding = getExecPlan(
                "select ai_custom_embedding('text_plan_embedding', k1, map{'dimensions': 3}) from t7");
        Assertions.assertTrue(aiNodes(embedding).get(0).getAi_model_configs()
                .get(embedding.getAIModelBindings().configurationId("text_plan_embedding")).isSetText_embedding());
        Assertions.assertThrows(Exception.class, () -> getExecPlan("select ai_custom_embedding('text_plan_chat', 'p')"));
        Assertions.assertThrows(Exception.class, () -> getExecPlan("select ai_custom_query('text_plan_embedding', 'p')"));
        Assertions.assertThrows(Exception.class, () -> getExecPlan("select ai_custom_query('missing_model', 'p')"));
    }

    @Test
    public void testBoundSnapshotSurvivesAlterAndSameNameRecreationAcrossAIProjects() throws Exception {
        String name = "snapshot_plan_chat";
        manager().createModel(name, properties("CHAT", "original-model"), "", true);
        ExecPlan template = getExecPlan(
                "select ai_custom_query('" + name + "', ai_custom_query('" + name + "', k1)) from t7");
        AIModelBindings bindings = template.getAIModelBindings();
        AIModel original = bindings.getRequiredModel(name);
        manager().alterModel(original, Map.of("model", "altered-model"), null);
        assertSnapshot(rebuild(template, bindings), bindings.configurationId(name), original.getId(), "original-model");
        manager().dropModel(manager().getByName(name));
        AIModel recreated = manager().createModel(name, properties("CHAT", "recreated-model"), "", false);
        Assertions.assertNotEquals(original.getId(), recreated.getId());
        assertSnapshot(rebuild(template, bindings), bindings.configurationId(name), original.getId(), "original-model");
        ExecPlan next = getExecPlan(
                "select ai_custom_query('" + name + "', ai_custom_query('" + name + "', k1)) from t7");
        assertSnapshot(next, next.getAIModelBindings().configurationId(name), recreated.getId(), "recreated-model");
        Assertions.assertThrows(Exception.class, () -> rebuild(template, AIModelBindings.EMPTY),
                "Physical planning must not recover a missing binding from mutable global metadata");
    }

    @Test
    public void testOrdinaryQueryHasNoNamedModelDependencies() throws Exception {
        Assertions.assertSame(AIModelBindings.EMPTY, getExecPlan("select k1 from t7").getAIModelBindings());
    }

    @Test
    public void testQueryDumpCannotRebindAnUncapturedModel() {
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
    public void testSchemaRetryDoesNotRebindARecreatedModel() throws Exception {
        String name = "retry_plan_chat";
        AIModel original = manager().createModel(name, properties("CHAT", "original-model"), "", true);
        String sql = "select ai_custom_query('" + name + "', ai_custom_query('" + name + "', k1)) from t7";
        boolean previousLock = connectContext.getSessionVariable().isCboUseDBLock();
        connectContext.getSessionVariable().setCboUseDBLock(false);
        AtomicInteger validations = new AtomicInteger();
        try (MockedStatic<OptimisticVersion> versions = Mockito.mockStatic(
                OptimisticVersion.class, Mockito.CALLS_REAL_METHODS)) {
            versions.when(() -> OptimisticVersion.validateTableUpdate(Mockito.any(), Mockito.anyLong()))
                    .thenAnswer(invocation -> {
                        if (validations.incrementAndGet() == 1) {
                            manager().dropModel(original);
                            manager().createModel(name, properties("CHAT", "recreated-model"), "", false);
                            return false;
                        }
                        return invocation.callRealMethod();
                    });
            ExecPlan plan = getExecPlan(sql);
            Assertions.assertTrue(validations.get() >= 2, "The actual optimistic planning retry must run");
            Assertions.assertSame(original, plan.getAIModelBindings().getRequiredModel(name));
            assertSnapshot(plan, plan.getAIModelBindings().configurationId(name), original.getId(), "original-model");
        } finally {
            connectContext.getSessionVariable().setCboUseDBLock(previousLock);
        }
        ExecPlan next = getExecPlan(sql);
        assertSnapshot(next, next.getAIModelBindings().configurationId(name),
                manager().getByName(name).getId(), "recreated-model");
    }

    private static ExecPlan rebuild(ExecPlan template, AIModelBindings bindings) {
        return PlanFragmentBuilder.createPhysicalPlan(template.getPhysicalPlan(), connectContext,
                template.getOutputColumns(), template.getColumnRefFactory(), template.getColNames(),
                TResultSinkType.MYSQL_PROTOCAL, true, false, bindings);
    }

    private static void assertSnapshot(ExecPlan plan, String key, long id, String providerModel) {
        List<TAIProjectNode> nodes = aiNodes(plan);
        Assertions.assertEquals(2, nodes.size());
        for (TAIProjectNode node : nodes) {
            Assertions.assertEquals(Set.of(key), node.getAi_model_configs().keySet());
            TAIModelConfiguration config = node.getAi_model_configs().get(key);
            Assertions.assertEquals(id, config.getModel_id());
            Assertions.assertEquals(providerModel, config.getChat().getModel());
        }
    }

    private static AIModelMgr manager() {
        return GlobalStateMgr.getCurrentState().getAIModelMgr();
    }

    private static Map<String, String> properties(String capability, String model) {
        return Map.of("capability", capability, "provider", "openai_compatible",
                "endpoint", "https://models.example.test/v1/inference", "model", model, "credential_ref", "TEST_MODEL");
    }

    private static List<TAIProjectNode> aiNodes(ExecPlan plan) {
        return plan.getFragments().stream().flatMap(fragment -> fragment.getPlanRoot().treeToThrift().getNodes().stream())
                .filter(node -> node.getNode_type() == TPlanNodeType.AI_PROJECT_NODE)
                .map(node -> node.getAi_project_node()).toList();
    }
}
