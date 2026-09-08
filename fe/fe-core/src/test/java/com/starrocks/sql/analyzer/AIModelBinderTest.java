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

import com.starrocks.catalog.AIModel;
import com.starrocks.common.Config;
import com.starrocks.server.AIModelMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.AIModelBindings;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Set;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AIModelBinderTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @AfterEach
    public void clearConfig() {
        Config.ai_default_chat_endpoint = "";
        Config.ai_default_chat_model = "";
        Config.ai_default_chat_provider = "";
    }

    @Test
    public void testOrdinaryAndSystemCallsDoNotReadNamedModelMetadata() {
        AIModelMgr manager = Mockito.mock(AIModelMgr.class);
        Assertions.assertSame(AIModelBindings.EMPTY, AIModelBinder.bind(analyzeSuccess("select 1"), manager));
        Config.ai_default_chat_endpoint = "https://example.test/v1/chat/completions";
        Config.ai_default_chat_model = "default-model";
        Config.ai_default_chat_provider = "openai_compatible";
        Assertions.assertSame(AIModelBindings.EMPTY,
                AIModelBinder.bind(analyzeSuccess("select ai_complete('p')"), manager));
        Mockito.verifyNoInteractions(manager);
    }

    @Test
    public void testAllNamesAreReadOnceAndReanalysisKeepsOriginalRevision() throws Exception {
        AIModel chat = model(101, "chat", "CHAT");
        AIModel embedding = model(102, "embedding", "TEXT_EMBEDDING");
        AIModelMgr manager = Mockito.mock(AIModelMgr.class);
        Mockito.when(manager.getModelsByNames(Set.of("chat", "embedding")))
                .thenReturn(Map.of("chat", chat, "embedding", embedding));
        StatementBase stmt = analyzeSuccess("select ai_custom_query(concat('ch', 'at'), "
                + "ai_custom_query('chat', 'p')), ai_custom_embedding('embedding', 'p')");
        AIModelBindings bindings = AIModelBinder.bind(stmt, manager);
        Mockito.verify(manager).getModelsByNames(Set.of("chat", "embedding"));
        Mockito.verifyNoMoreInteractions(manager);
        Assertions.assertEquals(2, bindings.modelsRequiringUsage().size());
        Assertions.assertSame(chat, bindings.getRequiredModel("chat"));
        Assertions.assertEquals("model:101:1", bindings.configurationId("chat"));
        AIModelBinder.validateBindings(stmt, bindings);
        Mockito.verifyNoMoreInteractions(manager);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> bindings.modelsRequiringUsage().clear());
    }

    @Test
    public void testMissingAndWrongCapabilityFailBeforePlanning() throws Exception {
        AIModelMgr manager = Mockito.mock(AIModelMgr.class);
        StatementBase stmt = analyzeSuccess("select ai_custom_query('embedding', 'p')");
        Mockito.when(manager.getModelsByNames(Set.of("embedding"))).thenReturn(Map.of());
        Assertions.assertThrows(SemanticException.class, () -> AIModelBinder.bind(stmt, manager));
        Mockito.when(manager.getModelsByNames(Set.of("embedding")))
                .thenReturn(Map.of("embedding", model(102, "embedding", "TEXT_EMBEDDING")));
        Assertions.assertThrows(SemanticException.class, () -> AIModelBinder.bind(stmt, manager));
    }

    @Test
    public void testReanalysisCannotAddAnUnauthorizedDependency() throws Exception {
        AIModelBindings original = new AIModelBindings(Map.of("chat", model(101, "chat", "CHAT")));
        StatementBase rewritten = analyzeSuccess("select ai_custom_query('other', 'p')");
        Assertions.assertThrows(SemanticException.class, () -> AIModelBinder.validateBindings(rewritten, original));
    }

    private static AIModel model(long id, String name, String capability) throws Exception {
        return AIModel.create(id, name, Map.of("capability", capability, "provider", "openai_compatible",
                "endpoint", "https://example.test/v1/inference", "model", "provider-model",
                "credential_ref", "TEST_MODEL"), "");
    }
}
