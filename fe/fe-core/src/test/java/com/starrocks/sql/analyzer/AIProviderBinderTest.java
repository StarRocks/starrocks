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

import com.starrocks.common.Config;
import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.server.AIProviderMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.AIModelConfigs;
import com.starrocks.sql.common.AIProviderBindings;
import com.starrocks.thrift.TAIModelSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AIProviderBinderTest {
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
    public void testOrdinaryAndSystemCallsDoNotReadProviderMetadata() {
        AIProviderMgr manager = Mockito.mock(AIProviderMgr.class);
        Assertions.assertSame(AIProviderBindings.EMPTY, AIProviderBinder.bind(analyzeSuccess("select 1"), manager));
        Config.ai_default_chat_endpoint = "https://example.test/v1/chat/completions";
        Config.ai_default_chat_model = "default-model";
        Config.ai_default_chat_provider = "openai_compatible";
        Assertions.assertSame(AIProviderBindings.EMPTY,
                AIProviderBinder.bind(analyzeSuccess("select ai_complete('p')"), manager));
        Mockito.verifyNoInteractions(manager);
    }

    @Test
    public void testAllNamesAreReadOnceAndReanalysisKeepsOriginalSnapshot() {
        AIProvider chat = provider(101, "chat", AIProviderType.CHAT);
        AIProvider embedding = provider(102, "embedding", AIProviderType.EMBEDDING);
        AIProviderMgr manager = Mockito.mock(AIProviderMgr.class);
        Mockito.when(manager.getProvidersByNames(Set.of("chat", "embedding")))
                .thenReturn(Map.of("chat", chat, "embedding", embedding));
        StatementBase stmt = analyzeSuccess("select ai_custom_query(concat('ch', 'at'), "
                + "ai_custom_query('chat', 'p')), ai_custom_embedding('embedding', 'p')");
        AIProviderBindings bindings = AIProviderBinder.bind(stmt, manager);
        Mockito.verify(manager).getProvidersByNames(Set.of("chat", "embedding"));
        Mockito.verifyNoMoreInteractions(manager);
        AIModelConfigs.ModelConfig chatConfig = bindings.getRequiredConfig("chat");
        AIModelConfigs.ModelConfig embeddingConfig = bindings.getRequiredConfig("embedding");
        Assertions.assertEquals(TAIModelSource.PROVIDER, chatConfig.source());
        Assertions.assertEquals(chat.getId(), chatConfig.providerId());
        Assertions.assertEquals("CHAT", chatConfig.capability());
        Assertions.assertEquals("TEXT_EMBEDDING", embeddingConfig.capability());
        Assertions.assertEquals(3, embeddingConfig.dimensions());
        Assertions.assertEquals("provider:" + chat.getId(), bindings.configurationId("chat"));
        chat.mergeParams(Map.of("model", "changed-model", "api_key", "changed-key", "timeout_ms", "2400",
                "protocol", "anthropic"));
        embedding.mergeParams(Map.of("dimensions", "6"));
        AIProviderBinder.validateBindings(stmt, bindings);
        Assertions.assertEquals("provider-model", chatConfig.model());
        Assertions.assertEquals("test-provider-key", chatConfig.apiKey());
        Assertions.assertEquals(1200, chatConfig.timeoutMs());
        Assertions.assertEquals(3, embeddingConfig.dimensions());
        Assertions.assertEquals("openai_compatible", chatConfig.toThrift().getChat().getProvider());
        Mockito.verifyNoMoreInteractions(manager);
        SemanticException error = Assertions.assertThrows(SemanticException.class,
                () -> AIProviderBinder.bind(stmt, manager));
        Assertions.assertEquals("AI functions require an OPENAI provider protocol, not ANTHROPIC", error.getDetailMsg());
    }

    @ParameterizedTest
    @CsvSource({"CHAT, ANTHROPIC", "CHAT, COHERE", "EMBEDDING, ANTHROPIC", "EMBEDDING, COHERE"})
    public void testUnsupportedProtocolsFailAtBinding(AIProviderType type, String protocol) {
        AIProvider provider = provider(103, "unsupported", type);
        provider.mergeParams(Map.of("protocol", protocol));
        AIProviderMgr manager = Mockito.mock(AIProviderMgr.class);
        Mockito.when(manager.getProvidersByNames(Set.of("unsupported"))).thenReturn(Map.of("unsupported", provider));
        String function = type == AIProviderType.CHAT ? "ai_custom_query" : "ai_custom_embedding";
        StatementBase stmt = analyzeSuccess("select " + function + "('unsupported', 'p')");

        SemanticException error = Assertions.assertThrows(SemanticException.class,
                () -> AIProviderBinder.bind(stmt, manager));
        Assertions.assertEquals("AI functions require an OPENAI provider protocol, not " + protocol, error.getDetailMsg());
        Mockito.verify(manager).getProvidersByNames(Set.of("unsupported"));
        Mockito.verifyNoMoreInteractions(manager);
    }

    @Test
    public void testMissingAndWrongCapabilityFailBeforePlanning() {
        AIProviderMgr manager = Mockito.mock(AIProviderMgr.class);
        StatementBase stmt = analyzeSuccess("select ai_custom_query('embedding', 'p')");
        Mockito.when(manager.getProvidersByNames(Set.of("embedding"))).thenReturn(Map.of());
        Assertions.assertThrows(SemanticException.class, () -> AIProviderBinder.bind(stmt, manager));
        Mockito.when(manager.getProvidersByNames(Set.of("embedding")))
                .thenReturn(Map.of("embedding", provider(102, "embedding", AIProviderType.EMBEDDING)));
        Assertions.assertThrows(SemanticException.class, () -> AIProviderBinder.bind(stmt, manager));
        Mockito.when(manager.getProvidersByNames(Set.of("embedding")))
                .thenReturn(Map.of("embedding", provider(102, "embedding", AIProviderType.RERANK)));
        Assertions.assertThrows(SemanticException.class, () -> AIProviderBinder.bind(stmt, manager));
    }

    @Test
    public void testReanalysisCannotAddAnUncapturedDependency() {
        AIProviderBindings original = new AIProviderBindings(Map.of("chat",
                AIModelConfigs.fromProvider(provider(101, "chat", AIProviderType.CHAT))));
        StatementBase rewritten = analyzeSuccess("select ai_custom_query('other', 'p')");
        Assertions.assertThrows(SemanticException.class, () -> AIProviderBinder.validateBindings(rewritten, original));
    }

    @Test
    public void testBindingsDetachTheirInputMap() {
        AIModelConfigs.ModelConfig config = AIModelConfigs.fromProvider(provider(101, "chat", AIProviderType.CHAT));
        Map<String, AIModelConfigs.ModelConfig> configs = new HashMap<>(Map.of("chat", config));
        AIProviderBindings bindings = new AIProviderBindings(configs);
        configs.clear();
        Assertions.assertEquals(config, bindings.getRequiredConfig("chat"));
    }

    @Test
    public void testManagerBatchSnapshotDetachesMutableProviderMetadata() {
        AIProviderMgr manager = new AIProviderMgr();
        AIProvider provider = provider(101, "chat", AIProviderType.CHAT);
        manager.replayCreateProvider(provider);
        Map<String, AIProvider> captured = manager.getProvidersByNames(Set.of("chat", "missing"));
        Assertions.assertEquals(Set.of("chat"), captured.keySet());
        Assertions.assertNotSame(provider, captured.get("chat"));
        provider.mergeParams(Map.of("model", "new-model", "api_key", "new-key"));
        Assertions.assertEquals("provider-model", captured.get("chat").getModel());
        Assertions.assertEquals("test-provider-key", captured.get("chat").getApiKey());
        captured.get("chat").mergeParams(Map.of("model", "snapshot-only-model"));
        Assertions.assertEquals("new-model", manager.getProvider("chat").getModel());
    }

    private static AIProvider provider(long id, String name, AIProviderType type) {
        return new AIProvider(new UUID(0, id).toString(), name, type,
                Map.of("endpoint", "https://example.test/v1/inference", "model", "provider-model",
                        "api_key", "test-provider-key", "timeout_ms", "1200", "dimensions", "3"), "");
    }
}
