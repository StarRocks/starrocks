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

package com.starrocks.analysis;

import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.AIProviderMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AIProviderAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.aiprovider.AlterAIProviderStmt;
import com.starrocks.sql.ast.aiprovider.CreateAIProviderStmt;
import com.starrocks.sql.ast.aiprovider.DropAIProviderStmt;
import com.starrocks.sql.ast.aiprovider.SetDefaultAIProviderStmt;
import com.starrocks.sql.parser.NodePosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AIProviderAnalysisTest {
    private AIProviderMgr providers;
    private GlobalStateMgr state;

    @BeforeEach
    public void setUp() {
        providers = new AIProviderMgr();
        state = mock(GlobalStateMgr.class);
        when(state.getAIProviderMgr()).thenReturn(providers);
        providers.replayCreateProvider(storedProvider(AIProviderType.EMBEDDING));
    }

    private ConnectContext context() {
        ConnectContext context = new ConnectContext();
        context.setGlobalStateMgr(state);
        return context;
    }

    private AIProvider storedProvider(AIProviderType type) {
        Map<String, String> properties = switch (type) {
            case CHAT -> validChat();
            case EMBEDDING -> validEmbedding();
            case RERANK -> validRerank();
        };
        CreateAIProviderStmt statement = create(type.name(), properties);
        AIProviderAnalyzer.analyze(statement, context());
        return new AIProvider("p1-id", "p1", type, statement.getProperties(), "");
    }

    @Test
    public void testAlterValidatesPropertiesAgainstStoredTypeWithoutMutatingMetadata() {
        for (AIProviderType type : AIProviderType.values()) {
            AIProvider provider = storedProvider(type);
            providers.replayCreateProvider(provider);
            for (String key : new String[] {"unknown_property", "typo_timeout_ms", "dimensions", "max_documents"}) {
                if (type == AIProviderType.EMBEDDING && key.equals("dimensions")
                        || type == AIProviderType.RERANK && key.equals("max_documents")) {
                    continue;
                }
                Map<String, String> before = Map.copyOf(provider.getParams());
                AlterAIProviderStmt statement = new AlterAIProviderStmt(false, "p1",
                        new LinkedHashMap<>(Map.of("model", "replacement", key, "256")), NodePosition.ZERO);
                SemanticException error = Assertions.assertThrows(SemanticException.class,
                        () -> AIProviderAnalyzer.analyze(statement, context()));
                Assertions.assertTrue(error.getMessage().contains("Unknown property '" + key + "'"));
                Assertions.assertEquals(before, providers.getProvider("p1").getParams());
            }
        }
    }

    @Test
    public void testAlterAllowsStoredTypeSpecificProperty() {
        for (AIProviderType type : new AIProviderType[] {AIProviderType.EMBEDDING, AIProviderType.RERANK}) {
            providers.replayCreateProvider(storedProvider(type));
            String key = type == AIProviderType.EMBEDDING ? "dimensions" : "max_documents";
            AIProviderAnalyzer.analyze(new AlterAIProviderStmt(false, "p1",
                    new LinkedHashMap<>(Map.of(key, "256")), NodePosition.ZERO), context());
        }
    }

    @Test
    public void testMissingProviderIsHandledByManagerIncludingIfExists() throws Exception {
        for (boolean ifExists : new boolean[] {false, true}) {
            AlterAIProviderStmt statement = new AlterAIProviderStmt(ifExists, "missing",
                    new LinkedHashMap<>(Map.of("model", "new-model")), NodePosition.ZERO);
            Assertions.assertDoesNotThrow(() -> AIProviderAnalyzer.analyze(statement, context()));
        }
        Assertions.assertDoesNotThrow(() -> providers.alterProvider("missing", Map.of("model", "new-model"), true));
        Assertions.assertThrows(com.starrocks.common.MetaNotFoundException.class,
                () -> providers.alterProvider("missing", Map.of("model", "new-model"), false));
        Assertions.assertNull(providers.getProvider("missing"));
    }


    private static Map<String, String> validEmbedding() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("endpoint", "https://api.openai.com/v1/embeddings");
        p.put("model", "text-embedding-3-small");
        return p;
    }

    private static Map<String, String> validRerank() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("endpoint", "https://openrouter.ai/api/v1/rerank");
        p.put("model", "cohere/rerank-4-fast");
        return p;
    }

    private static Map<String, String> validChat() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("endpoint", "https://api.anthropic.com/v1/messages");
        p.put("model", "claude-sonnet-4-5");
        return p;
    }

    private static CreateAIProviderStmt create(String type, Map<String, String> props) {
        return new CreateAIProviderStmt(false, "p1", type, props, null, NodePosition.ZERO);
    }

    @Test
    public void testCreateEmbeddingValid() {
        AIProviderAnalyzer.analyze(create("embedding", validEmbedding()), context());
    }

    @Test
    public void testCreateRerankValid() {
        Map<String, String> p = validRerank();
        p.put("max_documents", "500");
        AIProviderAnalyzer.analyze(create("rerank", p), context());
    }

    @Test
    public void testCreateRejectsUnknownType() {
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(create("bogus", validEmbedding()), context()));
    }

    @Test
    public void testCreateFillsDefaultProtocol() {
        // CREATE without protocol gets the type's default written into the statement properties.
        CreateAIProviderStmt emb = create("embedding", validEmbedding());
        AIProviderAnalyzer.analyze(emb, context());
        Assertions.assertEquals("openai", emb.getProperties().get("protocol"));

        CreateAIProviderStmt rr = create("rerank", validRerank());
        AIProviderAnalyzer.analyze(rr, context());
        Assertions.assertEquals("cohere", rr.getProperties().get("protocol"));

        CreateAIProviderStmt chat = create("chat", validChat());
        AIProviderAnalyzer.analyze(chat, context());
        Assertions.assertEquals("openai", chat.getProperties().get("protocol"));

        // An explicit value is kept as written.
        Map<String, String> p = validChat();
        p.put("protocol", "anthropic");
        CreateAIProviderStmt explicit = create("chat", p);
        AIProviderAnalyzer.analyze(explicit, context());
        Assertions.assertEquals("anthropic", explicit.getProperties().get("protocol"));
    }

    @Test
    public void testCreateChatAnthropic() {
        Map<String, String> p = validChat();
        p.put("protocol", "anthropic");
        AIProviderAnalyzer.analyze(create("chat", p), context());
    }

    @Test
    public void testCreateRerankCohere() {
        Map<String, String> p = validRerank();
        p.put("protocol", "cohere");
        AIProviderAnalyzer.analyze(create("rerank", p), context());
    }

    @Test
    public void testCreateRejectsInvalidProtocol() {
        Map<String, String> p = validChat();
        p.put("protocol", "gemini");
        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(create("chat", p), context()));
        Assertions.assertTrue(ex.getMessage().contains("invalid AI provider protocol"), ex.getMessage());

        Map<String, String> empty = validChat();
        empty.put("protocol", "");
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(create("chat", empty), context()));
    }

    @Test
    public void testAlterProtocol() {
        // ALTER is type-agnostic: any supported protocol value passes, only the value shape is checked.
        Map<String, String> ok = new LinkedHashMap<>();
        ok.put("protocol", "anthropic");
        AIProviderAnalyzer.analyze(
                new AlterAIProviderStmt(false, "p1", ok, NodePosition.ZERO), context());

        Map<String, String> bad = new LinkedHashMap<>();
        bad.put("protocol", "bogus");
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(
                        new AlterAIProviderStmt(false, "p1", bad, NodePosition.ZERO), context()));

        Map<String, String> empty = new LinkedHashMap<>();
        empty.put("protocol", "");
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(
                        new AlterAIProviderStmt(false, "p1", empty, NodePosition.ZERO), context()));
    }

    @Test
    public void testCreateMissingEndpoint() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("model", "m");
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(create("embedding", p), context()));
    }

    @Test
    public void testCreateMissingModel() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("endpoint", "https://x.example.com/v1/embeddings");
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(create("embedding", p), context()));
    }

    @Test
    public void testCreateRejectsNonHttpEndpoint() {
        Map<String, String> p = validEmbedding();
        p.put("endpoint", "ftp://x.example.com/v1/embeddings");
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(create("embedding", p), context()));
    }

    @Test
    public void testCreateRejectsNonPositiveDim() {
        Map<String, String> p = validEmbedding();
        p.put("dimensions", "0");
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(create("embedding", p), context()));
    }

    @Test
    public void testCreateRejectsUnknownProperty() {
        Map<String, String> p = validEmbedding();
        p.put("extra_thing", "x");
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(create("embedding", p), context()));
    }

    @Test
    public void testCreateRejectsEmbeddingKeyForRerankType() {
        // dimensions belongs to the embedding allowlist, not rerank.
        Map<String, String> p = validRerank();
        p.put("dimensions", "1536");
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(create("rerank", p), context()));
    }

    @Test
    public void testCreateRejectsEmptyName() {
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(
                        new CreateAIProviderStmt(false, "", "embedding", validEmbedding(), null, NodePosition.ZERO),
                        context()));
    }

    @Test
    public void testAlterEmptyPropertiesRejected() {
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(
                        new AlterAIProviderStmt(false, "p1", new LinkedHashMap<>(), NodePosition.ZERO),
                        context()));
    }

    @Test
    public void testAlterValid() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("api_key", "sk-rotated");
        AIProviderAnalyzer.analyze(
                new AlterAIProviderStmt(false, "p1", p, NodePosition.ZERO), context());
    }

    @Test
    public void testAlterRejectsEmptyEndpoint() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("endpoint", "");
        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(
                        new AlterAIProviderStmt(false, "p1", p, NodePosition.ZERO), context()));
        Assertions.assertTrue(ex.getMessage().contains("endpoint"), ex.getMessage());
    }

    @Test
    public void testAlterAllowsEmptyApiKey() {
        // Empty api_key is meaningful: it disables the Authorization header for local providers.
        Map<String, String> p = new LinkedHashMap<>();
        p.put("api_key", "");
        AIProviderAnalyzer.analyze(
                new AlterAIProviderStmt(false, "p1", p, NodePosition.ZERO), context());
    }

    @Test
    public void testDropEmptyName() {
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(
                        new DropAIProviderStmt(false, "", NodePosition.ZERO), context()));
    }

    @Test
    public void testSetDefaultEmptyName() {
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(
                        new SetDefaultAIProviderStmt("", NodePosition.ZERO), context()));
    }
}
