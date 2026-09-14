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

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AIProviderAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.aiprovider.AlterAIProviderStmt;
import com.starrocks.sql.ast.aiprovider.CreateAIProviderStmt;
import com.starrocks.sql.ast.aiprovider.DropAIProviderStmt;
import com.starrocks.sql.ast.aiprovider.SetDefaultAIProviderStmt;
import com.starrocks.sql.parser.NodePosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

public class AIProviderAnalysisTest {

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
        AIProviderAnalyzer.analyze(create("embedding", validEmbedding()), new ConnectContext());
    }

    @Test
    public void testCreateRerankValid() {
        Map<String, String> p = validRerank();
        p.put("max_documents", "500");
        AIProviderAnalyzer.analyze(create("rerank", p), new ConnectContext());
    }

    @Test
    public void testCreateRejectsUnknownType() {
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(create("bogus", validEmbedding()), new ConnectContext()));
    }

    @Test
    public void testCreateRejectsLegacyTextType() {
        // The placeholder type was renamed to chat; the error must advertise the new name.
        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(create("text", validChat()), new ConnectContext()));
        Assertions.assertTrue(ex.getMessage().contains("chat"), ex.getMessage());
    }

    @Test
    public void testCreateFillsDefaultProtocol() {
        // CREATE without protocol gets the type's default written into the statement properties.
        CreateAIProviderStmt emb = create("embedding", validEmbedding());
        AIProviderAnalyzer.analyze(emb, new ConnectContext());
        Assertions.assertEquals("openai", emb.getProperties().get("protocol"));

        CreateAIProviderStmt rr = create("rerank", validRerank());
        AIProviderAnalyzer.analyze(rr, new ConnectContext());
        Assertions.assertEquals("cohere", rr.getProperties().get("protocol"));

        CreateAIProviderStmt chat = create("chat", validChat());
        AIProviderAnalyzer.analyze(chat, new ConnectContext());
        Assertions.assertEquals("openai", chat.getProperties().get("protocol"));

        // An explicit value is kept as written.
        Map<String, String> p = validChat();
        p.put("protocol", "anthropic");
        CreateAIProviderStmt explicit = create("chat", p);
        AIProviderAnalyzer.analyze(explicit, new ConnectContext());
        Assertions.assertEquals("anthropic", explicit.getProperties().get("protocol"));
    }

    @Test
    public void testAlterDoesNotFillProtocol() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("timeout_ms", "5000");
        AlterAIProviderStmt stmt = new AlterAIProviderStmt(false, "p1", p, NodePosition.ZERO);
        AIProviderAnalyzer.analyze(stmt, new ConnectContext());
        Assertions.assertFalse(stmt.getProperties().containsKey("protocol"));
    }

    @Test
    public void testCreateChatAnthropic() {
        Map<String, String> p = validChat();
        p.put("protocol", "anthropic");
        AIProviderAnalyzer.analyze(create("chat", p), new ConnectContext());
    }

    @Test
    public void testCreateRerankCohere() {
        Map<String, String> p = validRerank();
        p.put("protocol", "cohere");
        AIProviderAnalyzer.analyze(create("rerank", p), new ConnectContext());
    }

    @Test
    public void testCreateRejectsProtocolNotAllowedForType() {
        Map<String, String> emb = validEmbedding();
        emb.put("protocol", "anthropic");
        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(create("embedding", emb), new ConnectContext()));
        Assertions.assertTrue(ex.getMessage().contains("not supported for AI provider type embedding"),
                ex.getMessage());

        Map<String, String> embCohere = validEmbedding();
        embCohere.put("protocol", "cohere");
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(create("embedding", embCohere), new ConnectContext()));

        Map<String, String> rr = validRerank();
        rr.put("protocol", "openai");
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(create("rerank", rr), new ConnectContext()));
    }

    @Test
    public void testCreateRejectsInvalidProtocol() {
        Map<String, String> p = validChat();
        p.put("protocol", "gemini");
        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(create("chat", p), new ConnectContext()));
        Assertions.assertTrue(ex.getMessage().contains("invalid AI provider protocol"), ex.getMessage());

        Map<String, String> empty = validChat();
        empty.put("protocol", "");
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(create("chat", empty), new ConnectContext()));
    }

    @Test
    public void testAlterProtocol() {
        // No provider named p1 is registered here, so only the type-independent checks apply; the
        // type-aware ALTER checks are covered in AIProviderMgrTest against a stored provider.
        Map<String, String> ok = new LinkedHashMap<>();
        ok.put("protocol", "anthropic");
        AIProviderAnalyzer.analyze(
                new AlterAIProviderStmt(false, "p1", ok, NodePosition.ZERO), new ConnectContext());

        Map<String, String> bad = new LinkedHashMap<>();
        bad.put("protocol", "bogus");
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(
                        new AlterAIProviderStmt(false, "p1", bad, NodePosition.ZERO), new ConnectContext()));

        Map<String, String> empty = new LinkedHashMap<>();
        empty.put("protocol", "");
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(
                        new AlterAIProviderStmt(false, "p1", empty, NodePosition.ZERO), new ConnectContext()));
    }

    @Test
    public void testCreateMissingEndpoint() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("model", "m");
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(create("embedding", p), new ConnectContext()));
    }

    @Test
    public void testCreateMissingModel() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("endpoint", "https://x.example.com/v1/embeddings");
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(create("embedding", p), new ConnectContext()));
    }

    @Test
    public void testCreateRejectsNonHttpEndpoint() {
        Map<String, String> p = validEmbedding();
        p.put("endpoint", "ftp://x.example.com/v1/embeddings");
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(create("embedding", p), new ConnectContext()));
    }

    @Test
    public void testCreateRejectsNonPositiveDim() {
        Map<String, String> p = validEmbedding();
        p.put("dimensions", "0");
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(create("embedding", p), new ConnectContext()));
    }

    @Test
    public void testCreateRejectsUnknownProperty() {
        Map<String, String> p = validEmbedding();
        p.put("extra_thing", "x");
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(create("embedding", p), new ConnectContext()));
    }

    @Test
    public void testCreateRejectsEmbeddingKeyForRerankType() {
        // dimensions belongs to the embedding allowlist, not rerank.
        Map<String, String> p = validRerank();
        p.put("dimensions", "1536");
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(create("rerank", p), new ConnectContext()));
    }

    @Test
    public void testCreateRejectsEmptyName() {
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(
                        new CreateAIProviderStmt(false, "", "embedding", validEmbedding(), null, NodePosition.ZERO),
                        new ConnectContext()));
    }

    @Test
    public void testAlterEmptyPropertiesRejected() {
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(
                        new AlterAIProviderStmt(false, "p1", new LinkedHashMap<>(), NodePosition.ZERO),
                        new ConnectContext()));
    }

    @Test
    public void testAlterValid() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("api_key", "sk-rotated");
        AIProviderAnalyzer.analyze(
                new AlterAIProviderStmt(false, "p1", p, NodePosition.ZERO), new ConnectContext());
    }

    @Test
    public void testAlterRejectsEmptyEndpoint() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("endpoint", "");
        SemanticException ex = Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(
                        new AlterAIProviderStmt(false, "p1", p, NodePosition.ZERO), new ConnectContext()));
        Assertions.assertTrue(ex.getMessage().contains("endpoint"), ex.getMessage());
    }

    @Test
    public void testAlterAllowsEmptyApiKey() {
        // Empty api_key is meaningful: it disables the Authorization header for local providers.
        Map<String, String> p = new LinkedHashMap<>();
        p.put("api_key", "");
        AIProviderAnalyzer.analyze(
                new AlterAIProviderStmt(false, "p1", p, NodePosition.ZERO), new ConnectContext());
    }

    @Test
    public void testDropEmptyName() {
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(
                        new DropAIProviderStmt(false, "", NodePosition.ZERO), new ConnectContext()));
    }

    @Test
    public void testSetDefaultEmptyName() {
        Assertions.assertThrows(SemanticException.class, () ->
                AIProviderAnalyzer.analyze(
                        new SetDefaultAIProviderStmt("", NodePosition.ZERO), new ConnectContext()));
    }
}
