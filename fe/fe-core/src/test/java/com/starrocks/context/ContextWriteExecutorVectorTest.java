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

package com.starrocks.context;

import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.server.AIProviderMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Pins the SYNC-only embedding write path. CONTEXT UPSERT emits an INSERT whose embedding
 * column is computed by the BE-side {@code embedding(text, parse_json(config))} scalar
 * function. The provider must be configured at write time — there is no async backfill.
 */
public class ContextWriteExecutorVectorTest {

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterAll
    public static void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @BeforeEach
    public void resetMgrState() throws Exception {
        AIProviderMgr mgr = GlobalStateMgr.getCurrentState().getAIProviderMgr();
        Field idToProvider = AIProviderMgr.class.getDeclaredField("idToProvider");
        idToProvider.setAccessible(true);
        idToProvider.set(mgr, new HashMap<>());
        Field defaultId = AIProviderMgr.class.getDeclaredField("defaultProviderId");
        defaultId.setAccessible(true);
        defaultId.set(mgr, "");
    }

    @Test
    public void testEmbeddingExpressionRejectsNullConfig() {
        // Provider unavailable → writer fails immediately. There is no daemon to backfill.
        SemanticException ex = Assertions.assertThrows(SemanticException.class,
                () -> ContextWriteExecutor.embeddingExpression(null, "hello"));
        Assertions.assertTrue(ex.getMessage().contains("EMBEDDING PROVIDER"), ex.getMessage());
    }

    @Test
    public void testEmbeddingExpressionEmptyTextYieldsEmptyArray() {
        // Empty text is a defensive sentinel — the writer filters empty-text fragments before
        // calling embeddingExpression, but if the call lands here we still return the empty-array
        // literal so the (NOT NULL) column accepts the row.
        Assertions.assertEquals("[]",
                ContextWriteExecutor.embeddingExpression("{\"endpoint\":\"x\"}", ""));
        Assertions.assertEquals("[]",
                ContextWriteExecutor.embeddingExpression("{\"endpoint\":\"x\"}", null));
    }

    @Test
    public void testEmbeddingExpressionWrapsTextAndConfig() {
        String configJson = "{\"endpoint\":\"http://x\"}";
        String expr = ContextWriteExecutor.embeddingExpression(configJson, "body");
        Assertions.assertTrue(expr.startsWith("embedding("), expr);
        Assertions.assertTrue(expr.contains("'body'"), expr);
        Assertions.assertTrue(expr.contains("parse_json("), expr);
    }

    @Test
    public void testBuildEmbeddingConfigJsonNullWhenNoDefaultProvider() {
        // No DEFAULT EMBEDDING PROVIDER → null config → caller fails fast.
        Assertions.assertNull(ContextWriteExecutor.buildEmbeddingConfigJson());
    }

    @Test
    public void testBuildEmbeddingConfigJsonInlinesApiKeyPlaintext() throws Exception {
        AIProviderMgr mgr = GlobalStateMgr.getCurrentState().getAIProviderMgr();
        Map<String, String> props = new LinkedHashMap<>();
        props.put(AIProvider.PROPERTY_ENDPOINT, "https://example.com/v1/embeddings");
        props.put(AIProvider.PROPERTY_MODEL, "text-embedding-3-small");
        props.put(AIProvider.PROPERTY_API_KEY, "sk-test-write-path");
        mgr.createProvider("ep_writer", AIProviderType.EMBEDDING, props, null);
        mgr.setDefaultProvider("ep_writer");

        String json = ContextWriteExecutor.buildEmbeddingConfigJson();
        Assertions.assertNotNull(json);
        // Plaintext is the new contract — the env. indirection has been removed entirely.
        Assertions.assertTrue(json.contains("\"api_key\":\"sk-test-write-path\""), json);
        Assertions.assertFalse(json.contains("\"api_key\":\"env."), json);
        Assertions.assertTrue(json.contains("\"endpoint\":\"https://example.com/v1/embeddings\""), json);
    }
}
