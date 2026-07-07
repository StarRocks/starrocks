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

package com.starrocks.context.embedding;

import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.context.error.ContextErrorCode;
import com.starrocks.context.error.ContextException;
import com.starrocks.server.AIProviderMgr;
import com.starrocks.server.GlobalStateMgr;
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

public class EmbeddingConfigJsonTest {

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
        // Wipe shared singleton state between tests so each one starts from a clean mgr.
        AIProviderMgr mgr = GlobalStateMgr.getCurrentState().getAIProviderMgr();
        Field idToProvider = AIProviderMgr.class.getDeclaredField("idToProvider");
        idToProvider.setAccessible(true);
        idToProvider.set(mgr, new HashMap<>());
        Field defaultId = AIProviderMgr.class.getDeclaredField("defaultProviderId");
        defaultId.setAccessible(true);
        defaultId.set(mgr, "");
    }

    @Test
    public void testReturnsNullWhenNoDefaultProvider() {
        // Fresh state: no provider has been created/set as default in this test
        Assertions.assertNull(EmbeddingConfigJson.build());
    }

    @Test
    public void testRequireBuildThrowsWhenNoDefaultProvider() {
        ContextException ex = Assertions.assertThrows(ContextException.class,
                EmbeddingConfigJson::requireBuild);
        Assertions.assertSame(ContextErrorCode.VECTOR_NOT_READY, ex.getCode());
        Assertions.assertTrue(ex.getMessage().contains("DEFAULT EMBEDDING PROVIDER"),
                ex.getMessage());
    }

    @Test
    public void testApiKeyIsInlinedPlaintextAndNotEnvPrefix() throws Exception {
        AIProviderMgr mgr = GlobalStateMgr.getCurrentState().getAIProviderMgr();
        Map<String, String> props = new LinkedHashMap<>();
        props.put(AIProvider.PROPERTY_ENDPOINT, "https://api.openai.com/v1/embeddings");
        props.put(AIProvider.PROPERTY_MODEL, "text-embedding-3-small");
        props.put(AIProvider.PROPERTY_DIMENSIONS, "1536");
        props.put(AIProvider.PROPERTY_API_KEY, "sk-unit-test-key");
        mgr.createProvider("ep_plaintext", AIProviderType.EMBEDDING, props, "unit test");
        mgr.setDefaultProvider("ep_plaintext");

        String json = EmbeddingConfigJson.build();
        Assertions.assertNotNull(json);
        // The api_key MUST be inlined as plaintext (no env. prefix).
        Assertions.assertTrue(json.contains("\"api_key\":\"sk-unit-test-key\""), json);
        Assertions.assertFalse(json.contains("\"api_key\":\"env."), json);
        Assertions.assertTrue(json.contains("\"endpoint\":\"https://api.openai.com/v1/embeddings\""), json);
        Assertions.assertTrue(json.contains("\"model\":\"text-embedding-3-small\""), json);
        Assertions.assertTrue(json.contains("\"dimensions\":1536"), json);
    }

    @Test
    public void testNoApiKeyOmitsFieldAndBeAcceptsIt() throws Exception {
        // Local/self-hosted providers don't need an auth header. The FE must serialize a
        // valid config_json that simply lacks the api_key field — the BE side accepts the
        // missing field (matches the relaxed BE contract in ai_functions.cpp).
        AIProviderMgr mgr = GlobalStateMgr.getCurrentState().getAIProviderMgr();
        Map<String, String> props = new LinkedHashMap<>();
        props.put(AIProvider.PROPERTY_ENDPOINT, "http://local-llm:8080/v1/embeddings");
        props.put(AIProvider.PROPERTY_MODEL, "local-model");
        mgr.createProvider("ep_local", AIProviderType.EMBEDDING, props, null);
        mgr.setDefaultProvider("ep_local");

        String json = EmbeddingConfigJson.build();
        Assertions.assertNotNull(json);
        Assertions.assertFalse(json.contains("api_key"),
                "config_json must omit api_key entirely when provider has no key: " + json);
        Assertions.assertTrue(json.contains("\"endpoint\":\"http://local-llm:8080/v1/embeddings\""), json);
    }

    @Test
    public void testEscapesQuotesInEndpoint() throws Exception {
        AIProviderMgr mgr = GlobalStateMgr.getCurrentState().getAIProviderMgr();
        Map<String, String> props = new LinkedHashMap<>();
        props.put(AIProvider.PROPERTY_ENDPOINT, "https://example.com/\"weird\"");
        props.put(AIProvider.PROPERTY_MODEL, "m1");
        mgr.createProvider("ep_quotes", AIProviderType.EMBEDDING, props, null);
        mgr.setDefaultProvider("ep_quotes");

        String json = EmbeddingConfigJson.build();
        Assertions.assertNotNull(json);
        Assertions.assertTrue(json.contains("https://example.com/\\\"weird\\\""), json);
    }
}
