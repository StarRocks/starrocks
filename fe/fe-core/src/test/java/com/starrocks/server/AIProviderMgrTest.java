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

package com.starrocks.server;

import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/** Tests for the unified AIProvider registry: per-type defaults, rerank type, and — critically —
 *  backward-compatible load of the pre-unification (embedding-only, untyped) persisted format. */
public class AIProviderMgrTest {

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterAll
    public static void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @BeforeEach
    public void resetState() throws Exception {
        AIProviderMgr mgr = GlobalStateMgr.getCurrentState().getAIProviderMgr();
        setField(mgr, "idToProvider", new HashMap<>());
        setField(mgr, "defaultByType", new EnumMap<>(AIProviderType.class));
        setField(mgr, "defaultProviderId", "");
    }

    private static void setField(Object o, String name, Object val) throws Exception {
        Field f = AIProviderMgr.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(o, val);
    }

    private static Map<String, String> embProps() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put(AIProvider.PROPERTY_ENDPOINT, "https://api.openai.com/v1/embeddings");
        p.put(AIProvider.PROPERTY_MODEL, "text-embedding-3-small");
        p.put(AIProvider.PROPERTY_DIMENSIONS, "1536");
        return p;
    }

    private static Map<String, String> rerankProps() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put(AIProvider.PROPERTY_ENDPOINT, "https://openrouter.ai/api/v1/rerank");
        p.put(AIProvider.PROPERTY_MODEL, "cohere/rerank-4-fast");
        return p;
    }

    @Test
    public void testPerTypeDefaultsAreIndependent() throws Exception {
        AIProviderMgr mgr = GlobalStateMgr.getCurrentState().getAIProviderMgr();
        mgr.createProvider("emb", AIProviderType.EMBEDDING, embProps(), null);
        mgr.createProvider("rr", AIProviderType.RERANK, rerankProps(), null);
        mgr.setDefaultProvider("emb");
        mgr.setDefaultProvider("rr");

        Assertions.assertEquals("emb", mgr.getDefaultProvider(AIProviderType.EMBEDDING).getName());
        Assertions.assertEquals("rr", mgr.getDefaultProvider(AIProviderType.RERANK).getName());
        Assertions.assertEquals(AIProviderType.RERANK, mgr.getProvider("rr").getType());
        Assertions.assertEquals(1, mgr.listProviders(AIProviderType.RERANK).size());
        Assertions.assertEquals(1, mgr.listProviders(AIProviderType.EMBEDDING).size());
        // Setting the rerank default must not disturb the embedding default.
        mgr.setDefaultProvider("rr");
        Assertions.assertEquals("emb", mgr.getDefaultProvider(AIProviderType.EMBEDDING).getName());
    }

    @Test
    public void testGsonRoundTripPreservesTypesAndDefaults() throws Exception {
        AIProviderMgr mgr = GlobalStateMgr.getCurrentState().getAIProviderMgr();
        mgr.createProvider("emb", AIProviderType.EMBEDDING, embProps(), "e");
        mgr.createProvider("rr", AIProviderType.RERANK, rerankProps(), "r");
        mgr.setDefaultProvider("emb");
        mgr.setDefaultProvider("rr");

        String json = GsonUtils.GSON.toJson(mgr);
        AIProviderMgr restored = GsonUtils.GSON.fromJson(json, AIProviderMgr.class);
        Assertions.assertEquals("emb", restored.getDefaultProvider(AIProviderType.EMBEDDING).getName());
        Assertions.assertEquals("rr", restored.getDefaultProvider(AIProviderType.RERANK).getName());
        Assertions.assertEquals(AIProviderType.RERANK, restored.getProvider("rr").getType());
    }

    @Test
    public void testLegacyEmbeddingFormatMigrates() {
        // Pre-unification image/journal shape: single "defaultProviderId", no per-type map, and
        // provider records with no "t" (type) tag. Must load as EMBEDDING with the default migrated.
        String legacy = "{"
                + "\"defaultProviderId\":\"id1\","
                + "\"idToProvider\":{\"id1\":{"
                + "  \"i\":\"id1\",\"n\":\"legacy_emb\","
                + "  \"p\":{\"endpoint\":\"https://x/v1/embeddings\",\"model\":\"m\",\"dimensions\":\"1536\"},"
                + "  \"c\":\"\"}}}";
        AIProviderMgr restored = GsonUtils.GSON.fromJson(legacy, AIProviderMgr.class);
        AIProvider p = restored.getProvider("legacy_emb");
        Assertions.assertNotNull(p);
        Assertions.assertEquals(AIProviderType.EMBEDDING, p.getType());
        Assertions.assertNotNull(restored.getDefaultProvider(AIProviderType.EMBEDDING));
        Assertions.assertEquals("legacy_emb", restored.getDefaultProvider(AIProviderType.EMBEDDING).getName());
        Assertions.assertNull(restored.getDefaultProvider(AIProviderType.RERANK));
    }
}
