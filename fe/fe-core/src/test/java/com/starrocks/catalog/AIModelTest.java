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

package com.starrocks.catalog;

import com.google.gson.JsonObject;
import com.starrocks.common.DdlException;
import com.starrocks.persist.gson.GsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AIModelTest {
    private static Map<String, String> properties() {
        return Map.of("capability", "CHAT", "provider", "openai_compatible",
                "endpoint", "https://models.example.test/v1/chat/completions", "model", "chat-model",
                "credential_ref", "TEST_MODEL");
    }

    @Test
    public void testTypedMetadataAndGsonRoundTrip() throws Exception {
        AIModel model = AIModel.create(10, "ChatModel", properties(), "comment");
        Assertions.assertEquals(1, model.getRevision());
        Assertions.assertEquals(AIModel.Capability.CHAT, model.getCapability());
        Assertions.assertEquals(AIModel.Provider.OPENAI_COMPATIBLE, model.getProvider());
        Assertions.assertEquals("openai_compatible", model.getProvider().getSqlName());
        Assertions.assertEquals("chat-model", model.getRemoteModel());
        AIModel restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(model), AIModel.class);
        restored.validatePersistedState();
        Assertions.assertEquals(model, restored);
        Assertions.assertEquals(model.hashCode(), restored.hashCode());
    }

    @Test
    public void testEmbeddingMetadata() throws Exception {
        Map<String, String> properties = new HashMap<>(properties());
        properties.put("capability", "TEXT_EMBEDDING");
        properties.put("endpoint", "https://models.example.test/v1/embeddings");
        AIModel model = AIModel.create(11, "EmbeddingModel", properties, null);
        properties.put("model", "mutated");
        Assertions.assertEquals(AIModel.Capability.TEXT_EMBEDDING, model.getCapability());
        Assertions.assertEquals("chat-model", model.getRemoteModel());
        Assertions.assertEquals("", model.getComment());
    }

    @Test
    public void testAlterReplacesSnapshotAndPreservesIdentity() throws Exception {
        AIModel before = AIModel.create(10, "ChatModel", properties(), "comment");
        AIModel after = before.withAlteredProperties(Map.of("model", "new-model"), "");
        Assertions.assertEquals(before.getId(), after.getId());
        Assertions.assertEquals(before.getName(), after.getName());
        Assertions.assertEquals(2, after.getRevision());
        Assertions.assertEquals("chat-model", before.getRemoteModel());
        Assertions.assertEquals("comment", before.getComment());
        Assertions.assertEquals("new-model", after.getRemoteModel());
        Assertions.assertEquals("", after.getComment());
        Assertions.assertSame(before, before.withAlteredProperties(Map.of("model", "chat-model"), null));
    }

    @Test
    public void testAlterProtectsCapabilityAndCredentialReference() throws Exception {
        AIModel model = AIModel.create(10, "ChatModel", properties(), "comment");
        Assertions.assertThrows(DdlException.class,
                () -> model.withAlteredProperties(Map.of("capability", "TEXT_EMBEDDING"), null));
        Assertions.assertThrows(DdlException.class,
                () -> model.withAlteredProperties(Map.of("credential_ref", "OTHER_REF"), null));
        Assertions.assertSame(model, model.withAlteredProperties(
                Map.of("capability", "CHAT", "credential_ref", "TEST_MODEL"), null));
    }

    @Test
    public void testInvalidPropertiesNeverEchoValues() {
        for (Map.Entry<String, String> invalid : Map.of(
                "api_key", "secret-api-key", "type", "ai_model", "credential_ref", "bad-ref",
                "provider", "OpenAI_Compatible", "capability", "IMAGE", "model", " ").entrySet()) {
            Map<String, String> properties = new HashMap<>(properties());
            properties.put(invalid.getKey(), invalid.getValue());
            DdlException error = Assertions.assertThrows(DdlException.class,
                    () -> AIModel.create(10, "ChatModel", properties, ""));
            Assertions.assertFalse(error.getMessage().contains("secret-api-key"));
        }
    }

    @Test
    public void testRejectUnsafeEndpoints() {
        for (String endpoint : new String[] {"http://models.example.test/v1/chat",
                "https://user:secret-api-key@models.example.test/v1/chat",
                "https://models.example.test/v1/chat?api_key=secret-api-key",
                "https://models.example.test/v1/chat#fragment", "https://models.example.test:0/v1/chat",
                "https://models.example.test:/v1/chat", "https://models.example.test:65536/v1/chat",
                "https://models.example.test/v1/\nchat"}) {
            Map<String, String> properties = new HashMap<>(properties());
            properties.put("endpoint", endpoint);
            DdlException error = Assertions.assertThrows(DdlException.class,
                    () -> AIModel.create(10, "ChatModel", properties, ""));
            Assertions.assertFalse(error.getMessage().contains("secret-api-key"));
        }
    }

    @Test
    public void testPersistedMetadataIsValidatedWithoutConstructor() {
        AIModel incomplete = GsonUtils.GSON.fromJson("{\"id\":10,\"name\":\"ChatModel\"}", AIModel.class);
        Assertions.assertThrows(IOException.class, incomplete::validatePersistedState);
    }

    @Test
    public void testAllPropertiesAreRequiredAndControlFree() {
        for (String key : properties().keySet()) {
            Map<String, String> missing = new HashMap<>(properties());
            missing.remove(key);
            Assertions.assertThrows(DdlException.class, () -> AIModel.create(10, "ChatModel", missing, ""));
            missing.put(key, "\n");
            Assertions.assertThrows(DdlException.class, () -> AIModel.create(10, "ChatModel", missing, ""));
        }
    }

    @Test
    public void testInvalidPersistedEnumAndRevisionOverflow() throws Exception {
        AIModel model = AIModel.create(10, "ChatModel", properties(), "");
        JsonObject json = GsonUtils.GSON.toJsonTree(model).getAsJsonObject();
        json.addProperty("provider", "unknown");
        AIModel invalid = GsonUtils.GSON.fromJson(json, AIModel.class);
        Assertions.assertThrows(IOException.class, invalid::validatePersistedState);
        json = GsonUtils.GSON.toJsonTree(model).getAsJsonObject();
        json.addProperty("revision", Long.MAX_VALUE);
        AIModel lastRevision = GsonUtils.GSON.fromJson(json, AIModel.class);
        lastRevision.validatePersistedState();
        Assertions.assertThrows(DdlException.class,
                () -> lastRevision.withAlteredProperties(Map.of("model", "next"), null));
        Assertions.assertSame(lastRevision, lastRevision.withAlteredProperties(Map.of(), null));
    }
}
