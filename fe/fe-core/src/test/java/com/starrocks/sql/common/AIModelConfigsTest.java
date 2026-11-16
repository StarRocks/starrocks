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

package com.starrocks.sql.common;

import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.AIProviderProtocol;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.thrift.TAIProvider;
import com.starrocks.thrift.TAIProviderProtocol;
import com.starrocks.thrift.TAIProviderType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class AIModelConfigsTest {
    @Test
    public void testProviderTypeMappingAndProtocolNormalization() {
        Map<AIProviderType, TAIProviderType> types = Map.of(
                AIProviderType.CHAT, TAIProviderType.CHAT,
                AIProviderType.EMBEDDING, TAIProviderType.EMBEDDING,
                AIProviderType.RERANK, TAIProviderType.RERANK);
        types.forEach((type, wireType) -> {
            AIProvider provider = provider(type);
            provider.mergeParams(Map.of("protocol", "OpEnAi"));
            TAIProvider wire = provider.toThrift();
            Assertions.assertEquals(wireType, wire.getType());
            Assertions.assertEquals(provider.getId(), wire.getId());
            Assertions.assertEquals(provider.getName(), wire.getName());
            Assertions.assertEquals(TAIProviderProtocol.OPENAI, wire.getProtocol());
            Assertions.assertEquals(provider.getParams().get("protocol"), wire.getParams().get("protocol"));
            Assertions.assertEquals("OpEnAi", provider.getParams().get("protocol"));
        });
        Assertions.assertEquals(TAIProviderProtocol.OPENAI, provider(AIProviderType.CHAT).toThrift().getProtocol());
        Assertions.assertEquals(TAIProviderProtocol.OPENAI, provider(AIProviderType.EMBEDDING).toThrift().getProtocol());
        Assertions.assertEquals(TAIProviderProtocol.COHERE, provider(AIProviderType.RERANK).toThrift().getProtocol());
    }

    @Test
    public void testAllProtocolMappingsPreserveOriginalParams() {
        Map<AIProviderProtocol, TAIProviderProtocol> protocols = Map.of(
                AIProviderProtocol.OPENAI, TAIProviderProtocol.OPENAI,
                AIProviderProtocol.ANTHROPIC, TAIProviderProtocol.ANTHROPIC,
                AIProviderProtocol.COHERE, TAIProviderProtocol.COHERE);
        protocols.forEach((protocol, wireProtocol) -> {
            AIProvider provider = provider(AIProviderType.CHAT);
            provider.mergeParams(Map.of("protocol", protocol.lower()));
            Map<String, String> originalParams = provider.getParams();
            TAIProvider wire = provider.toThrift();
            Assertions.assertEquals(wireProtocol, wire.getProtocol());
            Assertions.assertEquals(provider.getParams().get("protocol"), wire.getParams().get("protocol"));
            Assertions.assertEquals(originalParams, provider.getParams());
        });
    }

    @Test
    public void testWirePropertiesAreIndependentCopies() {
        AIProvider provider = provider(AIProviderType.EMBEDDING);
        provider.mergeParams(Map.of("api_key", "test-key", "timeout_ms", "123", "dimensions", "256"));
        TAIProvider wire = provider.toThrift();
        provider.mergeParams(Map.of("model", "changed-model", "api_key", "changed-key"));
        Assertions.assertEquals("provider-model", wire.getParams().get("model"));
        Assertions.assertEquals("test-key", wire.getParams().get("api_key"));
        Assertions.assertEquals("123", wire.getParams().get("timeout_ms"));
        Assertions.assertEquals("256", wire.getParams().get("dimensions"));
        wire.getParams().put("model", "wire-model");
        Assertions.assertEquals("changed-model", provider.getModel());
        Assertions.assertFalse(provider.getMaskedParams().toString().contains("changed-key"));
    }

    @Test
    public void testOptionalPropertiesRemainAbsent() {
        TAIProvider wire = provider(AIProviderType.EMBEDDING).toThrift();
        Assertions.assertFalse(wire.getParams().containsKey("api_key"));
        Assertions.assertFalse(wire.getParams().containsKey("dimensions"));
        Assertions.assertFalse(wire.getParams().containsKey("timeout_ms"));
    }

    @Test
    public void testUnsupportedTypesAndProtocolsFailBeforeExecution() {
        Assertions.assertThrows(SemanticException.class,
                () -> AIModelConfigs.validateProvider(provider(AIProviderType.RERANK)));
        for (AIProviderType type : new AIProviderType[] {AIProviderType.CHAT, AIProviderType.EMBEDDING}) {
            AIProvider provider = provider(type);
            for (String protocol : new String[] {"anthropic", "cohere"}) {
                provider.mergeParams(Map.of("protocol", protocol, "api_key", "secret-test-key"));
                SemanticException error = Assertions.assertThrows(SemanticException.class,
                        () -> AIModelConfigs.validateProvider(provider));
                Assertions.assertTrue(error.getMessage().contains("OPENAI"));
                Assertions.assertFalse(error.getMessage().contains("secret-test-key"));
            }
        }
    }

    @Test
    public void testPositiveIntegerValidationAndEmptyDefaults() {
        for (String key : new String[] {"timeout_ms", "dimensions"}) {
            for (String value : new String[] {"0", "-1", "2147483648", "12junk"}) {
                AIProvider provider = provider(AIProviderType.EMBEDDING);
                provider.mergeParams(Map.of(key, value));
                Assertions.assertThrows(SemanticException.class, () -> AIModelConfigs.validateProvider(provider));
            }
            for (String value : new String[] {"", "1", "+123", "2147483647"}) {
                AIProvider provider = provider(AIProviderType.EMBEDDING);
                provider.mergeParams(Map.of(key, value));
                Assertions.assertDoesNotThrow(() -> AIModelConfigs.validateProvider(provider));
            }
        }
    }

    @Test
    public void testHttpAndHttpsWithOptionalKey() {
        for (String scheme : new String[] {"http", "https"}) {
            AIProvider provider = provider(AIProviderType.CHAT);
            provider.mergeParams(Map.of("endpoint", scheme + "://models.example.test/v1/inference"));
            Assertions.assertDoesNotThrow(() -> AIModelConfigs.validateProvider(provider));
            provider.mergeParams(Map.of("api_key", "test-key"));
            Assertions.assertDoesNotThrow(() -> AIModelConfigs.validateProvider(provider));
        }
    }

    private static AIProvider provider(AIProviderType type) {
        return new AIProvider("test-provider-id", "test-provider", type, new HashMap<>(Map.of(
                "endpoint", "https://models.example.test/v1/inference", "model", "provider-model")), "");
    }
}
