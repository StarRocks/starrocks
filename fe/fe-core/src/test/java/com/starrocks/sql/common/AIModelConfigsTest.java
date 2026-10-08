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

import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.Config;
import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.AIModelConfigs.DefaultModelRequirement;
import com.starrocks.thrift.TAIEndpointConfig;
import com.starrocks.thrift.TAIModelConfiguration;
import com.starrocks.thrift.TAIModelSource;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.AIModelConfigs.DefaultModelRequirement.OPTIONAL;
import static com.starrocks.sql.common.AIModelConfigs.DefaultModelRequirement.REQUIRED;

public class AIModelConfigsTest {
    @BeforeEach
    public void setUpSystemChat() {
        Config.ai_default_chat_endpoint = "https://models.example.test/v1/chat/completions";
        Config.ai_default_chat_model = "default-model";
        Config.ai_default_chat_provider = "openai_compatible";
    }

    @AfterEach
    public void clearSystemChat() {
        Config.ai_default_chat_endpoint = "";
        Config.ai_default_chat_model = "";
        Config.ai_default_chat_provider = "";
        Config.ai_default_embedding_endpoint = "";
        Config.ai_default_embedding_model = "";
        Config.ai_default_embedding_provider = "";
    }

    @Test
    public void testResolvedAIOverloadsIdentifyOnlyExplicitModelArguments() {
        FunctionSet functions = new FunctionSet();
        functions.init();
        Set<Long> explicitModelIds = Set.of(200102L, 200103L, 200111L, 200113L, 200115L, 200117L,
                200119L, 200121L, 200123L, 200125L, 200127L, 200132L, 200133L);
        Set<Long> observedExplicitModelIds = functions.getBuiltinFunctions().stream()
                .filter(Function::isAi)
                .filter(AIModelConfigs::hasExplicitModel)
                .map(Function::getFunctionId)
                .collect(Collectors.toSet());
        Assertions.assertEquals(explicitModelIds, observedExplicitModelIds,
                "Provider selectors and default-model arguments must not be classified as explicit remote models");
        Set<Long> namedModelIds = Set.of(200140L, 200141L, 200142L, 200143L);
        functions.getBuiltinFunctions().stream().filter(Function::isAi).forEach(function -> {
            long id = function.getFunctionId();
            Assertions.assertEquals(explicitModelIds.contains(id) ? 0 : -1, AIModelConfigs.getModelArgument(function));
            Assertions.assertEquals(namedModelIds.contains(id) ? 0 : -1, AIModelConfigs.getProviderArgument(function));
            int providerArgument = AIModelConfigs.getProviderArgument(function);
            Assertions.assertEquals(function.getAiModelSource() == TAIModelSource.PROVIDER, providerArgument >= 0);
            if (providerArgument >= 0) {
                Assertions.assertTrue(providerArgument < function.getNumArgs());
                Assertions.assertTrue(function.getArgs()[providerArgument].isVarchar());
            }
        });
    }

    @Test
    public void testUnregisteredFunctionHasNoModelArgumentMetadata() {
        Function function = new Function(new FunctionName("ai_embed"), new Type[] {VarcharType.VARCHAR},
                VarcharType.VARCHAR, false);
        Assertions.assertThrows(IllegalStateException.class, () -> AIModelConfigs.hasExplicitModel(function));
        Assertions.assertThrows(IllegalStateException.class, () -> AIModelConfigs.isTextEmbedding(function));
    }

    @Test
    public void testResolvedAIOverloadsUseGeneratedCapabilities() {
        FunctionSet functions = new FunctionSet();
        functions.init();
        Set<Long> embeddingIds = functions.getBuiltinFunctions().stream()
                .filter(Function::isAi)
                .filter(AIModelConfigs::isTextEmbedding)
                .map(Function::getFunctionId)
                .collect(Collectors.toSet());
        Assertions.assertEquals(Set.of(200130L, 200131L, 200132L, 200133L, 200142L, 200143L), embeddingIds);
    }

    @ParameterizedTest
    @CsvSource({"CHAT,", "CHAT, openai", "CHAT, OpEnAi",
            "EMBEDDING,", "EMBEDDING, openai", "EMBEDDING, OpEnAi"})
    public void testOpenAIProviderProtocolsUseExistingWireAdapter(AIProviderType type, String protocol) {
        Map<String, String> properties = new HashMap<>(Map.of(
                "endpoint", "https://models.example.test/v1/inference", "model", "provider-model"));
        if (protocol != null) {
            properties.put("protocol", protocol);
        }
        AIProvider provider = new AIProvider(UUID.randomUUID().toString(), "openai_provider", type, properties, "");

        AIModelConfigs.ModelConfig config = AIModelConfigs.fromProvider(provider);
        TAIModelConfiguration thrift = config.toThrift();
        boolean embedding = type == AIProviderType.EMBEDDING;
        Assertions.assertEquals(embedding ? "TEXT_EMBEDDING" : "CHAT", config.capability());
        Assertions.assertEquals(embedding, thrift.isSetEmbedding());
        Assertions.assertEquals(!embedding, thrift.isSetChat());
        Assertions.assertEquals("openai_compatible", config.provider());
        Assertions.assertEquals("openai_compatible",
                (embedding ? thrift.getEmbedding() : thrift.getChat()).getProvider());
    }

    @ParameterizedTest
    @CsvSource({"CHAT, ANTHROPIC", "CHAT, COHERE", "EMBEDDING, ANTHROPIC", "EMBEDDING, COHERE"})
    public void testUnsupportedProviderProtocolsFailBeforeExecution(AIProviderType type, String protocol) {
        AIProvider provider = new AIProvider(UUID.randomUUID().toString(), "unsupported_provider", type,
                Map.of("endpoint", "https://sensitive-endpoint.example.test/v1/inference", "model", "sensitive-model",
                        "api_key", "sensitive-test-key", "protocol", protocol), "");

        SemanticException error = Assertions.assertThrows(SemanticException.class,
                () -> AIModelConfigs.fromProvider(provider));
        Assertions.assertEquals("AI functions require an OPENAI provider protocol, not " + protocol, error.getDetailMsg());
    }

    @Test
    public void testProviderConfigurationIsAnImmutableSnapshot() {
        AIProvider original = new AIProvider(UUID.randomUUID().toString(), "snapshot_provider", AIProviderType.CHAT,
                Map.of("endpoint", "https://before.example.test/v1/chat/completions",
                        "model", "before-model", "api_key", "snapshot-test-key", "timeout_ms", "123"), "");
        AIModelConfigs.ModelConfig captured = AIModelConfigs.fromProvider(original);
        original.mergeParams(Map.of("model", "after-model", "api_key", "changed-test-key", "timeout_ms", "456"));
        Assertions.assertEquals("before-model", captured.model());
        Assertions.assertEquals("after-model", AIModelConfigs.fromProvider(original).model());
        Assertions.assertEquals(TAIModelSource.PROVIDER, captured.source());
        Assertions.assertEquals("snapshot-test-key", captured.toThrift().getChat().getApi_key());
        Assertions.assertEquals(123, captured.toThrift().getChat().getTimeout_ms());
        Assertions.assertEquals(TAIModelSource.PROVIDER, captured.toThrift().getSource());
        Assertions.assertFalse(captured.toThrift().toString().contains(original.getId()));
        Assertions.assertFalse(captured.toString().contains("snapshot-test-key"));
        Assertions.assertFalse(AIModelConfigs.fromSystemChat(AIModelConfigs.systemChatSnapshot(REQUIRED))
                .toThrift().isSetSource());
    }

    @Test
    public void testSystemChatConfigurationBoundaryContainsOnlyPublicEndpointMetadata() {
        AIModelConfigs.SystemChatConfig config = AIModelConfigs.systemChatSnapshot(REQUIRED);

        Assertions.assertEquals("https://models.example.test/v1/chat/completions", config.endpoint());
        Assertions.assertEquals("default-model", config.model());
        Assertions.assertEquals("openai_compatible", config.provider());
    }

    @Test
    public void testKeylessEmbeddingProviderPreservesTypedDefaults() {
        AIProvider provider = new AIProvider(UUID.randomUUID().toString(), "keyless", AIProviderType.EMBEDDING,
                Map.of("endpoint", "http://models.example.test/v1/embeddings", "model", "embedding-model",
                        "dimensions", "256", "timeout_ms", "1000"), "");
        AIModelConfigs.ModelConfig config = AIModelConfigs.fromProvider(provider);
        TAIEndpointConfig endpoint = config.toThrift().getEmbedding();
        Assertions.assertEquals("TEXT_EMBEDDING", config.capability());
        Assertions.assertEquals(provider.getEndpoint(), endpoint.getEndpoint());
        Assertions.assertEquals(256, endpoint.getDimensions());
        Assertions.assertEquals(1000, endpoint.getTimeout_ms());
        Assertions.assertFalse(endpoint.isSetApi_key());
        provider.mergeParams(Map.of("api_key", "test-key"));
        Assertions.assertThrows(StarRocksPlannerException.class, () -> AIModelConfigs.fromProvider(provider));
    }

    @Test
    public void testProviderRejectsInvalidTransportConfigurationBeforeExecution() {
        AIProvider provider = new AIProvider(UUID.randomUUID().toString(), "invalid", AIProviderType.CHAT,
                Map.of("endpoint", "https://models.example.test/v1/chat/completions", "model", "chat-model"), "");
        for (String key : Set.of("timeout_ms", "api_key")) {
            AIProvider invalid = new AIProvider(provider);
            invalid.mergeParams(Map.of(key, key.equals("timeout_ms") ? "0" : "test\r\nInjected: header"));
            RuntimeException error = Assertions.assertThrows(RuntimeException.class,
                    () -> AIModelConfigs.fromProvider(invalid));
            Assertions.assertFalse(error.getMessage().contains("Injected"));
        }
        provider.setType(AIProviderType.RERANK);
        SemanticException error = Assertions.assertThrows(SemanticException.class, () -> AIModelConfigs.fromProvider(provider));
        Assertions.assertEquals("AI functions require a CHAT or EMBEDDING provider, not RERANK", error.getDetailMsg());
    }

    @Test
    public void testThriftConfigurationHasExecutionFieldsAndOrdinals() {
        Assertions.assertEquals(Set.of("endpoint", "model", "provider", "api_key", "timeout_ms", "dimensions"),
                Arrays.stream(TAIEndpointConfig._Fields.values())
                        .map(TAIEndpointConfig._Fields::getFieldName)
                        .collect(Collectors.toSet()));
        Assertions.assertEquals(Set.of("chat", "embedding", "source"),
                Arrays.stream(TAIModelConfiguration._Fields.values())
                        .map(TAIModelConfiguration._Fields::getFieldName)
                        .collect(Collectors.toSet()));

        Assertions.assertEquals(1, TAIEndpointConfig._Fields.ENDPOINT.getThriftFieldId());
        Assertions.assertEquals(2, TAIEndpointConfig._Fields.MODEL.getThriftFieldId());
        Assertions.assertEquals(3, TAIEndpointConfig._Fields.PROVIDER.getThriftFieldId());
        Assertions.assertEquals(4, TAIEndpointConfig._Fields.API_KEY.getThriftFieldId());
        Assertions.assertEquals(5, TAIEndpointConfig._Fields.TIMEOUT_MS.getThriftFieldId());
        Assertions.assertEquals(6, TAIEndpointConfig._Fields.DIMENSIONS.getThriftFieldId());
        Assertions.assertEquals(1, TAIModelConfiguration._Fields.CHAT.getThriftFieldId());
        Assertions.assertEquals(2, TAIModelConfiguration._Fields.EMBEDDING.getThriftFieldId());
        Assertions.assertEquals(3, TAIModelConfiguration._Fields.SOURCE.getThriftFieldId());
        Assertions.assertEquals(0, TAIModelSource.SYSTEM.getValue());
        Assertions.assertEquals(1, TAIModelSource.PROVIDER.getValue());
    }

    @Test
    public void testEmbeddingConfigurationNormalizesMissingDefaultAndRemainsIndependent() {
        Config.ai_default_embedding_endpoint = "https://models.example.test/v1/embeddings";
        Config.ai_default_embedding_model = null;
        Config.ai_default_embedding_provider = "openai_compatible";
        AIModelConfigs.ModelConfig snapshot = AIModelConfigs.systemEmbeddingSnapshot(OPTIONAL);
        Assertions.assertEquals("", snapshot.model());
        Assertions.assertNull(snapshot.apiKey());
        Assertions.assertThrows(StarRocksPlannerException.class, () -> AIModelConfigs.systemEmbeddingSnapshot(REQUIRED));
        Config.ai_default_embedding_endpoint = "";
        Assertions.assertDoesNotThrow(() -> AIModelConfigs.validateSystemEmbedding(snapshot, OPTIONAL));
        Assertions.assertThrows(StarRocksPlannerException.class, () -> AIModelConfigs.systemEmbeddingSnapshot(OPTIONAL));
        Assertions.assertDoesNotThrow(() -> AIModelConfigs.systemChatSnapshot(REQUIRED));
    }

    @Test
    public void testExplicitModelConfigurationMayOmitDefaultModel() {
        Config.ai_default_chat_model = "";
        AIModelConfigs.SystemChatConfig config = AIModelConfigs.systemChatSnapshot(OPTIONAL);
        Assertions.assertEquals("", config.model());
    }

    @Test
    public void testPromptOnlyConfigurationRequiresDefaultModel() {
        Config.ai_default_chat_model = "";
        StarRocksPlannerException exception = Assertions.assertThrows(StarRocksPlannerException.class,
                () -> AIModelConfigs.validateSystemChat(REQUIRED));
        Assertions.assertTrue(exception.getMessage().contains("ai_default_chat_model"));
    }

    @Test
    public void testCapturedSystemChatValidationDoesNotReadGlobals() {
        AIModelConfigs.SystemChatConfig capturedWithoutDefault = new AIModelConfigs.SystemChatConfig(
                "https://captured.example.test/v1/chat/completions", null, "openai_compatible");
        Assertions.assertEquals("", capturedWithoutDefault.model());
        Assertions.assertDoesNotThrow(() -> AIModelConfigs.validateSystemChat(capturedWithoutDefault, OPTIONAL));
        StarRocksPlannerException missingDefault = Assertions.assertThrows(StarRocksPlannerException.class,
                () -> AIModelConfigs.validateSystemChat(capturedWithoutDefault, REQUIRED));
        Assertions.assertTrue(missingDefault.getMessage().contains("ai_default_chat_model"));

        AIModelConfigs.SystemChatConfig captured = new AIModelConfigs.SystemChatConfig(
                "https://captured.example.test/v1/chat/completions", "captured-model", "openai_compatible");
        Config.ai_default_chat_endpoint = "";
        Config.ai_default_chat_model = "";
        Config.ai_default_chat_provider = "";
        Assertions.assertDoesNotThrow(() -> AIModelConfigs.validateSystemChat(captured, REQUIRED));
    }

    @Test
    public void testEndpointAcceptsDefaultAndExplicitValidPorts() {
        String[] validEndpoints = {
                "https://models.example.test/v1/chat/completions",
                "https://models.example.test:1/v1/chat/completions",
                "https://models.example.test:443/v1/chat/completions",
                "https://models.example.test:65535/v1/chat/completions"
        };
        for (String endpoint : validEndpoints) {
            Config.ai_default_chat_endpoint = endpoint;
            Assertions.assertEquals(endpoint, AIModelConfigs.systemChatSnapshot(REQUIRED).endpoint());
        }
    }

    @Test
    public void testEndpointRejectsExplicitPortZero() {
        Config.ai_default_chat_endpoint = "https://models.example.test:0/v1/chat/completions";

        StarRocksPlannerException exception = Assertions.assertThrows(StarRocksPlannerException.class,
                () -> AIModelConfigs.validateSystemChat(REQUIRED));
        Assertions.assertTrue(exception.getMessage().contains("ai_default_chat_endpoint"));
        Assertions.assertFalse(exception.getMessage().contains(Config.ai_default_chat_endpoint));
    }

    @Test
    public void testSystemEndpointsRejectQueryParametersWithoutLeakingValues() {
        Config.ai_default_chat_endpoint = "https://models.example.test/v1/chat/completions?api_key=secret-sentinel";
        Config.ai_default_embedding_endpoint = "https://models.example.test/v1/embeddings?key=secret-sentinel";
        Config.ai_default_embedding_model = "embedding-model";
        Config.ai_default_embedding_provider = "openai_compatible";
        Assertions.assertAll(
                () -> {
                    StarRocksPlannerException failure = Assertions.assertThrows(StarRocksPlannerException.class,
                            () -> AIModelConfigs.systemChatSnapshot(REQUIRED));
                    Assertions.assertFalse(failure.getMessage().contains("secret-sentinel"));
                    Assertions.assertTrue(failure.getMessage().contains("query"));
                },
                () -> {
                    StarRocksPlannerException failure = Assertions.assertThrows(StarRocksPlannerException.class,
                            () -> AIModelConfigs.systemEmbeddingSnapshot(REQUIRED));
                    Assertions.assertFalse(failure.getMessage().contains("secret-sentinel"));
                    Assertions.assertTrue(failure.getMessage().contains("query"));
                });
    }

    @Test
    public void testModelRejectsEveryControlCharacterBeforeRequirementPolicy() {
        for (DefaultModelRequirement requirement : DefaultModelRequirement.values()) {
            for (int codePoint = 0; codePoint <= 0x1f; codePoint++) {
                assertModelControlRejected(requirement, (char) codePoint);
            }
            assertModelControlRejected(requirement, (char) 0x7f);
        }
    }

    private static void assertModelControlRejected(DefaultModelRequirement requirement, char control) {
        String configuredModel = "sensitive" + control + "model";
        Config.ai_default_chat_model = configuredModel;

        StarRocksPlannerException exception = Assertions.assertThrows(StarRocksPlannerException.class,
                () -> AIModelConfigs.validateSystemChat(requirement),
                () -> "control U+" + String.format("%04X", (int) control) + " with " + requirement);
        Assertions.assertTrue(exception.getMessage().contains("ai_default_chat_model"));
        Assertions.assertTrue(exception.getMessage().contains("control character"));
        Assertions.assertFalse(exception.getMessage().contains(configuredModel));
    }

    @Test
    public void testEndpointPolicy() {
        String[] invalidEndpoints = {
                "models.example.test/v1/chat/completions",
                "http://models.example.test/chat",
                "ftp://models.example.test/chat",
                "https:///chat",
                "https://user:password@models.example.test/chat",
                "https://models.example.test/chat#fragment",
                "https://models.example.test:65536/chat",
                "https://models.example.test:/chat",
                "https://models.example.test/chat\r\nInjected: true"
        };
        for (String endpoint : invalidEndpoints) {
            Config.ai_default_chat_endpoint = endpoint;
            StarRocksPlannerException exception = Assertions.assertThrows(StarRocksPlannerException.class,
                    () -> AIModelConfigs.validateSystemChat(REQUIRED), endpoint);
            Assertions.assertTrue(exception.getMessage().contains("ai_default_chat_endpoint"));
            Assertions.assertFalse(exception.getMessage().contains(endpoint));
        }
    }

    @Test
    public void testOnlyOpenAICompatibleProviderIsAccepted() {
        Config.ai_default_chat_provider = "custom-provider-secret-name";
        StarRocksPlannerException exception = Assertions.assertThrows(StarRocksPlannerException.class,
                () -> AIModelConfigs.validateSystemChat(REQUIRED));
        Assertions.assertTrue(exception.getMessage().contains("openai_compatible"));
        Assertions.assertFalse(exception.getMessage().contains("custom-provider-secret-name"));
    }
}
