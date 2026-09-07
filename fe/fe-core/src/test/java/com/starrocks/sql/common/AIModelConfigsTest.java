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

import com.starrocks.common.Config;
import com.starrocks.sql.common.AIModelConfigs.DefaultModelRequirement;
import com.starrocks.thrift.TAIEndpointConfig;
import com.starrocks.thrift.TAIModelConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    }

    @Test
    public void testSystemChatConfigurationBoundaryContainsOnlyPublicEndpointMetadata() {
        AIModelConfigs.SystemChatConfig config = AIModelConfigs.systemChatSnapshot(REQUIRED);

        Assertions.assertEquals("https://models.example.test/v1/chat/completions", config.endpoint());
        Assertions.assertEquals("default-model", config.model());
        Assertions.assertEquals("openai_compatible", config.provider());
    }

    @Test
    public void testThriftConfigurationSchemaIsCredentialFree() {
        Assertions.assertEquals(Set.of("endpoint", "model", "provider"),
                Arrays.stream(TAIEndpointConfig._Fields.values())
                        .map(TAIEndpointConfig._Fields::getFieldName)
                        .collect(Collectors.toSet()));
        Assertions.assertEquals(Set.of("chat"),
                Arrays.stream(TAIModelConfiguration._Fields.values())
                        .map(TAIModelConfiguration._Fields::getFieldName)
                        .collect(Collectors.toSet()));

        Stream<String> fieldNames = Stream.concat(
                Arrays.stream(TAIEndpointConfig._Fields.values()).map(TAIEndpointConfig._Fields::getFieldName),
                Arrays.stream(TAIModelConfiguration._Fields.values())
                        .map(TAIModelConfiguration._Fields::getFieldName));
        fieldNames.map(String::toLowerCase).forEach(fieldName -> {
            Assertions.assertFalse(fieldName.contains("key"), fieldName);
            Assertions.assertFalse(fieldName.contains("secret"), fieldName);
            Assertions.assertFalse(fieldName.contains("credential"), fieldName);
            Assertions.assertFalse(fieldName.contains("token"), fieldName);
            Assertions.assertFalse(fieldName.contains("env"), fieldName);
            Assertions.assertFalse(fieldName.contains("environment"), fieldName);
        });
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
