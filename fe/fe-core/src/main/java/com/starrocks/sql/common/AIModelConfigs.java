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

import com.google.common.base.Preconditions;
import com.starrocks.builtins.VectorizedBuiltinFunctions;
import com.starrocks.builtins.VectorizedBuiltinFunctions.AICapability;
import com.starrocks.builtins.VectorizedBuiltinFunctions.AIFunctionDescriptor;
import com.starrocks.catalog.Function;
import com.starrocks.common.Config;
import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.AIProviderProtocol;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.thrift.TAIEndpointConfig;
import com.starrocks.thrift.TAIModelConfiguration;
import com.starrocks.thrift.TAIModelSource;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;

/** Maps SYSTEM settings and provider metadata to immutable execution configurations. */
public final class AIModelConfigs {
    public static final String SYSTEM_CHAT_CONFIG_ID = "__system_chat__";
    public static final String SYSTEM_EMBEDDING_CONFIG_ID = "__system_embedding__";
    public static final String OPENAI_COMPATIBLE_PROVIDER = "openai_compatible";

    private AIModelConfigs() {
    }

    public enum DefaultModelRequirement {
        REQUIRED,
        OPTIONAL
    }

    public static boolean isTextEmbedding(Function function) {
        return descriptor(function).capability() == AICapability.TEXT_EMBEDDING;
    }

    public static boolean hasExplicitModel(Function function) {
        return getModelArgument(function) >= 0;
    }

    public static int getModelArgument(Function function) {
        return descriptor(function).modelArgument();
    }

    public static int getProviderArgument(Function function) {
        return descriptor(function).providerArgument();
    }

    private static AIFunctionDescriptor descriptor(Function function) {
        AIFunctionDescriptor descriptor = VectorizedBuiltinFunctions.getAIFunctionDescriptor(function.getFunctionId());
        Preconditions.checkState(descriptor != null, "AI function is missing generated descriptor metadata");
        return descriptor;
    }

    public static String providerName(Expr expression) {
        if (!expression.isConstant() || !expression.getType().isStringType()) {
            throw new SemanticException("AI function requires a constant AI provider name", expression.getPos());
        }
        Expr folded = ExprUtils.analyzeAndCastFold(expression.clone());
        if (!(folded instanceof StringLiteral literal) || literal.getValue().isBlank()) {
            throw new SemanticException("AI function requires a nonblank constant AI provider name", expression.getPos());
        }
        return literal.getValue();
    }

    public static String systemConfigId(Function function) {
        Preconditions.checkState(function.getAiModelSource() == TAIModelSource.SYSTEM, "Unsupported AI model source");
        return isTextEmbedding(function) ? SYSTEM_EMBEDDING_CONFIG_ID : SYSTEM_CHAT_CONFIG_ID;
    }

    public static ModelConfig fromSystemChat(SystemChatConfig config) {
        return new ModelConfig("CHAT", config.endpoint(), config.model(), config.provider());
    }

    public static ModelConfig systemEmbeddingSnapshot(DefaultModelRequirement requirement) {
        ModelConfig config = new ModelConfig("TEXT_EMBEDDING", Config.ai_default_embedding_endpoint,
                Config.ai_default_embedding_model, Config.ai_default_embedding_provider);
        validateSystemEmbedding(config, requirement);
        return config;
    }

    public static void validateSystemEmbedding(ModelConfig config, DefaultModelRequirement requirement) {
        validateSystem(config, requirement);
    }

    public static ModelConfig fromProvider(AIProvider provider) {
        AIProviderType type = provider.getType();
        if (type != AIProviderType.CHAT && type != AIProviderType.EMBEDDING) {
            throw new SemanticException("AI functions require a CHAT or EMBEDDING provider, not " + type);
        }
        AIProviderProtocol protocol = provider.getProtocol();
        if (protocol != AIProviderProtocol.OPENAI) {
            throw new SemanticException("AI functions require an OPENAI provider protocol, not " + protocol);
        }
        String endpoint = provider.getEndpoint();
        String model = provider.getModel();
        String apiKey = provider.getApiKey();
        if (endpoint == null || endpoint.isBlank() || model == null || model.isBlank()
                || containsControlCharacter(model)) {
            throw new SemanticException("AI provider requires a valid endpoint and model");
        }
        if (apiKey != null && containsControlCharacter(apiKey)) {
            throw new SemanticException("AI provider api_key must not contain control characters");
        }
        validateEndpoint(endpoint, "AI provider endpoint", apiKey == null || apiKey.isEmpty());
        final Integer timeout;
        final Integer dimensions;
        try {
            timeout = provider.getTimeoutMs();
            dimensions = type == AIProviderType.EMBEDDING ? provider.getDimensions() : null;
        } catch (NumberFormatException e) {
            throw new SemanticException("AI provider timeout_ms and dimensions must be positive integers");
        }
        if ((timeout != null && timeout <= 0) || (dimensions != null && dimensions <= 0)) {
            throw new SemanticException("AI provider timeout_ms and dimensions must be positive integers");
        }
        return new ModelConfig(type == AIProviderType.CHAT ? "CHAT" : "TEXT_EMBEDDING", endpoint, model,
                OPENAI_COMPATIBLE_PROVIDER, apiKey, TAIModelSource.PROVIDER, timeout, dimensions);
    }

    public static void validateSystemChat(DefaultModelRequirement defaultModelRequirement) {
        systemChatSnapshot(defaultModelRequirement);
    }

    public static SystemChatConfig systemChatSnapshot(DefaultModelRequirement defaultModelRequirement) {
        String endpoint = Config.ai_default_chat_endpoint;
        String model = Config.ai_default_chat_model;
        String provider = Config.ai_default_chat_provider;

        SystemChatConfig config = new SystemChatConfig(endpoint, model, provider);
        validateSystemChat(config, defaultModelRequirement);
        return config;
    }

    public static void validateSystemChat(SystemChatConfig config,
                                          DefaultModelRequirement defaultModelRequirement) {
        validateSystem(fromSystemChat(config), defaultModelRequirement);
    }

    private static void validateSystem(ModelConfig config, DefaultModelRequirement defaultModelRequirement) {
        boolean embedding = "TEXT_EMBEDDING".equals(config.capability());
        String prefix = embedding ? "ai_default_embedding_" : "ai_default_chat_";
        String functionName = embedding ? "ai_embed" : "ai_complete";
        String endpoint = requireNonBlank(config.endpoint(), prefix + "endpoint", functionName);
        validateEndpoint(endpoint, prefix + "endpoint");

        String provider = requireNonBlank(config.provider(), prefix + "provider", functionName);
        if (!OPENAI_COMPATIBLE_PROVIDER.equals(provider)) {
            throw invalidConfig(prefix + "provider",
                    "must be '" + OPENAI_COMPATIBLE_PROVIDER + "'");
        }

        String model = config.model();
        if (containsControlCharacter(model)) {
            throw invalidConfig(prefix + "model", "must not contain control characters");
        }
        if (defaultModelRequirement == DefaultModelRequirement.REQUIRED && model.trim().isEmpty()) {
            throw invalidConfig(prefix + "model",
                    "must be set for " + (embedding ? "text-only " : "prompt-only ") + functionName + " calls");
        }
    }

    private static String requireNonBlank(String value, String configName, String functionName) {
        if (value == null || value.trim().isEmpty()) {
            throw invalidConfig(configName, "must be set for SYSTEM " + functionName + " calls");
        }
        return value;
    }

    private static void validateEndpoint(String endpoint, String configName) {
        validateEndpoint(endpoint, configName, false);
    }

    private static void validateEndpoint(String endpoint, String configName, boolean allowHttp) {
        String schemes = allowHttp ? "HTTP(S)" : "HTTPS";
        if (containsControlCharacter(endpoint)) {
            throw invalidConfig(configName, "must be a valid complete " + schemes + " URL");
        }

        final URI uri;
        try {
            uri = new URI(endpoint);
        } catch (URISyntaxException e) {
            throw invalidConfig(configName, "must be a valid complete " + schemes + " URL");
        }

        String scheme = uri.getScheme();
        boolean supportedScheme = scheme != null && ("https".equals(scheme.toLowerCase(Locale.ROOT))
                || (allowHttp && "http".equals(scheme.toLowerCase(Locale.ROOT))));
        int port = uri.getPort();
        String rawAuthority = uri.getRawAuthority();
        if (!supportedScheme || uri.getHost() == null || uri.getHost().isEmpty()
                || (port != -1 && (port < 1 || port > 65535))
                || (rawAuthority != null && rawAuthority.endsWith(":"))
                || uri.getRawUserInfo() != null || uri.getRawQuery() != null || uri.getRawFragment() != null) {
            throw invalidConfig(configName,
                    "must be a complete " + schemes + " URL without userinfo, query or fragment");
        }
    }

    private static boolean containsControlCharacter(String value) {
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch <= 0x1f || ch == 0x7f) {
                return true;
            }
        }
        return false;
    }

    private static StarRocksPlannerException invalidConfig(String configName, String requirement) {
        return new StarRocksPlannerException(
                "FE config '" + configName + "' " + requirement, ErrorType.USER_ERROR);
    }

    public record SystemChatConfig(String endpoint, String model, String provider) {
        public SystemChatConfig {
            model = model == null ? "" : model;
        }
    }

    /** Immutable execution configuration. Credentials are excluded from diagnostic rendering. */
    public record ModelConfig(String capability, String endpoint, String model, String provider, String apiKey,
                              TAIModelSource source, Integer timeoutMs, Integer dimensions) {
        public ModelConfig(String capability, String endpoint, String model, String provider) {
            this(capability, endpoint, model, provider, null, TAIModelSource.SYSTEM, null, null);
        }

        public ModelConfig {
            model = model == null ? "" : model;
        }

        @Override
        public String toString() {
            return "ModelConfig[capability=" + capability + ", source=" + source + "]";
        }

        public TAIModelConfiguration toThrift() {
            TAIEndpointConfig endpointConfig = new TAIEndpointConfig();
            endpointConfig.setEndpoint(endpoint);
            endpointConfig.setModel(model);
            endpointConfig.setProvider(provider);
            if (apiKey != null) {
                endpointConfig.setApi_key(apiKey);
            }
            if (timeoutMs != null) {
                endpointConfig.setTimeout_ms(timeoutMs);
            }
            if (dimensions != null) {
                endpointConfig.setDimensions(dimensions);
            }
            TAIModelConfiguration result = new TAIModelConfiguration();
            if (source == TAIModelSource.PROVIDER) {
                result.setSource(source);
            }
            if ("TEXT_EMBEDDING".equals(capability)) {
                result.setEmbedding(endpointConfig);
            } else {
                result.setChat(endpointConfig);
            }
            return result;
        }
    }
}
