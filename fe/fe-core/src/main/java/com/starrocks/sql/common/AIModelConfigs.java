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
import com.starrocks.catalog.AIModel;
import com.starrocks.catalog.Function;
import com.starrocks.common.Config;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.thrift.TAIEndpointConfig;
import com.starrocks.thrift.TAIModelConfiguration;
import com.starrocks.thrift.TAIModelSource;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Locale;

/** Maps SYSTEM settings and bound AI models to credential-free execution configurations. */
public final class AIModelConfigs {
    public static final String SYSTEM_CHAT_CONFIG_ID = "__system_chat__";
    public static final String SYSTEM_TEXT_EMBEDDING_CONFIG_ID = "__system_text_embedding__";
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

    public static int getAIModelArgument(Function function) {
        return descriptor(function).aiModelArgument();
    }

    private static AIFunctionDescriptor descriptor(Function function) {
        AIFunctionDescriptor descriptor = VectorizedBuiltinFunctions.getAIFunctionDescriptor(function.getFunctionId());
        Preconditions.checkState(descriptor != null, "AI function is missing generated descriptor metadata");
        return descriptor;
    }

    public static String aiModelName(Expr expression) {
        if (!expression.isConstant() || !expression.getType().isStringType()) {
            throw new SemanticException("AI function requires a constant AI model name", expression.getPos());
        }
        Expr folded = ExprUtils.analyzeAndCastFold(expression.clone());
        if (!(folded instanceof StringLiteral literal) || literal.getValue().isBlank()) {
            throw new SemanticException("AI function requires a nonblank constant AI model name", expression.getPos());
        }
        return literal.getValue();
    }

    public static String configId(Function function, List<Expr> arguments) {
        return configId(function, arguments, AIModelBindings.EMPTY);
    }

    public static String configId(Function function, List<Expr> arguments, AIModelBindings bindings) {
        if (function.getAiModelSource() == TAIModelSource.AI_MODEL) {
            return bindings.configurationId(aiModelName(arguments.get(getAIModelArgument(function))));
        }
        Preconditions.checkState(function.getAiModelSource() == TAIModelSource.SYSTEM, "Unsupported AI model source");
        return isTextEmbedding(function) ? SYSTEM_TEXT_EMBEDDING_CONFIG_ID : SYSTEM_CHAT_CONFIG_ID;
    }

    public static ModelConfig fromSystemChat(SystemChatConfig config) {
        return new ModelConfig("CHAT", config.endpoint(), config.model(), config.provider(), null);
    }

    public static ModelConfig systemEmbeddingSnapshot(DefaultModelRequirement requirement) {
        ModelConfig config = new ModelConfig("TEXT_EMBEDDING", Config.ai_default_embedding_endpoint,
                Config.ai_default_embedding_model, Config.ai_default_embedding_provider, null);
        validateSystemEmbedding(config, requirement);
        return config;
    }

    public static void validateSystemEmbedding(ModelConfig config, DefaultModelRequirement requirement) {
        validateSystem(config, requirement);
    }

    public static ModelConfig fromModel(AIModel model) {
        return new ModelConfig(model.getCapability().name(), model.getEndpoint(), model.getRemoteModel(),
                model.getProvider().getSqlName(), model.getCredentialRef(), TAIModelSource.AI_MODEL, model.getId());
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
        if (containsControlCharacter(endpoint)) {
            throw invalidConfig(configName, "must be a valid complete HTTPS URL");
        }

        final URI uri;
        try {
            uri = new URI(endpoint);
        } catch (URISyntaxException e) {
            throw invalidConfig(configName, "must be a valid complete HTTPS URL");
        }

        String scheme = uri.getScheme();
        boolean httpsScheme = scheme != null && "https".equals(scheme.toLowerCase(Locale.ROOT));
        int port = uri.getPort();
        String rawAuthority = uri.getRawAuthority();
        if (!httpsScheme || uri.getHost() == null || uri.getHost().isEmpty()
                || (port != -1 && (port < 1 || port > 65535))
                || (rawAuthority != null && rawAuthority.endsWith(":"))
                || uri.getRawUserInfo() != null || uri.getRawQuery() != null || uri.getRawFragment() != null) {
            throw invalidConfig(configName,
                    "must be a complete HTTPS URL without userinfo, query or fragment");
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

    /** Immutable planning snapshot; the credential reference is a name, never an API key. */
    public record ModelConfig(String capability, String endpoint, String model, String provider, String credentialRef,
                              TAIModelSource source, long modelId) {
        public ModelConfig(String capability, String endpoint, String model, String provider, String credentialRef) {
            this(capability, endpoint, model, provider, credentialRef, TAIModelSource.SYSTEM, 0);
        }

        public ModelConfig {
            model = model == null ? "" : model;
        }

        public TAIModelConfiguration toThrift() {
            TAIEndpointConfig endpointConfig = new TAIEndpointConfig();
            endpointConfig.setEndpoint(endpoint);
            endpointConfig.setModel(model);
            endpointConfig.setProvider(provider);
            if (credentialRef != null) {
                endpointConfig.setCredential_ref(credentialRef);
            }
            TAIModelConfiguration result = new TAIModelConfiguration();
            if (source == TAIModelSource.AI_MODEL) {
                result.setSource(source);
                result.setModel_id(modelId);
            }
            if ("TEXT_EMBEDDING".equals(capability)) {
                result.setText_embedding(endpointConfig);
            } else {
                result.setChat(endpointConfig);
            }
            return result;
        }
    }
}
