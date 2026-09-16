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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;

/** Provides validated, credential-free configuration snapshots for SYSTEM AI calls. */
public final class AIModelConfigs {
    public static final String SYSTEM_CHAT_CONFIG_ID = "__system_chat__";
    public static final String OPENAI_COMPATIBLE_PROVIDER = "openai_compatible";

    private AIModelConfigs() {
    }

    public enum DefaultModelRequirement {
        REQUIRED,
        OPTIONAL
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
        String endpoint = requireNonBlank(config.endpoint(), "ai_default_chat_endpoint");
        validateEndpoint(endpoint);

        String provider = requireNonBlank(config.provider(), "ai_default_chat_provider");
        if (!OPENAI_COMPATIBLE_PROVIDER.equals(provider)) {
            throw invalidConfig("ai_default_chat_provider",
                    "must be '" + OPENAI_COMPATIBLE_PROVIDER + "'");
        }

        String model = config.model();
        if (containsControlCharacter(model)) {
            throw invalidConfig("ai_default_chat_model", "must not contain control characters");
        }
        if (defaultModelRequirement == DefaultModelRequirement.REQUIRED && model.trim().isEmpty()) {
            throw invalidConfig("ai_default_chat_model", "must be set for prompt-only ai_complete calls");
        }
    }

    private static String requireNonBlank(String value, String configName) {
        if (value == null || value.trim().isEmpty()) {
            throw invalidConfig(configName, "must be set for SYSTEM ai_complete calls");
        }
        return value;
    }

    private static void validateEndpoint(String endpoint) {
        if (containsControlCharacter(endpoint)) {
            throw invalidConfig("ai_default_chat_endpoint", "must be a valid complete HTTPS URL");
        }

        final URI uri;
        try {
            uri = new URI(endpoint);
        } catch (URISyntaxException e) {
            throw invalidConfig("ai_default_chat_endpoint", "must be a valid complete HTTPS URL");
        }

        String scheme = uri.getScheme();
        boolean httpsScheme = scheme != null && "https".equals(scheme.toLowerCase(Locale.ROOT));
        int port = uri.getPort();
        String rawAuthority = uri.getRawAuthority();
        if (!httpsScheme || uri.getHost() == null || uri.getHost().isEmpty()
                || (port != -1 && (port < 1 || port > 65535))
                || (rawAuthority != null && rawAuthority.endsWith(":"))
                || uri.getRawUserInfo() != null || uri.getRawFragment() != null) {
            throw invalidConfig("ai_default_chat_endpoint",
                    "must be a complete HTTPS URL without userinfo or a fragment");
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
}
