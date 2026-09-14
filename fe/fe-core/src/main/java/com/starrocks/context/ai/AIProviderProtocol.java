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

package com.starrocks.context.ai;

import com.google.common.base.Strings;

import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The HTTP request/response protocol an {@link AIProvider} endpoint speaks. Stored as the
 * {@code protocol} property of the provider (lowercase name), so it needs no persistence schema of
 * its own.
 *
 * <ul>
 *   <li>OPENAI — OpenAI-compatible: {@code /v1/embeddings} for embedding providers, Chat Completions
 *       for chat providers. The de-facto format most vendors (OpenAI, DeepSeek, Qwen, vLLM, Ollama,
 *       OpenRouter, ...) expose.</li>
 *   <li>ANTHROPIC — Anthropic Messages API. Chat providers only.</li>
 *   <li>COHERE — Cohere-compatible {@code /rerank} (Cohere, Jina, Voyage, OpenRouter, local TEI).
 *       Rerank providers only.</li>
 * </ul>
 *
 * <p>Every provider has a protocol: when the user omits it, {@link #defaultFor(AIProviderType)} picks
 * the protocol the type implied before this property existed, and {@code AIProviderMgr} writes it into
 * the params so SHOW/DESC never show an empty value.
 */
public enum AIProviderProtocol {
    OPENAI,
    ANTHROPIC,
    COHERE;

    public static AIProviderProtocol fromString(String s) {
        if (Strings.isNullOrEmpty(s)) {
            throw new IllegalArgumentException("AI provider protocol cannot be empty");
        }
        try {
            return valueOf(s.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "invalid AI provider protocol '" + s + "'; expected one of openai, anthropic, cohere");
        }
    }

    /** Lowercase wire/DDL form (e.g. "openai"). */
    public String lower() {
        return name().toLowerCase(Locale.ROOT);
    }

    /** The protocol a provider of the given type uses when none is specified. */
    public static AIProviderProtocol defaultFor(AIProviderType type) {
        switch (type) {
            case RERANK:
                return COHERE;
            case EMBEDDING:
            case CHAT:
            default:
                return OPENAI;
        }
    }

    /** The protocols a provider of the given type may declare. */
    public static Set<AIProviderProtocol> allowedFor(AIProviderType type) {
        switch (type) {
            case RERANK:
                return EnumSet.of(COHERE);
            case CHAT:
                return EnumSet.of(OPENAI, ANTHROPIC);
            case EMBEDDING:
            default:
                return EnumSet.of(OPENAI);
        }
    }

    /** @throws IllegalArgumentException if {@code protocol} is not valid for {@code type}. */
    public static void checkAllowed(AIProviderType type, AIProviderProtocol protocol) {
        Set<AIProviderProtocol> allowed = allowedFor(type);
        if (!allowed.contains(protocol)) {
            throw new IllegalArgumentException(String.format(
                    "protocol '%s' is not supported for AI provider type %s; allowed: %s",
                    protocol.lower(), type.lower(),
                    allowed.stream().map(AIProviderProtocol::lower).collect(Collectors.toList())));
        }
    }
}
