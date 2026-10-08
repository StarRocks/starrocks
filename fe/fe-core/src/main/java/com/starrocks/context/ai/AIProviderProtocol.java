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

import java.util.Locale;

/**
 * The HTTP request/response protocol an {@link AIProvider} endpoint speaks. Stored as the
 * {@code protocol} property of the provider (lowercase name), so it needs no persistence schema of
 * its own.
 *
 * <ul>
 *   <li>OPENAI — OpenAI-compatible: {@code /v1/embeddings} for embedding providers, Chat Completions
 *       for chat providers. The de-facto format most vendors (OpenAI, DeepSeek, Qwen, vLLM, Ollama,
 *       OpenRouter, ...) expose.</li>
 *   <li>ANTHROPIC — Anthropic Messages API.</li>
 *   <li>COHERE — Cohere-compatible {@code /rerank} (Cohere, Jina, Voyage, OpenRouter, local TEI).</li>
 * </ul>
 *
 * <p>Any protocol may be declared on any provider type; the type only decides the default. Every
 * provider has a protocol: when CREATE omits it, the analyzer fills in {@link #defaultFor(AIProviderType)},
 * the protocol the type implied before this property existed, so SHOW/DESC never show an empty value.
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
}
