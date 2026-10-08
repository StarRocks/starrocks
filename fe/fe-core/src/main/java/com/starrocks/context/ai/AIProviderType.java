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
 * The kind of external AI service an {@link AIProvider} talks to. A single {@code AIProviderMgr}
 * registry holds providers of every type and keeps one default per type, so embedding, rerank and
 * chat providers share the same DDL, persistence and credential handling instead of each duplicating
 * the machinery.
 *
 * <ul>
 *   <li>EMBEDDING — text in, vector out.</li>
 *   <li>RERANK — query + documents in, relevance scores out.</li>
 *   <li>CHAT — chat/completion endpoint: any input modality the model accepts (text, images, documents)
 *       in, text out. This is the provider kind AI functions consume.</li>
 * </ul>
 *
 * <p>The wire protocol each type speaks is carried separately by {@link AIProviderProtocol}.
 *
 * <p>EMBEDDING is the original type — providers persisted before the unification carry no type tag,
 * so deserialization MUST treat a missing type as EMBEDDING (see {@code AIProviderMgr.gsonPostProcess}).
 */
public enum AIProviderType {
    EMBEDDING,
    RERANK,
    CHAT;

    public static AIProviderType fromString(String s) {
        if (Strings.isNullOrEmpty(s)) {
            throw new IllegalArgumentException("AI provider type is required");
        }
        try {
            return valueOf(s.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "invalid AI provider type '" + s + "'; expected one of embedding, rerank, chat");
        }
    }

    /** Lowercase wire/DDL form (e.g. "embedding"). */
    public String lower() {
        return name().toLowerCase(Locale.ROOT);
    }
}
