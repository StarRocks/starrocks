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

package com.starrocks.context.embedding;

import com.google.common.base.Strings;
import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.context.error.ContextErrorCode;
import com.starrocks.context.error.ContextException;
import com.starrocks.server.GlobalStateMgr;

/**
 * Builds the JSON config blob handed to the BE {@code embedding(text, config_json)} scalar
 * function. The blob is materialized from the current DEFAULT EMBEDDING PROVIDER, which is
 * the single source of truth for endpoint, model, dimensions, timeout and api_key — all
 * persisted on the FE metadata journal/image so cluster upgrades preserve the credential.
 */
public final class EmbeddingConfigJson {

    private EmbeddingConfigJson() {
    }

    /**
     * Same as {@link #build()} but raises {@link ContextErrorCode#VECTOR_NOT_READY} when no
     * DEFAULT EMBEDDING PROVIDER exists. Call this from every write entry point (REST + SQL
     * UPSERT) so a misconfigured cluster fails fast and uniformly.
     */
    public static String requireBuild() {
        String json = build();
        if (json == null) {
            throw new ContextException(ContextErrorCode.VECTOR_NOT_READY,
                    "CONTEXT UPSERT requires a DEFAULT EMBEDDING PROVIDER",
                    "Run: CREATE EMBEDDING PROVIDER ...; SET <name> AS DEFAULT EMBEDDING PROVIDER");
        }
        return json;
    }

    /**
     * Render the current DEFAULT EMBEDDING PROVIDER as a JSON string suitable for the second
     * argument of {@code embedding(text, parse_json(...))}. Returns {@code null} when no
     * default provider is configured — callers should treat a null return as "embedding
     * unavailable" and write NULL into the embedding column (or skip the search query
     * embedding step).
     *
     * <p>The api_key value is inlined verbatim into the JSON; the {@code env.X} indirection
     * is gone. Producers of this string must keep it out of audit logs, EXPLAIN output, and
     * any other path that would leak the key beyond the BE-side embedding() call site.
     */
    public static String build() {
        AIProvider provider = GlobalStateMgr.getCurrentState().getAIProviderMgr()
                .getDefaultProvider(AIProviderType.EMBEDDING);
        if (provider == null) {
            return null;
        }
        String url = provider.getEndpoint();
        if (Strings.isNullOrEmpty(url)) {
            return null;
        }
        Integer dim = provider.getDimensions();
        Integer timeoutMs = provider.getTimeoutMs();
        String apiKey = provider.getApiKey();
        String model = provider.getModel();

        StringBuilder sb = new StringBuilder(256);
        sb.append('{');
        sb.append("\"endpoint\":").append(jsonString(url));
        sb.append(",\"model\":").append(jsonString(model));
        if (dim != null && dim > 0) {
            sb.append(",\"dimensions\":").append(dim);
        }
        if (timeoutMs != null && timeoutMs > 0) {
            sb.append(",\"timeout_ms\":").append(timeoutMs);
        }
        if (!Strings.isNullOrEmpty(apiKey)) {
            sb.append(",\"api_key\":").append(jsonString(apiKey));
        }
        sb.append('}');
        return sb.toString();
    }

    private static String jsonString(String s) {
        if (s == null) {
            return "\"\"";
        }
        StringBuilder sb = new StringBuilder(s.length() + 2);
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append('"');
        return sb.toString();
    }
}
