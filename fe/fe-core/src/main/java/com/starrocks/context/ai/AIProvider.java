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
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Persisted metadata for one external AI service provider (embedding / rerank / future text). The
 * full property bag — including the API key — lives on the FE meta journal and image, so cluster
 * upgrades that wipe the installation directory do not lose the credential.
 *
 * <p>This generalizes the original {@code EmbeddingProvider}: the persisted field names {@code i/n/p/c}
 * are unchanged so pre-unification images/journals deserialize as-is, and the new {@code t} (type)
 * field is absent on old records — callers must treat a null type as {@link AIProviderType#EMBEDDING}
 * (handled centrally in {@code AIProviderMgr.gsonPostProcess} and in legacy edit-log replay).
 */
public class AIProvider implements Writable {

    public static final String CREDENTIAL_MASK = "******";

    // Common params (all types)
    public static final String PROPERTY_ENDPOINT = "endpoint";
    public static final String PROPERTY_MODEL = "model";
    public static final String PROPERTY_TIMEOUT_MS = "timeout_ms";
    public static final String PROPERTY_API_KEY = "api_key";
    // Embedding-specific
    public static final String PROPERTY_DIMENSIONS = "dimensions";
    // Rerank-specific (optional): cap documents sent per rerank request
    public static final String PROPERTY_MAX_DOCUMENTS = "max_documents";
    // Rerank-specific (optional): overall wall-clock budget (ms) for the whole rerank call, across
    // all retry attempts. Bounds how long a slow/black-holing reranker can stall a synchronous search
    // before it degrades to fusion order.
    public static final String PROPERTY_DEADLINE_MS = "deadline_ms";

    @SerializedName("i")
    private String id;

    @SerializedName("n")
    private String name;

    // Nullable on records persisted before the AIProvider unification; null means EMBEDDING.
    @SerializedName("t")
    private AIProviderType type;

    @SerializedName("p")
    private Map<String, String> params;

    @SerializedName("c")
    private String comment;

    public AIProvider(String id, String name, AIProviderType type, Map<String, String> params, String comment) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.params = new LinkedHashMap<>(params);
        this.comment = comment == null ? "" : comment;
    }

    public AIProvider(AIProvider other) {
        this.id = other.id;
        this.name = other.name;
        this.type = other.type;
        this.params = new LinkedHashMap<>(other.params);
        this.comment = other.comment;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    /** Never null after normalization; legacy records without a tag are treated as EMBEDDING. */
    public AIProviderType getType() {
        return type == null ? AIProviderType.EMBEDDING : type;
    }

    public void setType(AIProviderType type) {
        this.type = type;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment == null ? "" : comment;
    }

    public Map<String, String> getParams() {
        return new LinkedHashMap<>(params);
    }

    public void mergeParams(Map<String, String> patch) {
        for (Map.Entry<String, String> e : patch.entrySet()) {
            params.put(e.getKey(), e.getValue());
        }
    }

    public String getEndpoint() {
        return params.get(PROPERTY_ENDPOINT);
    }

    public String getModel() {
        return params.get(PROPERTY_MODEL);
    }

    public Integer getDimensions() {
        String v = params.get(PROPERTY_DIMENSIONS);
        return Strings.isNullOrEmpty(v) ? null : Integer.parseInt(v);
    }

    public Integer getTimeoutMs() {
        String v = params.get(PROPERTY_TIMEOUT_MS);
        return Strings.isNullOrEmpty(v) ? null : Integer.parseInt(v);
    }

    public Integer getDeadlineMs() {
        String v = params.get(PROPERTY_DEADLINE_MS);
        return Strings.isNullOrEmpty(v) ? null : Integer.parseInt(v);
    }

    public Integer getMaxDocuments() {
        String v = params.get(PROPERTY_MAX_DOCUMENTS);
        return Strings.isNullOrEmpty(v) ? null : Integer.parseInt(v);
    }

    public String getApiKey() {
        return params.get(PROPERTY_API_KEY);
    }

    public Map<String, String> getMaskedParams() {
        Map<String, String> masked = new LinkedHashMap<>(params);
        addMaskForCredential(masked);
        return masked;
    }

    public static void addMaskForCredential(Map<String, String> params) {
        if (params.containsKey(PROPERTY_API_KEY)) {
            params.put(PROPERTY_API_KEY, CREDENTIAL_MASK);
        }
    }

    public static boolean isCredentialKey(String key) {
        return PROPERTY_API_KEY.equals(key);
    }

    public static Map<String, String> emptyParams() {
        return new HashMap<>();
    }
}
