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

package com.starrocks.connector.paimon;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.connector.index.ConnectorIndexType;
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.connector.index.TopNIndexCondition;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** Versioned FE/BE request carried by the virtual Paimon index table. */
public final class PaimonGlobalIndexRequest {
    public static final int PREDICATE_VERSION = 1;
    public static final int TOP_N_VERSION = 2;
    private static final String TOP_N_KIND = "top_n";
    private static final Gson GSON = new Gson();

    private int version;
    private long snapshotId;
    private String kind;
    private JsonObject predicate;
    private JsonObject scoreExpression;
    private Integer localLimit;
    private Boolean ascending;
    private Map<String, String> indexes;

    private PaimonGlobalIndexRequest() {
    }

    public static PaimonGlobalIndexRequest create(long snapshotId, IndexCondition condition) {
        PaimonGlobalIndexRequest request = new PaimonGlobalIndexRequest();
        request.snapshotId = snapshotId;
        if (condition instanceof TopNIndexCondition) {
            TopNIndexCondition topN = (TopNIndexCondition) condition;
            if (topN.getPredicate() != null) {
                throw new IllegalArgumentException("Filtered Paimon Vector TopN is not supported by protocol v2");
            }
            request.version = TOP_N_VERSION;
            request.kind = TOP_N_KIND;
            request.scoreExpression = GSON.toJsonTree(
                    ScalarOperatorSerializer.toJson(topN.getScoreExpression())).getAsJsonObject();
            request.localLimit = topN.getCandidateLimit();
            request.ascending = topN.isAscending();
        } else {
            request.version = PREDICATE_VERSION;
            request.predicate = GSON.toJsonTree(
                    ScalarOperatorSerializer.toJson(condition.getPredicate())).getAsJsonObject();
        }
        request.indexes = new LinkedHashMap<>();
        condition.getRequiredIndexes().forEach(
                (column, type) -> request.indexes.put(column, type.name().toLowerCase(Locale.ROOT)));
        return request;
    }

    public static PaimonGlobalIndexRequest parse(String json) {
        try {
            PaimonGlobalIndexRequest request = GSON.fromJson(json, PaimonGlobalIndexRequest.class);
            if (request == null || request.snapshotId < 0
                    || request.indexes == null || request.indexes.isEmpty()) {
                throw new IllegalArgumentException("Invalid Paimon Global Index request");
            }
            Map<String, ConnectorIndexType> requiredIndexes = request.getRequiredIndexes();
            if (request.version == PREDICATE_VERSION) {
                if (request.kind != null || request.predicate == null || request.scoreExpression != null
                        || request.localLimit != null || request.ascending != null) {
                    throw new IllegalArgumentException("Invalid Paimon predicate request");
                }
            } else if (request.version == TOP_N_VERSION) {
                if (!TOP_N_KIND.equals(request.kind) || request.predicate != null || request.scoreExpression == null
                        || request.localLimit == null || request.localLimit <= 0
                        || request.ascending == null || requiredIndexes.size() != 1
                        || !requiredIndexes.containsValue(ConnectorIndexType.VECTOR)
                        || !isValidTopNScore(request.scoreExpression, request.ascending)) {
                    throw new IllegalArgumentException("Invalid Paimon Vector TopN request");
                }
            } else {
                throw new IllegalArgumentException("Unsupported Paimon Global Index request version");
            }
            return request;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid Paimon Global Index request JSON", e);
        }
    }

    public static PaimonGlobalIndexRequest parseTransport(String encoded) {
        try {
            byte[] json = Base64.getDecoder().decode(encoded);
            return parse(new String(json, StandardCharsets.UTF_8));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid Paimon Global Index request transport", e);
        }
    }

    public String toJson() {
        return GSON.toJson(this);
    }

    public String toTransportString() {
        return Base64.getEncoder().encodeToString(toJson().getBytes(StandardCharsets.UTF_8));
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public int getVersion() {
        return version;
    }

    public boolean isTopN() {
        return TOP_N_KIND.equals(kind);
    }

    public JsonObject getPredicate() {
        return predicate;
    }

    public JsonObject getScoreExpression() {
        return scoreExpression;
    }

    public int getLocalLimit() {
        if (!isTopN()) {
            throw new IllegalStateException("Local limit is available only for Vector TopN requests");
        }
        return localLimit;
    }

    public boolean isAscending() {
        if (!isTopN()) {
            throw new IllegalStateException("Score ordering is available only for Vector TopN requests");
        }
        return ascending;
    }

    public Map<String, ConnectorIndexType> getRequiredIndexes() {
        Map<String, ConnectorIndexType> result = new LinkedHashMap<>();
        indexes.forEach((column, type) -> result.put(column,
                ConnectorIndexType.valueOf(type.toUpperCase(Locale.ROOT))));
        return result;
    }

    private static boolean isValidTopNScore(JsonObject scoreExpression, boolean ascending) {
        if (!scoreExpression.has("o") || !scoreExpression.get("o").isJsonPrimitive()
                || !"ca".equals(scoreExpression.get("o").getAsString())
                || !scoreExpression.has("f") || !scoreExpression.get("f").isJsonPrimitive()
                || !scoreExpression.has("a") || !scoreExpression.get("a").isJsonArray()
                || scoreExpression.getAsJsonArray("a").size() != 2) {
            return false;
        }
        String function = scoreExpression.get("f").getAsString();
        if (FunctionSet.APPROX_L2_DISTANCE.equalsIgnoreCase(function)) {
            return ascending;
        }
        return !ascending && (FunctionSet.APPROX_COSINE_SIMILARITY.equalsIgnoreCase(function)
                || FunctionSet.APPROX_INNER_PRODUCT.equalsIgnoreCase(function));
    }
}
