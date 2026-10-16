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
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.connector.index.ConnectorIndexDescriptor;
import com.starrocks.connector.index.ConnectorIndexDescriptor.IndexType;
import com.starrocks.connector.index.ConnectorIndexDescriptor.Operation;
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.connector.index.TopNIndexCondition;
import com.starrocks.connector.index.VectorIndexMetric;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/** Versioned FE/BE request carried by the virtual Paimon index table. */
public final class PaimonGlobalIndexRequest {
    public static final int CURRENT_VERSION = 3;
    public static final int TOP_N_VERSION = 4;
    private static final String TOP_N_KIND = "unfiltered_top_n";
    private static final Gson GSON = new Gson();

    private int version;
    private Long snapshotId;
    private Long schemaId;
    private Long schemaTimeMillis;
    private JsonObject predicate;
    private String kind;
    private JsonObject scoreExpression;
    private Integer localLimit;
    private Boolean ascending;
    private String tablePath;
    private String branch;
    private Map<String, IndexReference> indexes;

    private static final class IndexReference {
        private Integer fieldId;
        private String provider;

        private IndexReference(int fieldId) {
            this(fieldId, "btree");
        }

        private IndexReference(int fieldId, String provider) {
            this.fieldId = fieldId;
            this.provider = provider;
        }
    }

    private PaimonGlobalIndexRequest() {
    }

    public static PaimonGlobalIndexRequest create(
            String tablePath, String branch, long snapshotId, long schemaId, long schemaTimeMillis,
            IndexCondition condition) {
        if (condition == null || (!(condition instanceof TopNIndexCondition) && condition.getPredicate() == null)) {
            throw new IllegalArgumentException("Paimon distributed execution requires an index condition");
        }
        PaimonGlobalIndexRequest request = new PaimonGlobalIndexRequest();
        request.version = CURRENT_VERSION;
        request.snapshotId = snapshotId;
        request.schemaId = schemaId;
        request.schemaTimeMillis = schemaTimeMillis;
        request.tablePath = tablePath;
        request.branch = branch;
        request.indexes = new LinkedHashMap<>();
        if (condition instanceof TopNIndexCondition) {
            TopNIndexCondition topN = (TopNIndexCondition) condition;
            if (topN.getPredicate() != null || topN.getPartitionPredicate() != null || topN.hasResidual()) {
                throw new IllegalArgumentException("Paimon Vector protocol v4 only supports unfiltered TopN");
            }
            request.version = TOP_N_VERSION;
            request.kind = TOP_N_KIND;
            request.scoreExpression = normalizedScoreExpression(topN);
            request.localLimit = topN.getK();
            request.ascending = topN.isAscending();
            request.indexes.put(topN.getColumnName(), new IndexReference(topN.getFieldId(), "lumina"));
            request.validate();
            return request;
        }
        request.predicate = GSON.toJsonTree(
                ScalarOperatorSerializer.toJson(condition.getPredicate())).getAsJsonObject();
        for (ColumnRefOperator column : condition.getPredicate().getColumnRefs()) {
            Integer fieldId = condition.getFieldIds().get(column.getId());
            if (fieldId == null || fieldId < 0) {
                throw new IllegalArgumentException("Missing stable field binding for " + column);
            }
            IndexReference previous = request.indexes.put(column.getName(), new IndexReference(fieldId));
            if (previous != null && !previous.fieldId.equals(fieldId)) {
                throw new IllegalArgumentException("Ambiguous index expression column " + column.getName());
            }
        }
        request.validate();
        return request;
    }

    private void validate() {
        if ((version != CURRENT_VERSION && version != TOP_N_VERSION)
                || snapshotId == null || snapshotId < 0 || tablePath == null || tablePath.isBlank()
                || schemaId == null || schemaId < 0 || schemaTimeMillis == null || schemaTimeMillis < 0
                || !"main".equals(branch) || indexes == null || indexes.isEmpty()) {
            throw new IllegalArgumentException("Invalid Paimon Global Index request");
        }
        if (version == CURRENT_VERSION) {
            if (predicate == null || kind != null || scoreExpression != null || localLimit != null || ascending != null) {
                throw new IllegalArgumentException("Invalid Paimon scalar v3 request");
            }
        } else if (!TOP_N_KIND.equals(kind) || predicate != null || indexes.size() != 1
                || localLimit == null || localLimit <= 0 || ascending == null || !validVectorScore()) {
            throw new IllegalArgumentException("Invalid Paimon Vector TopN v4 request");
        }
        getRequiredIndexes();
    }

    private static JsonObject normalizedScoreExpression(TopNIndexCondition topN) {
        JsonObject score = new JsonObject();
        score.addProperty("o", "ca");
        score.addProperty("f", switch (topN.getMetric()) {
            case L2 -> FunctionSet.APPROX_L2_DISTANCE;
            case COSINE -> FunctionSet.APPROX_COSINE_SIMILARITY;
            case INNER_PRODUCT -> FunctionSet.APPROX_INNER_PRODUCT;
        });
        JsonObject column = new JsonObject();
        column.addProperty("o", "cr");
        column.addProperty("n", topN.getColumnName());
        JsonObject vector = new JsonObject();
        vector.addProperty("o", "a");
        vector.addProperty("i", "float");
        JsonArray values = new JsonArray();
        for (float value : topN.getQueryVector()) {
            JsonObject element = new JsonObject();
            element.addProperty("o", "co");
            element.addProperty("t", "float");
            element.addProperty("v", value);
            values.add(element);
        }
        vector.add("c", values);
        JsonArray arguments = new JsonArray();
        arguments.add(column);
        arguments.add(vector);
        score.add("a", arguments);
        return score;
    }

    private boolean validVectorScore() {
        if (scoreExpression == null || !"ca".equals(stringMember(scoreExpression, "o"))) {
            return false;
        }
        String function = stringMember(scoreExpression, "f");
        if (!(ascending ? FunctionSet.APPROX_L2_DISTANCE.equals(function)
                : FunctionSet.APPROX_COSINE_SIMILARITY.equals(function)
                || FunctionSet.APPROX_INNER_PRODUCT.equals(function))) {
            return false;
        }
        JsonElement args = scoreExpression.get("a");
        if (args == null || !args.isJsonArray() || args.getAsJsonArray().size() != 2
                || !args.getAsJsonArray().get(0).isJsonObject() || !args.getAsJsonArray().get(1).isJsonObject()) {
            return false;
        }
        JsonObject column = args.getAsJsonArray().get(0).getAsJsonObject();
        JsonObject vector = args.getAsJsonArray().get(1).getAsJsonObject();
        if (!"cr".equals(stringMember(column, "o")) || !indexes.containsKey(stringMember(column, "n"))
                || !"a".equals(stringMember(vector, "o")) || !"float".equals(stringMember(vector, "i"))) {
            return false;
        }
        JsonElement values = vector.get("c");
        if (values == null || !values.isJsonArray() || values.getAsJsonArray().size() == 0) {
            return false;
        }
        for (JsonElement element : values.getAsJsonArray()) {
            if (!element.isJsonObject()) {
                return false;
            }
            JsonObject literal = element.getAsJsonObject();
            JsonElement value = literal.get("v");
            if (!"co".equals(stringMember(literal, "o")) || !"float".equals(stringMember(literal, "t"))
                    || value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()
                    || !Float.isFinite(value.getAsFloat())) {
                return false;
            }
        }
        return true;
    }

    private static String stringMember(JsonObject object, String name) {
        JsonElement value = object.get(name);
        return value != null && value.isJsonPrimitive() && value.getAsJsonPrimitive().isString()
                ? value.getAsString() : null;
    }

    public static PaimonGlobalIndexRequest parse(String json) {
        try {
            PaimonGlobalIndexRequest request = GSON.fromJson(json, PaimonGlobalIndexRequest.class);
            if (request == null) {
                throw new IllegalArgumentException("Invalid Paimon Global Index request");
            }
            request.validate();
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
        return version == TOP_N_VERSION;
    }

    public int getLocalLimit() {
        if (!isTopN()) {
            throw new IllegalStateException("Local limit is only available for Vector TopN requests");
        }
        return localLimit;
    }

    public boolean isAscending() {
        if (!isTopN()) {
            throw new IllegalStateException("Score ordering is only available for Vector TopN requests");
        }
        return ascending;
    }

    public Map<String, Integer> getFieldBindings() {
        Map<String, Integer> result = new LinkedHashMap<>();
        indexes.forEach((name, reference) -> result.put(name, reference.fieldId));
        return Map.copyOf(result);
    }

    public boolean matchesVectorIndex(ConnectorIndexDescriptor descriptor) {
        if (!isTopN() || descriptor.getType() != IndexType.VECTOR || !"lumina".equals(descriptor.getProvider())
                || descriptor.isNullable() || !descriptor.supports(Operation.VECTOR_TOP_N)) {
            return false;
        }
        IndexReference binding = indexes.get(descriptor.getColumnName());
        if (binding == null || binding.fieldId != descriptor.getFieldId()) {
            return false;
        }
        String function = stringMember(scoreExpression, "f");
        VectorIndexMetric metric = FunctionSet.APPROX_L2_DISTANCE.equals(function) ? VectorIndexMetric.L2
                : FunctionSet.APPROX_COSINE_SIMILARITY.equals(function)
                ? VectorIndexMetric.COSINE : VectorIndexMetric.INNER_PRODUCT;
        int dimension = scoreExpression.getAsJsonArray("a").get(1).getAsJsonObject().getAsJsonArray("c").size();
        return descriptor.getVectorMetric().filter(metric::equals).isPresent()
                && descriptor.getVectorDimension().orElse(-1) == dimension;
    }

    public boolean matchesSchema(long id, long timeMillis) {
        return schemaId == id && schemaTimeMillis == timeMillis;
    }

    public String getTablePath() {
        return tablePath;
    }

    public String getBranch() {
        return branch;
    }

    public Map<Integer, IndexType> getRequiredIndexes() {
        Map<Integer, IndexType> result = new LinkedHashMap<>();
        indexes.forEach((column, reference) -> {
            if (column == null || column.isEmpty() || reference == null
                    || reference.fieldId == null || reference.fieldId < 0
                    || !(isTopN() ? "lumina" : "btree").equals(reference.provider)) {
                throw new IllegalArgumentException("Invalid Paimon index field binding");
            }
            result.put(reference.fieldId, isTopN() ? IndexType.VECTOR : IndexType.RANGE);
        });
        return Map.copyOf(result);
    }
}
