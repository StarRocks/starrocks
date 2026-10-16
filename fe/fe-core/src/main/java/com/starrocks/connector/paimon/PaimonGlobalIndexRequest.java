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
import com.starrocks.connector.index.ConnectorIndexDescriptor.IndexType;
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/** Versioned FE/BE request carried by the virtual Paimon index table. */
public final class PaimonGlobalIndexRequest {
    public static final int CURRENT_VERSION = 3;
    private static final Gson GSON = new Gson();

    private int version;
    private Long snapshotId;
    private Long schemaId;
    private Long schemaTimeMillis;
    private JsonObject predicate;
    private String tablePath;
    private String branch;
    private Map<String, IndexReference> indexes;

    private static final class IndexReference {
        private Integer fieldId;
        private String provider;

        private IndexReference(int fieldId) {
            this.fieldId = fieldId;
            this.provider = "btree";
        }
    }

    private PaimonGlobalIndexRequest() {
    }

    public static PaimonGlobalIndexRequest create(
            String tablePath, String branch, long snapshotId, long schemaId, long schemaTimeMillis,
            IndexCondition condition) {
        if (condition == null || condition.getPredicate() == null) {
            throw new IllegalArgumentException("Paimon distributed execution requires a scalar index condition");
        }
        PaimonGlobalIndexRequest request = new PaimonGlobalIndexRequest();
        request.version = CURRENT_VERSION;
        request.snapshotId = snapshotId;
        request.schemaId = schemaId;
        request.schemaTimeMillis = schemaTimeMillis;
        request.tablePath = tablePath;
        request.branch = branch;
        request.predicate = GSON.toJsonTree(
                ScalarOperatorSerializer.toJson(condition.getPredicate())).getAsJsonObject();
        request.indexes = new LinkedHashMap<>();
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
        if (version != CURRENT_VERSION || snapshotId == null || snapshotId < 0 || tablePath == null || tablePath.isBlank()
                || schemaId == null || schemaId < 0 || schemaTimeMillis == null || schemaTimeMillis < 0
                || !"main".equals(branch) || predicate == null || indexes == null || indexes.isEmpty()) {
            throw new IllegalArgumentException("Invalid Paimon Global Index request");
        }
        getRequiredIndexes();
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
                    || reference.fieldId == null || reference.fieldId < 0 || !"btree".equals(reference.provider)) {
                throw new IllegalArgumentException("Invalid scalar index field binding");
            }
            result.put(reference.fieldId, IndexType.RANGE);
        });
        return Map.copyOf(result);
    }
}
