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
import com.starrocks.connector.index.ConnectorIndexType;
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** Versioned FE/BE request carried by the virtual Paimon index table. */
public final class PaimonGlobalIndexRequest {
    public static final int CURRENT_VERSION = 1;
    private static final Gson GSON = new Gson();

    private int version;
    private long snapshotId;
    private JsonObject predicate;
    private Map<String, String> indexes;

    private PaimonGlobalIndexRequest() {
    }

    public static PaimonGlobalIndexRequest create(long snapshotId, IndexCondition condition) {
        PaimonGlobalIndexRequest request = new PaimonGlobalIndexRequest();
        request.version = CURRENT_VERSION;
        request.snapshotId = snapshotId;
        request.predicate = GSON.toJsonTree(
                ScalarOperatorSerializer.toJson(condition.getPredicate())).getAsJsonObject();
        request.indexes = new LinkedHashMap<>();
        condition.getRequiredIndexes().forEach(
                (column, type) -> request.indexes.put(column, type.name().toLowerCase(Locale.ROOT)));
        return request;
    }

    public static PaimonGlobalIndexRequest parse(String json) {
        try {
            PaimonGlobalIndexRequest request = GSON.fromJson(json, PaimonGlobalIndexRequest.class);
            if (request == null || request.version != CURRENT_VERSION || request.snapshotId < 0
                    || request.predicate == null || request.indexes == null || request.indexes.isEmpty()) {
                throw new IllegalArgumentException("Invalid Paimon Global Index request");
            }
            request.getRequiredIndexes();
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

    public Map<String, ConnectorIndexType> getRequiredIndexes() {
        Map<String, ConnectorIndexType> result = new LinkedHashMap<>();
        indexes.forEach((column, type) -> result.put(column,
                ConnectorIndexType.valueOf(type.toUpperCase(Locale.ROOT))));
        return result;
    }
}
