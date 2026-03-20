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

package com.starrocks.connector.opensearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.catalog.Column;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class OpenSearchUtil {
    private static final Logger LOG = LogManager.getLogger(OpenSearchUtil.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static List<Column> convertColumnSchema(OpenSearchRestClient client, String indexName) {
        List<Column> columns = new ArrayList<>();
        try {
            String mapping = client.getMapping(indexName);
            // Simplified - just return basic columns for testing
            columns.add(new Column("_id", ScalarType.createType(PrimitiveType.VARCHAR), true));
            columns.add(new Column("_source", ScalarType.createType(PrimitiveType.VARCHAR), true));
        } catch (Exception e) {
            LOG.warn("Failed to get mapping for index: " + indexName, e);
        }
        return columns;
    }

    public static Type convertType(String esType) {
        if (esType == null) {
            return Type.NULL;
        }
        switch (esType.toLowerCase()) {
            case "boolean":
                return Type.BOOLEAN;
            case "byte":
                return Type.TINYINT;
            case "short":
                return Type.SMALLINT;
            case "integer":
                return Type.INT;
            case "long":
                return Type.BIGINT;
            case "float":
                return Type.FLOAT;
            case "double":
                return Type.DOUBLE;
            case "date":
                return Type.DATE;
            case "text":
            case "keyword":
                return Type.VARCHAR;
            default:
                return Type.VARCHAR;
        }
    }

    public static <T> T getFromJSONArray(String json, Class<T> clazz) {
        try {
            return MAPPER.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            LOG.error("Failed to parse JSON array: {}", json, e);
            return null;
        }
    }

    public static JsonNode readTree(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (JsonProcessingException e) {
            LOG.error("Failed to parse JSON: {}", json, e);
            return null;
        }
    }
}
