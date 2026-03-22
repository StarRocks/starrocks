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
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.NullType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * OpenSearch utility class for type conversion and column schema extraction.
 * NOTE: This is a CORE CLASS for OpenSearch connector functionality.
 * Changes to this class may affect query execution and schema discovery.
 * Please review carefully before modifying.
 */
public class OpenSearchUtil {
    private static final Logger LOG = LogManager.getLogger(OpenSearchUtil.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static List<Column> convertColumnSchema(OpenSearchRestClient client, String indexName) {
        List<Column> columns = new ArrayList<>();
        try {
            // Add _id column (always present in ES/OpenSearch)
            columns.add(new Column("_id", VarcharType.VARCHAR, true));
            columns.add(new Column("_index", VarcharType.VARCHAR, false));
            
            // Get mapping and parse fields
            String mapping = client.getMapping(indexName);
            if (mapping != null) {
                JsonNode root = MAPPER.readTree(mapping);
                // Navigate to properties: {indexName: {mappings: {properties: {fields}}}}
                JsonNode indexNode = root.get(indexName);
                if (indexNode == null) {
                    // Try without index name wrapper
                    indexNode = root;
                }
                JsonNode mappingsNode = indexNode.get("mappings");
                if (mappingsNode != null) {
                    JsonNode propertiesNode = mappingsNode.get("properties");
                    if (propertiesNode != null) {
                        propertiesNode.fields().forEachRemaining(entry -> {
                            String fieldName = entry.getKey();
                            JsonNode fieldProps = entry.getValue();
                            String fieldType = fieldProps.has("type") ? 
                                    fieldProps.get("type").asText() : "keyword";
                            Type srType = convertType(fieldType);
                            columns.add(new Column(fieldName, srType, true));
                        });
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to get mapping for index: " + indexName, e);
        }
        return columns;
    }

    public static Type convertType(String esType) {
        if (esType == null) {
            return NullType.NULL;
        }
        switch (esType.toLowerCase()) {
            case "boolean":
                return BooleanType.BOOLEAN;
            case "byte":
                return IntegerType.TINYINT;
            case "short":
                return IntegerType.SMALLINT;
            case "integer":
                return IntegerType.INT;
            case "long":
                return IntegerType.BIGINT;
            case "float":
                return FloatType.FLOAT;
            case "double":
                return FloatType.DOUBLE;
            case "date":
                return DateType.DATE;
            case "text":
            case "keyword":
                return TypeFactory.createDefaultCatalogString();
            default:
                return TypeFactory.createDefaultCatalogString();
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
