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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/external/elasticsearch/EsUtil.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.external.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.RangePartitionDesc;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

public class EsUtil {

    public static void analyzePartitionAndDistributionDesc(PartitionDesc partitionDesc,
                                                           DistributionDesc distributionDesc) {
        if (partitionDesc == null && distributionDesc == null) {
            return;
        }

        if (partitionDesc != null) {
            if (!(partitionDesc instanceof RangePartitionDesc)) {
                throw new SemanticException("Elasticsearch table only permit range partition");
            }

            RangePartitionDesc rangePartitionDesc = (RangePartitionDesc) partitionDesc;
            analyzePartitionDesc(rangePartitionDesc);
        }

        if (distributionDesc != null) {
            throw new SemanticException("could not support distribution clause");
        }
    }

    private static void analyzePartitionDesc(RangePartitionDesc partDesc) {
        if (partDesc.getPartitionColNames() == null || partDesc.getPartitionColNames().isEmpty()) {
            throw new SemanticException("No partition columns.");
        }

        if (partDesc.getPartitionColNames().size() > 1) {
            throw new SemanticException(
                    "Elasticsearch table's parition column could only be a single column");
        }
    }

    /**
     * get the json object from specified jsonObject
     *
     * @param jsonObject
     * @param key
     * @return
     */
    public static JSONObject getJsonObject(JSONObject jsonObject, String key, int fromIndex) {
        int firstOccr = key.indexOf('.', fromIndex);
        if (firstOccr == -1) {
            String token = key.substring(key.lastIndexOf('.') + 1);
            if (jsonObject.containsKey(token)) {
                return (JSONObject) jsonObject.get(token);
            } else {
                return null;
            }
        }
        String fieldName = key.substring(fromIndex, firstOccr);
        if (jsonObject.containsKey(fieldName)) {
            return getJsonObject((JSONObject) jsonObject.get(fieldName), key, firstOccr + 1);
        } else {
            return null;
        }
    }

    /**
     * @param properties
     * @param name
     * @return
     * @throws DdlException
     */
    public static boolean tryGetBoolean(Map<String, String> properties, String name) throws DdlException {
        String property = properties.get(name).trim();
        try {
            return Boolean.parseBoolean(property);
        } catch (Exception e) {
            throw new DdlException(String.format("fail to parse %s, %s = %s, `%s` should be like 'true' or 'false', "
                    + "value should be double quotation marks", name, name, property, name));
        }
    }

    /**
     * Generate columns from ElasticSearch.
     **/
    public static List<Column> convertColumnSchema(EsRestClient client, String index) {
        String mappings = client.getMapping(index);
        JSONObject properties = parseProperties(index, mappings);
        List<Column> columns = new ArrayList<>();
        for (String columnName : (Set<String>) properties.keySet()) {
            JSONObject columnAttr = (JSONObject) properties.get(columnName);
            //default set string.
            Type type = Type.STRING;
            if (columnAttr.containsKey("type")) {
                type = convertType(columnAttr.get("type").toString());
            }
            Column column = new Column(columnName, type, true, null, true,
                    null, "comment");
            columns.add(column);
        }
        return columns;
    }

    /**
     * Transfer es type to sr type.
     **/
    public static Type convertType(String esType) {
        switch (esType) {
            case "null":
                return Type.NULL;
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
            case "unsigned_long":
                return Type.LARGEINT;
            case "float":
            case "half_float":
                return Type.FLOAT;
            case "double":
            case "scaled_float":
                return Type.DOUBLE;
                //TODO
            case "date":
                return Type.DATETIME;
            case "keyword":
            case "text":
            case "ip":
            case "nested":
            case "object":
            default:
                return ScalarType.createType("STRING");
        }
    }

    /**
     * {
     *     "media_account": {
     *         "mappings": {
     *             "properties": {
     *                 "@timestamp": {
     *                     "type": "date"
     *                 },
     *                 "access_token": {
     *                     "type": "keyword"
     *                 },
     *                 "access_token_expires": {
     *                     "type": "long"
     *                 }
     *             }
     *         }
     *     }
     * @param index
     * @param mapping
     * @return
     */
    public static JSONObject parseProperties(String index, String mapping) {
        JSONObject mappingsElement = parseMappingsElement(mapping);
        JSONObject propertiesRoot = parsePropertiesRoot(mappingsElement);
        JSONObject properties = (JSONObject) propertiesRoot.get("properties");
        if (null == properties) {
            throw new StarRocksESException("index[" + index + "] 's properties not found for the ES");
        }
        return properties;
    }

    /**
     * get mappings element
     * @param indexMapping
     * @return
     */
    private static JSONObject parseMappingsElement(String indexMapping) {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(indexMapping);
        // If the indexName use alias takes the first mapping
        Iterator<String> keys = jsonObject.keySet().iterator();
        String docKey = keys.next();
        JSONObject docData = (JSONObject) jsonObject.get(docKey);
        return (JSONObject) docData.get("mappings");
    }

    /**
     * content
     * @param mappings
     * @return
     */
    private static JSONObject parsePropertiesRoot(JSONObject mappings) {
        String element = (String) mappings.keySet().iterator().next();
        if (!"properties".equals(element)) {
            // If type is not passed in takes the first type.
            return (JSONObject) mappings.get(element);
        }
        // Equal 7.x and after
        return mappings;
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true)
            .configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true)
            .setTimeZone(TimeZone.getDefault())
            .setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

    public static JsonNode readTree(String text) {
        try {
            return OBJECT_MAPPER.readTree(text);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("readTree exception.", e);
        }
    }

    public static <T> List<T> fromJSONArray(String text, Class<T> elementClass) {
        try {
            JavaType javaType = OBJECT_MAPPER.getTypeFactory().constructParametricType(ArrayList.class,
                    new Class[] {elementClass});
            return OBJECT_MAPPER.readValue(text, javaType);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("fromJSONArray exception.", e);
        }
    }

}
