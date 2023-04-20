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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/external/elasticsearch/MappingPhase.java

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

package com.starrocks.connector.elasticsearch;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.EsTable;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

import java.util.Iterator;

/**
 * Get index mapping from remote ES Cluster, and resolved `keyword` and `doc_values` field
 * Later we can use it to parse all relevant indexes
 */
public class MappingPhase implements SearchPhase {

    private EsRestClient client;

    // json response for `{index}/_mapping` API
    private String jsonMapping;

    public MappingPhase(EsRestClient client) {
        this.client = client;
    }

    @Override
    public void execute(SearchContext context) throws StarRocksConnectorException {
        jsonMapping = client.getMapping(context.sourceIndex());
    }

    @Override
    public void postProcess(SearchContext context) {
        resolveFields(context, jsonMapping);
    }

    /**
     * Parse the required field information from the json
     *
     * @param searchContext the current associated column searchContext
     * @param indexMapping  the return value of _mapping
     * @return fetchFieldsContext and docValueFieldsContext
     * @throws Exception
     */
    public void resolveFields(SearchContext searchContext, String indexMapping) throws StarRocksConnectorException {
        JSONObject jsonObject = new JSONObject(indexMapping);
        // the indexName use alias takes the first mapping
        Iterator<String> keys = jsonObject.keys();
        String docKey = keys.next();
        JSONObject docData = jsonObject.optJSONObject(docKey);
        JSONObject mappings = docData.optJSONObject("mappings");
        JSONObject rootSchema = mappings.optJSONObject(searchContext.type());
        JSONObject properties;
        // Elasticsearch 7.x, type was removed from ES mapping, default type is `_doc`
        // https://www.elastic.co/guide/en/elasticsearch/reference/7.0/removal-of-types.html
        // From Elasticsearch 8.x, Specifying types in requests is no longer supported,
        // The include_type_name parameter is removed
        // https://www.elastic.co/guide/en/elasticsearch/reference/7.17/removal-of-types.html
        if (rootSchema == null) {
            // 1. before 7.0, if the `type` does not exist in index, rootSchema is null
            //   this can throw exception within the `properties == null` predicate
            // 2. after or equal 8.x, type is removed from mappings
            properties = mappings.optJSONObject("properties");
        } else {
            properties = rootSchema.optJSONObject("properties");
        }
        if (properties == null) {
            throw new StarRocksConnectorException("index[" + searchContext.sourceIndex() + "] type[" + searchContext.type() +
                    "] mapping not found for the ES Cluster");
        }
        for (Column col : searchContext.columns()) {
            String colName = col.getName();
            // if column exists in StarRocks Table but no found in ES's mapping, we choose to ignore this situation?
            if (!properties.has(colName)) {
                continue;
            }
            JSONObject fieldObject = properties.optJSONObject(colName);

            resolveKeywordFields(searchContext, fieldObject, colName);
            resolveDocValuesFields(searchContext, fieldObject, colName);
        }
    }

    // get a field of keyword type in the fields
    private void resolveKeywordFields(SearchContext searchContext, JSONObject fieldObject, String colName) {
        String fieldType = fieldObject.optString("type");
        // string-type field used keyword type to generate predicate
        // if text field type seen, we should use the `field` keyword type?
        if ("text".equals(fieldType)) {
            JSONObject fieldsObject = fieldObject.optJSONObject("fields");
            if (fieldsObject != null) {
                for (String key : fieldsObject.keySet()) {
                    JSONObject innerTypeObject = fieldsObject.optJSONObject(key);
                    // just for text type
                    if ("keyword".equals(innerTypeObject.optString("type"))) {
                        searchContext.fetchFieldsContext().put(colName, colName + "." + key);
                    }
                }
            }
        }
    }

    private void resolveDocValuesFields(SearchContext searchContext, JSONObject fieldObject, String colName) {
        String fieldType = fieldObject.optString("type");
        // skip `nested` or object type
        if ("nested".equals(fieldType) || fieldObject.has("properties")) {
            return;
        }
        String docValueField = null;
        if (EsTable.DEFAULT_DOCVALUE_DISABLED_FIELDS.contains(fieldType)) {
            JSONObject fieldsObject = fieldObject.optJSONObject("fields");
            if (fieldsObject != null) {
                for (String key : fieldsObject.keySet()) {
                    JSONObject innerTypeObject = fieldsObject.optJSONObject(key);
                    if (EsTable.DEFAULT_DOCVALUE_DISABLED_FIELDS.contains(innerTypeObject.optString("type"))) {
                        continue;
                    }
                    if (innerTypeObject.has("doc_values")) {
                        boolean docValue = innerTypeObject.getBoolean("doc_values");
                        if (docValue) {
                            docValueField = colName;
                        }
                    } else {
                        // a : {c : {}} -> a -> a.c
                        docValueField = colName + "." + key;
                    }
                }
            }
        } else {
            // set doc_value = false manually
            if (fieldObject.has("doc_values")) {
                boolean docValue = fieldObject.optBoolean("doc_values");
                if (!docValue) {
                    return;
                }
            }
            docValueField = colName;
        }
        // docValueField Cannot be null
        if (StringUtils.isNotEmpty(docValueField)) {
            searchContext.docValueFieldsContext().put(colName, docValueField);
        }
    }
}
