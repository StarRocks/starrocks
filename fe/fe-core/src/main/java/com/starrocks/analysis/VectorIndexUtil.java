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

package com.starrocks.analysis;

import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IndexParams;
import com.starrocks.catalog.IndexParams.IndexParamItem;
import com.starrocks.catalog.IndexParams.IndexParamType;
import com.starrocks.catalog.KeysType;
import com.starrocks.common.Config;
import com.starrocks.common.VectorIndexParams;
import com.starrocks.common.VectorIndexParams.CommonIndexParamKey;
import com.starrocks.common.VectorIndexParams.IndexParamsKey;
import com.starrocks.common.VectorIndexParams.SearchParamsKey;
import com.starrocks.common.VectorIndexParams.VectorIndexType;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.IndexDef.IndexType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class VectorIndexUtil {

    public static void checkVectorIndexValid(Column column, Map<String, String> properties, KeysType keysType) {
        if (RunMode.isSharedDataMode()) {
            throw new SemanticException("The vector index does not support shared data mode");
        }
        if (!Config.enable_experimental_vector) {
            throw new SemanticException(
                    "The vector index is disabled, enable it by setting FE config `enable_experimental_vector` to true");
        }

        if (column.isAllowNull()) {
            throw new SemanticException("The vector index can only build on non-nullable column");
        }

        // Only support create vector index on DUPLICATE/PRIMARY table or key columns of UNIQUE/AGGREGATE table.
        if (keysType != KeysType.DUP_KEYS && keysType != KeysType.PRIMARY_KEYS) {
            throw new SemanticException("The vector index can only build on DUPLICATE or PRIMARY table");
        }

        // Type should be ARRAY<float>
        if (!(column.getType() instanceof ArrayType
                && ((ArrayType) column.getType()).getItemType().isFloat())) {
            throw new SemanticException("The vector index can only be build on column with type of array<float>.");
        }

        if (properties == null || properties.isEmpty()) {
            throw new SemanticException("You should set index_type at least to add a vector index.");
        }

        // check param keys which must not be null
        Map<String, IndexParamItem> mustNotNullParams = IndexParams.getInstance().getMustNotNullParams(IndexType.VECTOR);

        Map<String, IndexParamItem> indexIndexParams =
                IndexParams.getInstance().getKeySetByIndexTypeAndParamType(IndexType.VECTOR, IndexParamType.INDEX);
        Map<String, IndexParamItem> searchIndexParams =
                IndexParams.getInstance().getKeySetByIndexTypeAndParamType(IndexType.VECTOR, IndexParamType.SEARCH);

        Map<VectorIndexType, Set<String>> indexParamsGroupByType =
                Arrays.stream(IndexParamsKey.values()).filter(belong -> belong.getBelongVectorIndexType() != null)
                        .collect(Collectors.groupingBy(IndexParamsKey::getBelongVectorIndexType,
                                Collectors.mapping(Enum::name, Collectors.toSet())));

        Map<VectorIndexType, Set<String>> searchParamsGroupByType =
                Arrays.stream(VectorIndexParams.SearchParamsKey.values())
                        .filter(belong -> belong.getBelongVectorIndexType() != null)
                        .collect(Collectors.groupingBy(SearchParamsKey::getBelongVectorIndexType,
                                Collectors.mapping(Enum::name, Collectors.toSet())));

        Set<String> configIndexParams = new HashSet<>();
        Set<String> configSearchParams = new HashSet<>();

        Map<String, IndexParamItem> paramsNeedDefault = IndexParams.getInstance()
                .getKeySetByIndexTypeWithDefaultValue(IndexType.VECTOR);

        VectorIndexType vectorIndexType = null;
        for (Entry<String, String> propEntry : properties.entrySet()) {
            String propKey = propEntry.getKey();
            String upperPropKey = propKey.toUpperCase(Locale.ROOT);
            IndexParams.getInstance().checkParams(upperPropKey, propEntry.getValue());
            mustNotNullParams.remove(upperPropKey);
            paramsNeedDefault.remove(upperPropKey);
            if (upperPropKey.equalsIgnoreCase(CommonIndexParamKey.INDEX_TYPE.name())) {
                vectorIndexType = VectorIndexType.valueOf(propEntry.getValue().toUpperCase(Locale.ROOT));
            } else if (indexIndexParams.containsKey(upperPropKey)) {
                configIndexParams.add(upperPropKey);
            } else if (searchIndexParams.containsKey(upperPropKey)) {
                configSearchParams.add(upperPropKey);
            }
        }

        if (!mustNotNullParams.isEmpty()) {
            String errorMes =
                    mustNotNullParams.values().stream().map(IndexParamItem::getAlert).collect(Collectors.joining("/"));
            throw new SemanticException(errorMes);
        }

        if (vectorIndexType == null) {
            throw new SemanticException("The vector index type is unknown");
        }

        // check whether index and search params define with wrong index type
        configIndexParams.removeAll(Optional.ofNullable(indexParamsGroupByType.get(vectorIndexType))
                .orElse(Collections.emptySet()));

        configSearchParams.removeAll(Optional.ofNullable(searchParamsGroupByType.get(vectorIndexType))
                .orElse(Collections.emptySet()));

        if (!configIndexParams.isEmpty()) {
            throw new SemanticException(String.format("Index params %s should not define with %s", configIndexParams,
                    vectorIndexType));
        }

        if (!configSearchParams.isEmpty()) {
            throw new SemanticException(String.format("Search params %s should not define with %s", configSearchParams,
                    vectorIndexType));
        }

        if (vectorIndexType == VectorIndexType.IVFPQ) {
            String m = properties.get(IndexParamsKey.M_IVFPQ.name().toUpperCase());
            if (m == null) {
                throw new SemanticException("`M_IVFPQ` is required for IVFPQ index");
            }
            // m is a valid integer which is guaranteed by checkParams.
            int mValue = Integer.parseInt(m);

            String dim = properties.get(CommonIndexParamKey.DIM.name().toUpperCase());
            int dimValue = Integer.parseInt(dim);
            if (dimValue % mValue != 0) {
                throw new SemanticException("`DIM` should be a multiple of `M_IVFPQ` for IVFPQ index");
            }
        }

        // add default properties
        Set<String> indexParams = indexParamsGroupByType.get(vectorIndexType);
        paramsNeedDefault.keySet().removeIf(key -> !indexParams.contains(key));
        if (!paramsNeedDefault.isEmpty()) {
            addDefaultProperties(properties, paramsNeedDefault);
        }

        // Lower all the keys and values of properties.
        Map<String, String> lowerProperties = properties.entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().toLowerCase(), entry -> entry.getValue().toLowerCase()));
        properties.clear();
        properties.putAll(lowerProperties);
    }

    private static void addDefaultProperties(Map<String, String> properties, Map<String, IndexParamItem> paramsNeedDefault) {
        paramsNeedDefault.forEach((key, value) -> properties.put(key.toLowerCase(Locale.ROOT), value.getDefaultValue()));
    }
}
