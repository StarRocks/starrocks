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

package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.IndexParams;
import com.starrocks.catalog.IndexParams.IndexParamItem;
import com.starrocks.catalog.IndexParams.IndexParamType;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.InvertedIndexParams;
import com.starrocks.common.NgramBfIndexParamsKey;
import com.starrocks.common.VectorIndexParams;
import com.starrocks.common.VectorIndexParams.CommonIndexParamKey;
import com.starrocks.common.VectorIndexParams.IndexParamsKey;
import com.starrocks.common.VectorIndexParams.SearchParamsKey;
import com.starrocks.common.VectorIndexParams.VectorIndexType;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.IndexDef;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.starrocks.common.InvertedIndexParams.CommonIndexParamKey.IMP_LIB;
import static com.starrocks.common.InvertedIndexParams.IndexParamsKey.PARSER;
import static com.starrocks.common.InvertedIndexParams.InvertedIndexImpType.CLUCENE;

/**
 * Analyzer for IndexDef validation and analysis.
 * This class contains all the analyze logic that was previously in IndexDef.
 */
public class IndexAnalyzer {

    private static final int MAX_INDEX_NAME_LENGTH = 64;

    // InvertedIndexUtil constants
    public static String INVERTED_INDEX_PARSER_KEY = PARSER.name().toLowerCase(Locale.ROOT);
    public static String INVERTED_INDEX_PARSER_NONE = "none";
    public static String INVERTED_INDEX_PARSER_STANDARD = "standard";
    public static String INVERTED_INDEX_PARSER_ENGLISH = "english";
    public static String INVERTED_INDEX_PARSER_CHINESE = "chinese";

    // BloomFilterIndexUtil constants
    public static final String FPP_KEY = NgramBfIndexParamsKey.BLOOM_FILTER_FPP.toString().toLowerCase(Locale.ROOT);
    public static final String GRAM_NUM_KEY = NgramBfIndexParamsKey.GRAM_NUM.toString().toLowerCase(Locale.ROOT);
    public static final String CASE_SENSITIVE_KEY = NgramBfIndexParamsKey.CASE_SENSITIVE.toString().toLowerCase(Locale.ROOT);
    private static final double MAX_FPP = 0.05;
    private static final double MIN_FPP = 0.0001;

    /**
     * Analyzes the basic properties of an index definition.
     *
     * @param indexDef the index definition to analyze
     * @throws SemanticException if validation fails
     */
    public static void analyze(IndexDef indexDef) {
        if (indexDef.getColumns() == null) {
            throw new SemanticException("Index can not accept null column.");
        }
        if (Strings.isNullOrEmpty(indexDef.getIndexName())) {
            throw new SemanticException("index name cannot be blank.");
        }
        if (indexDef.getIndexName().length() > MAX_INDEX_NAME_LENGTH) {
            throw new SemanticException("index name too long, the index name length at most is 64.");
        }
        TreeSet<String> distinct = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        distinct.addAll(indexDef.getColumns());
        if (indexDef.getColumns().size() != distinct.size()) {
            throw new SemanticException("columns of index has duplicated.");
        }

        // right now only support single column index
        if (indexDef.getColumns().size() != 1) {
            throw new SemanticException(indexDef.getIndexName() + " index can only apply to a single column.");
        }
    }

    /**
     * Checks if a column is compatible with the specified index type and table keys type.
     *
     * @param column     the column to check
     * @param indexType  the type of the index
     * @param properties the index properties
     * @param keysType   the table keys type
     * @throws SemanticException if the column is not compatible with the index type
     */
    public static void checkColumn(Column column, IndexDef.IndexType indexType,
                                   Map<String, String> properties, KeysType keysType) {
        if (indexType == IndexDef.IndexType.BITMAP) {
            String indexColName = column.getName();
            PrimitiveType colType = column.getPrimitiveType();
            if (!(colType.isDateType() ||
                    colType.isFixedPointType() || colType.isDecimalV3Type() ||
                    colType.isStringType() || colType == PrimitiveType.BOOLEAN)) {
                throw new SemanticException(colType + " is not supported in bitmap index. "
                        + "invalid column: " + indexColName);
            } else if ((keysType == KeysType.AGG_KEYS || keysType == KeysType.UNIQUE_KEYS) && !column.isKey()) {
                throw new SemanticException(
                        "BITMAP index only used in columns of DUP_KEYS/PRIMARY_KEYS table or key columns of"
                                + " UNIQUE_KEYS/AGG_KEYS table. invalid column: " + indexColName);
            }
        } else if (indexType == IndexDef.IndexType.GIN) {
            checkInvertedIndexValid(column, properties, keysType);
        } else if (indexType == IndexDef.IndexType.NGRAMBF) {
            checkNgramBloomFilterIndexValid(column, properties, keysType);
        } else if (indexType == IndexDef.IndexType.VECTOR) {
            checkVectorIndexValid(column, properties, keysType);
        } else {
            throw new SemanticException("Unsupported index type: " + indexType);
        }
    }

    // InvertedIndexUtil methods
    public static String getInvertedIndexParser(Map<String, String> properties) {
        String parser = properties == null ? null : properties.get(INVERTED_INDEX_PARSER_KEY);
        // default is "none" if not set
        return parser != null ? parser : INVERTED_INDEX_PARSER_NONE;
    }

    private static boolean validGinColumnType(Column column) {
        return (column.getType() instanceof ScalarType)
                && (column.getPrimitiveType() == PrimitiveType.CHAR || column.getPrimitiveType() == PrimitiveType.VARCHAR);
    }

    public static void checkInvertedIndexValid(Column column, Map<String, String> properties, KeysType keysType) {
        if (keysType != KeysType.DUP_KEYS && keysType != KeysType.PRIMARY_KEYS) {
            throw new SemanticException("The inverted index can only be build on DUPLICATE/PRIMARY_KEYS table.");
        }
        if (!validGinColumnType(column)) {
            throw new SemanticException("The inverted index can only be build on column with type of CHAR/STRING/VARCHAR type.");
        }
        if (RunMode.isSharedDataMode()) {
            throw new SemanticException("The inverted index does not support shared data mode");
        }
        if (!Config.enable_experimental_gin) {
            throw new SemanticException(
                    "The inverted index is disabled, enable it by setting FE config `enable_experimental_gin` to true");
        }

        String impLibKey = IMP_LIB.name().toLowerCase(Locale.ROOT);
        if (properties.containsKey(impLibKey)) {
            String impValue = properties.get(impLibKey);
            if (!(CLUCENE.name().equalsIgnoreCase(impValue) || BUILTIN.name().equalsIgnoreCase(impValue))) {
                throw new SemanticException("Only support clucene or builtin implement for now. ");
            }
        }

        String noMatchParamKey = properties.keySet().stream()
                .filter(key -> !InvertedIndexParams.SUPPORTED_PARAM_KEYS.contains(key.toLowerCase(Locale.ROOT)))
                .collect(Collectors.joining(","));
        if (StringUtils.isNotEmpty(noMatchParamKey)) {
            throw new SemanticException(String.format("Do not support parameters %s for GIN. ", noMatchParamKey));
        }

        checkInvertedIndexParser(column.getName(), column.getPrimitiveType(), properties);

        // add default properties
        addDefaultProperties(properties);
    }

    private static void addDefaultProperties(Map<String, String> properties) {
        IndexParams.getInstance().getKeySetByIndexTypeWithDefaultValue(IndexDef.IndexType.GIN).entrySet()
                .stream().filter(entry -> !properties.containsKey(entry.getKey().toLowerCase(Locale.ROOT)))
                .forEach(entry -> properties.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue().getDefaultValue()));
    }

    public static void checkInvertedIndexParser(
            String indexColName, PrimitiveType colType, Map<String, String> properties) throws SemanticException {
        String parser = getInvertedIndexParser(properties);
        if (colType.isStringType()) {
            if (!(parser.equals(INVERTED_INDEX_PARSER_NONE)
                    || parser.equals(INVERTED_INDEX_PARSER_STANDARD)
                    || parser.equals(INVERTED_INDEX_PARSER_ENGLISH)
                    || parser.equals(INVERTED_INDEX_PARSER_CHINESE))) {
                throw new SemanticException("INVERTED index parser: " + parser
                        + " is invalid for column: " + indexColName + " of type " + colType);
            }
        } else if (!parser.equals(INVERTED_INDEX_PARSER_NONE)) {
            throw new SemanticException("INVERTED index with parser: " + parser
                    + " is not supported for column: " + indexColName + " of type " + colType);
        }
    }

    // VectorIndexUtil methods
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
        Map<String, IndexParamItem> mustNotNullParams = IndexParams.getInstance().getMustNotNullParams(IndexDef.IndexType.VECTOR);

        Map<String, IndexParamItem> indexIndexParams =
                IndexParams.getInstance().getKeySetByIndexTypeAndParamType(IndexDef.IndexType.VECTOR, IndexParamType.INDEX);
        Map<String, IndexParamItem> searchIndexParams =
                IndexParams.getInstance().getKeySetByIndexTypeAndParamType(IndexDef.IndexType.VECTOR, IndexParamType.SEARCH);

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
                .getKeySetByIndexTypeWithDefaultValue(IndexDef.IndexType.VECTOR);

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
            addDefaultVectorProperties(properties, paramsNeedDefault);
        }

        // Lower all the keys and values of properties.
        Map<String, String> lowerProperties = properties.entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().toLowerCase(), entry -> entry.getValue().toLowerCase()));
        properties.clear();
        properties.putAll(lowerProperties);
    }

    private static void addDefaultVectorProperties(Map<String, String> properties,
                                                   Map<String, IndexParamItem> paramsNeedDefault) {
        paramsNeedDefault.forEach((key, value) -> properties.put(key.toLowerCase(Locale.ROOT), value.getDefaultValue()));
    }

    // BloomFilterIndexUtil methods
    public static double analyzeBloomFilterFpp(Map<String, String> properties) throws SemanticException {
        double bfFpp = 0;
        if (properties != null && properties.containsKey(FPP_KEY)) {
            String bfFppStr = properties.get(FPP_KEY);
            try {
                bfFpp = Double.parseDouble(bfFppStr);
            } catch (NumberFormatException e) {
                throw new SemanticException("Bloom filter fpp is not Double");
            }

            // check range
            if (bfFpp < MIN_FPP || bfFpp > MAX_FPP) {
                throw new SemanticException("Bloom filter fpp should in [" + MIN_FPP + ", " + MAX_FPP + "]");
            }
        }

        return bfFpp;
    }

    private static void analyzeBloomFilterGramNum(Map<String, String> properties) throws SemanticException {
        if (properties != null && properties.containsKey(GRAM_NUM_KEY)) {
            int gramNum = Integer.parseInt(properties.get(GRAM_NUM_KEY));
            if (gramNum <= 0) {
                throw new SemanticException("Ngram Bloom filter's gram_num should be positive number");
            }
        }
    }

    private static void analyzeBloomFilterCaseSensitive(Map<String, String> properties) throws SemanticException {
        if (properties != null && properties.containsKey(CASE_SENSITIVE_KEY)) {
            String caseSensitive = properties.get(CASE_SENSITIVE_KEY);
            if (!caseSensitive.equalsIgnoreCase("true") && !caseSensitive.equalsIgnoreCase("false")) {
                throw new SemanticException("Ngram Bloom filter's case_sensitive should be true or false");
            }
        }
    }

    private static void addDefaultBloomFilterProperties(Map<String, String> properties) {
        properties.computeIfAbsent(FPP_KEY, k -> NgramBfIndexParamsKey.BLOOM_FILTER_FPP.getIndexParamItem().getDefaultValue());
        properties.computeIfAbsent(GRAM_NUM_KEY, k -> NgramBfIndexParamsKey.GRAM_NUM.getIndexParamItem().getDefaultValue());
        properties.computeIfAbsent(CASE_SENSITIVE_KEY,
                k -> NgramBfIndexParamsKey.CASE_SENSITIVE.getIndexParamItem().getDefaultValue());
    }

    public static void checkNgramBloomFilterIndexValid(Column column, Map<String, String> properties, KeysType keysType)
            throws SemanticException {
        Type type = column.getType();

        // tinyint/float/double columns don't support
        if (!type.isStringType()) {
            throw new SemanticException(String.format("Invalid ngram bloom filter column '%s': unsupported type %s",
                    column.getName(), type));
        }

        // Only support create Ngram bloom filter on DUPLICATE/PRIMARY table or key columns of UNIQUE/AGGREGATE table.
        if (!(column.isKey() || keysType == KeysType.PRIMARY_KEYS ||
                column.getAggregationType() == AggregateType.NONE)) {
            // Otherwise the result after aggregation may be wrong
            throw new SemanticException("Ngram Bloom filter index only used in columns of DUP_KEYS/PRIMARY table or "
                    + "key columns of UNIQUE_KEYS/AGG_KEYS table. invalid column: " + column.getName());
        }
        analyzeBloomFilterFpp(properties);
        analyzeBloomFilterGramNum(properties);
        analyzeBloomFilterCaseSensitive(properties);
        // prefer add default values here instead of Index::toThrift
        addDefaultBloomFilterProperties(properties);
    }

    public static void analyseBfWithNgramBf(Table table, Set<Index> newIndexs, Set<ColumnId> bfColumns) throws AnalysisException {
        if (newIndexs.isEmpty() || bfColumns == null || bfColumns.isEmpty()) {
            return;
        }

        for (Index index : newIndexs) {
            List<ColumnId> indexColumns = index.getColumns();
            if (index.getIndexType() == IndexDef.IndexType.NGRAMBF && bfColumns.contains(indexColumns.get(0))) {
                Column column = table.getColumn(indexColumns.get(0));
                throw new AnalysisException("column " + column.getName() +
                        " should only have one bloom filter index " +
                        "or ngram bloom filter index");
            }
        }
    }
}