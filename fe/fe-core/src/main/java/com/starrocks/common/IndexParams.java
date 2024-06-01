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

package com.starrocks.common;

import com.starrocks.common.InvertedIndexParams.InvertedIndexImpType;
import com.starrocks.common.io.ParamsKey;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.IndexDef.IndexType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

public class IndexParams {

    private static final Logger LOG = LogManager.getLogger(IndexParams.class);
    private final Map<String, IndexParamItem> paramsHolder = new HashMap<>();

    private IndexParams() {
        /* Vector Index */
        // common
        register(IndexType.VECTOR, IndexParamType.COMMON, VectorIndexParams.CommonIndexParamKey.INDEX_TYPE,
                true, false, null,
                "You should set `index_type` to IVFPQ/HNSW for a vector index.");
        register(IndexType.VECTOR, IndexParamType.COMMON, VectorIndexParams.CommonIndexParamKey.DIM,
                true, false, null,
                "You should set `dim` to a numeric value for a vector index.");
        register(IndexType.VECTOR, IndexParamType.COMMON, VectorIndexParams.CommonIndexParamKey.METRIC_TYPE,
                true, false, null,
                "You should set `metric_type` at least to add a vector index.");
        register(IndexType.VECTOR, IndexParamType.COMMON, VectorIndexParams.CommonIndexParamKey.IS_VECTOR_NORMED, false, true,
                "false", null);
        register(IndexType.VECTOR, IndexParamType.COMMON, VectorIndexParams.CommonIndexParamKey.INDEX_BUILD_THRESHOLD, false,
                false, null, null);

        // index
        register(IndexType.VECTOR, IndexParamType.INDEX, VectorIndexParams.IndexParamsKey.M, false, true, "16", null);
        register(IndexType.VECTOR, IndexParamType.INDEX, VectorIndexParams.IndexParamsKey.EFCONSTRUCTION, false, true, "40",
                null);
        register(IndexType.VECTOR, IndexParamType.INDEX, VectorIndexParams.IndexParamsKey.NBITS, false, false, "8", null);
        register(IndexType.VECTOR, IndexParamType.INDEX, VectorIndexParams.IndexParamsKey.NLIST, false, false, null, null);

        // search
        register(IndexType.VECTOR, IndexParamType.SEARCH, VectorIndexParams.SearchParamsKey.EFSEARCH, false, false, null, null);
        register(IndexType.VECTOR, IndexParamType.SEARCH, VectorIndexParams.SearchParamsKey.NPROBE, false, false, null, null);
        register(IndexType.VECTOR, IndexParamType.SEARCH, VectorIndexParams.SearchParamsKey.MAX_CODES, false, false, null, null);
        register(IndexType.VECTOR, IndexParamType.SEARCH, VectorIndexParams.SearchParamsKey.SCAN_TABLE_THRESHOLD, false, false,
                null, null);
        register(IndexType.VECTOR, IndexParamType.SEARCH, VectorIndexParams.SearchParamsKey.POLYSEMOUS_HT, false, false, null,
                null);
        register(IndexType.VECTOR, IndexParamType.SEARCH, VectorIndexParams.SearchParamsKey.RANGE_SEARCH_CONFIDENCE, false, false,
                null, null);

        /* GIN */
        // common
        register(IndexType.GIN, IndexParamType.COMMON, InvertedIndexParams.CommonIndexParamKey.IMP_LIB, true, true,
                InvertedIndexImpType.CLUCENE.toString().toLowerCase(), null);

        // index
        register(IndexType.GIN, IndexParamType.INDEX, InvertedIndexParams.IndexParamsKey.PARSER, true, true, "none", null);
        register(IndexType.GIN, IndexParamType.INDEX, InvertedIndexParams.IndexParamsKey.OMIT_TERM_FREQ_AND_POSITION, false,
                false,
                null, null);

        // search
        register(IndexType.GIN, IndexParamType.SEARCH, InvertedIndexParams.SearchParamsKey.IS_SEARCH_ANALYZED, false, false,
                "false", null);
        register(IndexType.GIN, IndexParamType.SEARCH, InvertedIndexParams.SearchParamsKey.DEFAULT_SEARCH_ANALYZER, false, false,
                "english", null);
        register(IndexType.GIN, IndexParamType.SEARCH, InvertedIndexParams.SearchParamsKey.RERANK, false, false, "false", null);

        /* NGramFilter */
        // index
        register(IndexType.NGRAMBF, IndexParamType.INDEX, NgramBfIndexParamsKey.GRAM_NUM, true, true,
                String.valueOf(FeConstants.DEFAULT_GRAM_NUM), null);
        register(IndexType.NGRAMBF, IndexParamType.INDEX, NgramBfIndexParamsKey.BLOOM_FILTER_FPP, true, true,
                String.valueOf(FeConstants.DEFAULT_BLOOM_FILTER_FPP), null);
        register(IndexType.NGRAMBF, IndexParamType.INDEX, NgramBfIndexParamsKey.CASE_SENSITIVE, true, true,
                String.valueOf(FeConstants.NGRAM_CASE_SENSITIVE), null);
    }

    private static class Holder {

        private static final IndexParams INSTANCE = new IndexParams();
    }

    public static IndexParams getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * @param paramType index param type
     * @param paramKey param key enum
     * @param mustNotNull whether this param must not be null
     * @param needDefault whether this param need a default value
     * @param defaultValue default value
     * @param alert when this param is not set, user defined alert message
     */
    public void register(IndexType indexType, IndexParamType paramType, ParamsKey paramKey, boolean mustNotNull,
            boolean needDefault,
            String defaultValue, String alert) {
        paramsHolder.put(paramKey.name().toUpperCase(Locale.ROOT),
                new IndexParamItem(indexType, paramType, paramKey, mustNotNull, needDefault, defaultValue, alert));
    }

    public Map<String, IndexParamItem> getKeySet(IndexType indexType, IndexParamType paramType) {
        try {
            return paramsHolder.entrySet().stream()
                    .filter(entry -> entry.getValue().indexType == indexType && entry.getValue().getParamType() == paramType)
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        } catch (Exception e) {
            LOG.error("", e);
        }
        return Collections.emptyMap();
    }

    public Map<String, IndexParamItem> getMustNotNullParams(IndexType indexType) {
        return paramsHolder.entrySet().stream()
                .filter(entry -> entry.getValue().indexType == indexType && entry.getValue().mustNotNull())
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    public void checkParams(String key, String value) throws SemanticException {
        Optional.ofNullable(paramsHolder.get(key)).ifPresent(p -> p.checkValue(value));
    }

    public enum IndexParamType {
        COMMON,
        INDEX,
        SEARCH,
        EXTRA
    }

    public static class IndexParamItem {

        private final IndexType indexType;
        private final IndexParamType paramType;
        private final ParamsKey paramKey;
        private final boolean mustNotNull;
        private final boolean needDefault;
        private final String defaultValue;
        private final String alert;

        private IndexParamItem(IndexType indexType, IndexParamType paramType, ParamsKey paramKey, boolean mustNotNull,
                boolean needDefault, String defaultValue, String alert) {
            this.indexType = indexType;
            this.paramType = paramType;
            this.paramKey = paramKey;
            this.mustNotNull = mustNotNull;
            this.needDefault = needDefault;
            this.defaultValue = defaultValue;
            this.alert = alert;
        }

        public IndexType getIndexType() {
            return indexType;
        }

        public IndexParamType getParamType() {
            return paramType;
        }

        public void checkValue(String value) {
            paramKey.check(value);
        }

        public ParamsKey getParamKey() {
            return paramKey;
        }

        public boolean mustNotNull() {
            return this.mustNotNull;
        }

        public boolean needDefault() {
            return needDefault;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public String getAlert() {
            return alert;
        }
    }
}
