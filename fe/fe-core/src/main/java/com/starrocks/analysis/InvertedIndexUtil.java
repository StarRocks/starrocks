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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.IndexParams;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.Config;
import com.starrocks.common.InvertedIndexParams;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.IndexDef.IndexType;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.common.InvertedIndexParams.CommonIndexParamKey.IMP_LIB;
import static com.starrocks.common.InvertedIndexParams.IndexParamsKey.DICT_GRAM_NUM;
import static com.starrocks.common.InvertedIndexParams.IndexParamsKey.PARSER;
import static com.starrocks.common.InvertedIndexParams.InvertedIndexImpType.BUILTIN;
import static com.starrocks.common.InvertedIndexParams.InvertedIndexImpType.CLUCENE;
import static com.starrocks.common.InvertedIndexParams.InvertedIndexImpType.TANTIVY;

public class InvertedIndexUtil {

    public static String INVERTED_INDEX_IMP_LIB_KEY = IMP_LIB.name().toLowerCase(Locale.ROOT);

    public static String INVERTED_INDEX_PARSER_KEY = PARSER.name().toLowerCase(Locale.ROOT);

    /**
     * Do not parse value, index and match with the whole value
     */
    public static String INVERTED_INDEX_PARSER_NONE = "none";

    /**
     * Parse value with StandardAnalyzer, which provides grammar based tokenization (based on the Unicode Text Segmentation
     * algorithm, as specified in https://unicode.org/reports/tr29/) and works well for most languages.
     */
    public static String INVERTED_INDEX_PARSER_STANDARD = "standard";

    /**
     * Parse value with the SimpleAnalyzer, which breaks text into tokens at any non-letter character,
     * such as numbers, spaces, hyphens and apostrophes, discards non-letter characters, and changes uppercase to lowercase.
     */
    public static String INVERTED_INDEX_PARSER_ENGLISH = "english";

    /**
     * Parse value with the LanguageAnalyzer based on chinese, which splits text into tokens according to the co-responding
     * chinese analyzer, default is CJKAnalyzer
     */
    public static String INVERTED_INDEX_PARSER_CHINESE = "chinese";

    /**
     * Parse value with jieba-rs dictionary-based segmentation (tantivy only)
     */
    public static String INVERTED_INDEX_PARSER_JIEBA = "jieba";

    /**
     * Alias for "chinese" — CJK bigram tokenizer (tantivy only)
     */
    public static String INVERTED_INDEX_PARSER_CJK = "cjk";

    public static String INVERTED_INDEX_DICT_GRAM_NUM_KEY = DICT_GRAM_NUM.toString().toLowerCase(Locale.ROOT);

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
        if (!Config.enable_experimental_gin) {
            throw new SemanticException("The inverted index is disabled, enable it by setting FE config `enable_experimental_gin` to true");
        }

        if (properties.containsKey(INVERTED_INDEX_IMP_LIB_KEY)) {
            String impValue = properties.get(INVERTED_INDEX_IMP_LIB_KEY);
            boolean isClucene = CLUCENE.name().equalsIgnoreCase(impValue);
            boolean isBuiltin = BUILTIN.name().equalsIgnoreCase(impValue);
            boolean isTantivy = TANTIVY.name().equalsIgnoreCase(impValue);
            if (!(isClucene || isBuiltin || isTantivy)) {
                throw new SemanticException("Only support clucene, builtin or tantivy implement for now. ");
            }

            if (isClucene && RunMode.isSharedDataMode()) {
                throw new SemanticException("Clucene inverted index does not support shared data mode");
            }

            // Phase 1 limitation: tantivy currently supports DUPLICATE_KEYS only.
            // PRIMARY_KEYS / AGG_KEYS support is planned for Phase 2.
            if (isTantivy && keysType != KeysType.DUP_KEYS) {
                throw new SemanticException(
                        "Tantivy inverted index only supports DUPLICATE_KEYS table in this release");
            }
        } else if (RunMode.isSharedDataMode()) {
            // Default for shared-data mode remains BUILTIN (unchanged from prior behavior).
            // Users must opt into tantivy explicitly via "imp_lib"="tantivy".
            properties.put(INVERTED_INDEX_IMP_LIB_KEY, BUILTIN.name().toLowerCase(Locale.ROOT));
        }

        // Tantivy-only parameters: support_phrase / support_bm25 must not be set on
        // clucene or builtin implementations (where they have no meaning).
        checkTantivyOnlyParams(properties);

        String noMatchParamKey = properties.keySet().stream()
                .filter(key -> !InvertedIndexParams.SUPPORTED_PARAM_KEYS.contains(key.toLowerCase(Locale.ROOT)))
                .collect(Collectors.joining(","));
        if (StringUtils.isNotEmpty(noMatchParamKey)) {
            throw new SemanticException(String.format("Do not support parameters %s for GIN. ", noMatchParamKey));
        }

        InvertedIndexUtil.checkInvertedIndexParser(column.getName(), column.getPrimitiveType(), properties);
        checkInvertedIndexNgram(properties);

        // add default properties
        addDefaultProperties(properties);

        // Tantivy-specific: ensure support_phrase is always present so that
        // SHOW CREATE TABLE displays it for tantivy indexes.
        String impValue = properties.get(INVERTED_INDEX_IMP_LIB_KEY);
        if (TANTIVY.name().equalsIgnoreCase(impValue)) {
            String spKey = InvertedIndexParams.SearchParamsKey.SUPPORT_PHRASE.name().toLowerCase(Locale.ROOT);
            properties.putIfAbsent(spKey, "false");
        }
    }

    private static void addDefaultProperties(Map<String, String> properties) {
        IndexParams.getInstance().getKeySetByIndexTypeWithDefaultValue(IndexType.GIN).entrySet()
                .stream().filter(entry -> !properties.containsKey(entry.getKey().toLowerCase(Locale.ROOT)))
                .forEach(entry -> properties.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue().getDefaultValue()));
    }

    public static void checkInvertedIndexParser(
            String indexColName, PrimitiveType colType, Map<String, String> properties) throws SemanticException {
        String parser = getInvertedIndexParser(properties);
        if (colType.isStringType()) {
            boolean isCommonParser = parser.equals(INVERTED_INDEX_PARSER_NONE)
                    || parser.equals(INVERTED_INDEX_PARSER_STANDARD)
                    || parser.equals(INVERTED_INDEX_PARSER_ENGLISH)
                    || parser.equals(INVERTED_INDEX_PARSER_CHINESE);
            boolean isJieba = parser.equals(INVERTED_INDEX_PARSER_JIEBA);
            boolean isCjk = parser.equals(INVERTED_INDEX_PARSER_CJK);
            if (!isCommonParser && !isJieba && !isCjk) {
                throw new SemanticException("INVERTED index parser: " + parser
                        + " is invalid for column: " + indexColName + " of type " + colType);
            }
            if (isJieba || isCjk) {
                String impValue = properties == null ? null : properties.get(INVERTED_INDEX_IMP_LIB_KEY);
                boolean isTantivy = TANTIVY.name().equalsIgnoreCase(impValue);
                if (!isTantivy) {
                    throw new SemanticException(
                            "parser '" + parser + "' is only supported for tantivy inverted index implementation");
                }
                if (isCjk) {
                    properties.put(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_CHINESE);
                }
            }
        } else if (!parser.equals(INVERTED_INDEX_PARSER_NONE)) {
            throw new SemanticException("INVERTED index with parser: " + parser
                    + " is not supported for column: " + indexColName + " of type " + colType);
        }
    }

    /**
     * Tantivy-only hint parameters (support_phrase / support_bm25) must not be
     * set on non-tantivy implementations. The hints are validated against the
     * SUPPORTED_PARAM_KEYS whitelist already; this guard ensures users do not
     * silently set them on clucene/builtin where they have no effect.
     */
    public static void checkTantivyOnlyParams(Map<String, String> properties) {
        String impValue = properties == null ? null : properties.get(INVERTED_INDEX_IMP_LIB_KEY);
        boolean isTantivy = TANTIVY.name().equalsIgnoreCase(impValue);
        if (isTantivy || properties == null) {
            return;
        }
        String supportPhraseKey =
                InvertedIndexParams.SearchParamsKey.SUPPORT_PHRASE.name().toLowerCase(Locale.ROOT);
        String supportBm25Key =
                InvertedIndexParams.SearchParamsKey.SUPPORT_BM25.name().toLowerCase(Locale.ROOT);
        if (properties.containsKey(supportPhraseKey)) {
            throw new SemanticException(
                    "support_phrase only supported for tantivy inverted index implementation");
        }
        if (properties.containsKey(supportBm25Key)) {
            throw new SemanticException(
                    "support_bm25 only supported for tantivy inverted index implementation");
        }
    }

    public static void checkInvertedIndexNgram(Map<String, String> properties) {
        String gramNum = properties == null ? null : properties.get(INVERTED_INDEX_DICT_GRAM_NUM_KEY);
        if (gramNum == null) {
            return;
        }

        if (!StringUtils.isNumeric(gramNum)) {
            throw new SemanticException("INVERTED index dict gram num " + gramNum + " is a invalid number.");
        }
        int realGramNum = Integer.parseInt(gramNum);
        if (realGramNum <= 0) {
            throw new SemanticException("INVERTED index dict gram num " + gramNum + " should be greater than zero.");
        }

        String impValue = properties.get(INVERTED_INDEX_IMP_LIB_KEY);
        if (!BUILTIN.name().equalsIgnoreCase(impValue)) {
            throw new SemanticException("INVERTED index with " + impValue + " implement is invalid for dict gram.");
        }
    }

    /**
     * Compound indexes use .idx files: GIN with imp_lib=tantivy.
     */
    public static boolean isCompoundIndex(Index index) {
        if (index.getIndexType() == IndexType.GIN) {
            Map<String, String> props = index.getProperties();
            String impLib = props == null ? null : props.get(INVERTED_INDEX_IMP_LIB_KEY);
            return TANTIVY.name().equalsIgnoreCase(impLib);
        }
        return false;
    }

    public static long countCompoundIndexes(List<Index> indexes) {
        if (indexes == null) {
            return 0;
        }
        return indexes.stream().filter(InvertedIndexUtil::isCompoundIndex).count();
    }
}
