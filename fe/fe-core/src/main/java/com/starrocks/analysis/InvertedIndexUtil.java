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
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.InvertedIndexParams;
import com.starrocks.common.InvertedIndexParams.IndexParamsKey;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.commons.lang3.StringUtils;

import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.common.InvertedIndexParams.CommonIndexParamKey.IMP_LIB;
import static com.starrocks.common.InvertedIndexParams.InvertedIndexImpType.CLUCENE;

public class InvertedIndexUtil {

    public static String INVERTED_INDEX_PARSER_KEY = IndexParamsKey.PARSER.name().toLowerCase(Locale.ROOT);

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
        if (keysType != KeysType.DUP_KEYS) {
            throw new SemanticException("The inverted index can only be build on DUPLICATE table.");
        }
        if (!validGinColumnType(column)) {
            throw new SemanticException("The inverted index can only be build on column with type of scalar type.");
        }

        String impLibKey = IMP_LIB.name().toLowerCase(Locale.ROOT);
        if (properties.containsKey(impLibKey)) {
            String impValue = properties.get(impLibKey);
            if (!CLUCENE.name().equalsIgnoreCase(impValue)) {
                throw new SemanticException("Only support clucene implement for now. ");
            }
        }

        String noMatchParamKey = properties.keySet().stream()
                .filter(key -> !InvertedIndexParams.SUPPORTED_PARAM_KEYS.contains(key.toLowerCase(Locale.ROOT)))
                .collect(Collectors.joining(","));
        if (StringUtils.isNotEmpty(noMatchParamKey)) {
            throw new SemanticException(String.format("Do not support parameters %s for GIN. ", noMatchParamKey));
        }

        InvertedIndexUtil.checkInvertedIndexParser(column.getName(), column.getPrimitiveType(), properties);
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
}
