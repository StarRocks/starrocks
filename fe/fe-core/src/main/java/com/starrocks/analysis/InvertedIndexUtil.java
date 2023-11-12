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
import com.starrocks.common.AnalysisException;
import com.starrocks.common.InvertedIndexParams.IndexParamsKey;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.Locale;
import java.util.Map;

import static com.starrocks.common.InvertedIndexParams.CommonIndexParamKey.IMP_LIB;
import static com.starrocks.common.InvertedIndexParams.InvertedIndexImpType.CLUCENE;

public class InvertedIndexUtil {

    public static String INVERTED_INDEX_PARSER_KEY = IndexParamsKey.PARSER.name().toLowerCase(Locale.ROOT);
    public static String INVERTED_INDEX_PARSER_NONE = "none";
    public static String INVERTED_INDEX_PARSER_STANDARD = "standard";
    public static String INVERTED_INDEX_PARSER_ENGLISH = "english";
    public static String INVERTED_INDEX_PARSER_CHINESE = "chinese";

    public static String getInvertedIndexParser(Map<String, String> properties) {
        String parser = properties == null ? null : properties.get(INVERTED_INDEX_PARSER_KEY);
        // default is "none" if not set
        return parser != null ? parser : INVERTED_INDEX_PARSER_NONE;
    }


    public static void checkInvertedIndexValid(Column column, Map<String, String> properties, KeysType keysType) {
        if (keysType != KeysType.DUP_KEYS) {
            throw new SemanticException("The inverted index can only be build on DUPLICATE table.");
        }
        if (!(column.getType() instanceof ScalarType)) {
            throw new SemanticException("The inverted index can only be build on column with type of scalar type.");
        }

        String impLibKey = IMP_LIB.name().toLowerCase(Locale.ROOT);
        if (properties.containsKey(impLibKey)) {
            String impValue = properties.get(impLibKey);
            if (!CLUCENE.name().equalsIgnoreCase(impValue)) {
                throw new SemanticException("Only support clucene implement for now");
            }
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
