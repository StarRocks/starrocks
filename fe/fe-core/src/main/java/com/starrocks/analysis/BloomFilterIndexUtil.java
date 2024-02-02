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

import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.NgramBfIndexParamsKey;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class BloomFilterIndexUtil {
    public static String FPP_KEY = NgramBfIndexParamsKey.BLOOM_FILTER_FPP.toString().toLowerCase(Locale.ROOT);
    public static String GRAM_NUM_KEY = NgramBfIndexParamsKey.GRAM_NUM.toString().toLowerCase(Locale.ROOT);
    private static final double MAX_FPP = 0.05;
    private static final double MIN_FPP = 0.0001;

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

    public static void analyzeBloomFilterGramNum(Map<String, String> properties) throws SemanticException {
        if (properties != null && properties.containsKey(GRAM_NUM_KEY)) {
            int gram_num = Integer.parseInt(properties.get(GRAM_NUM_KEY));
            if (gram_num <= 0) {
                throw new SemanticException("Ngram Bloom filter's gram_num should be positive number");
            }
        }
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
    }

    public static void analyseBfWithNgramBf(Set<Index> newIndexs, Set<String> bfColumns) throws AnalysisException {
        if (newIndexs.isEmpty() || bfColumns.isEmpty()) {
            return;
        }

        for (Index index : newIndexs) {
            if (bfColumns.contains(index.getColumns().get(0))) {
                throw new AnalysisException("column " + index.getColumns().get(0) +
                        " should only have one bloom filter index " +
                        "or ngram bloom filter index");
            }
        }
    }
}
