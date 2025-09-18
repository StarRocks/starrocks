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

import com.starrocks.common.io.ParamsKey;

import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InvertedIndexParams {

    public static final Set<String> SUPPORTED_PARAM_KEYS = Stream.of(Arrays.stream(CommonIndexParamKey.values()),
                    Arrays.stream(IndexParamsKey.values()), Arrays.stream(SearchParamsKey.values()))
            .flatMap(key -> key.map(k -> k.name().toLowerCase(Locale.ROOT)))
            .collect(Collectors.toSet());

    public enum InvertedIndexImpType {
        CLUCENE
    }

    public enum CommonIndexParamKey implements ParamsKey {
        /**
         * index implement lib, default is clucene
         */
        IMP_LIB
    }


    public enum IndexParamsKey implements ParamsKey {
        /**
         * Specific index parser
         */
        PARSER("none", true),

        /**
         * Whether to omit term frequency and term position when indexing
         */
        OMIT_TERM_FREQ_AND_POSITION("false"),

        /**
         * When parser = chinese, which mode will be used to tokenize.
         * Only 'coarse_grained' and 'fine_grained' are available.
         */
        PARSER_MODE("coarse_grained", true),

        /**
         * Whether support phrase or not. If supports phrase, more storage will be used.
         */
        SUPPORT_PHRASE("true", true),

        /**
         * Specifies preprocessing the text before tokenization, usually to affect tokenization behavior
         * char_filter_type: specifies different functional char_filters (currently only supports char_replace)
         *   char_replace replaces each char in the pattern with a char in the replacement
         * char_filter_pattern: characters to be replaced
         * char_filter_replacement: replacement character array, optional, defaults to a space character
         */
        CHAR_FILTER_TYPE("", false),
        CHAR_FILTER_PATTERN("", false),
        CHAR_FILTER_REPLACEMENT("", false),

        /**
         * Specifies the length limit for non-tokenized string indexes.
         */
        IGNORE_ABOVE("256", true),

        /**
         * Whether to convert tokens to lowercase for case-insensitive matching.
         */
        LOWER_CASE("true", true),

        /**
         * Specifying the stop word list to use, which will affect the behavior of the tokenizer.
         * If not specify, will use builtin stop words.
         * If specify 'none', will not use any stop words.
         */
        STOPWORDS("", true);

        private final String defaultValue;
        private boolean needDefault = false;

        IndexParamsKey(String defaultValue, boolean needDefault) {
            this.defaultValue = defaultValue;
            this.needDefault = needDefault;
        }

        IndexParamsKey(String defaultValue) {
            this.defaultValue = defaultValue;
        }
    }

    public enum SearchParamsKey implements ParamsKey {
        /**
         * Whether to analyze search text
         */
        IS_SEARCH_ANALYZED("false"),
        /**
         * Default search analyzer, it only works when 'IS_SEARCH_ANALYZED' is set to true.
         */
        DEFAULT_SEARCH_ANALYZER("english"),
        /**
         * Whether to support to reserve the rank result within index
         */
        RERANK("false");

        private String defaultValue;

        SearchParamsKey(String defaultValue) {
            this.defaultValue = defaultValue;
        }
    }
}
