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

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

public class InvertedIndexParams {

    public static void setDefaultParamsValue(Map<String, String> properties, ParamsKey[] e) {
        Arrays.stream(e).filter(k -> !properties.containsKey(k.name()) && k.needDefault())
                .forEach(k -> properties.put(k.name().toLowerCase(Locale.ROOT), k.defaultValue()));
    }

    public interface ParamsKey {

        String defaultValue();

        default boolean needDefault() {
            return false;
        }

        // auto implemented by Enum.name()
        String name();
    }

    public enum InvertedIndexImpType {
        CLUCENE
    }

    public enum CommonIndexParamKey implements ParamsKey {
        // index implement lib, default is clucene
        IMP_LIB {
            @Override
            public String defaultValue() {
                return InvertedIndexImpType.CLUCENE.name();
            }

            @Override
            public boolean needDefault() {
                return true;
            }
        }
    }


    public enum IndexParamsKey implements ParamsKey {
        PARSER("none", true),
        OMIT_TERM_FREQ_AND_POSITION("false"),
        COMPOUND_FORMAT("false");

        private final String defaultValue;
        private boolean needDefault = false;

        IndexParamsKey(String defaultValue, boolean needDefault) {
            this.defaultValue = defaultValue;
            this.needDefault = needDefault;
        }

        IndexParamsKey(String defaultValue) {
            this.defaultValue = defaultValue;
        }

        @Override
        public String defaultValue() {
            return defaultValue;
        }

        @Override
        public boolean needDefault() {
            return needDefault;
        }
    }

    public enum SearchParamsKey implements ParamsKey {
        IS_SEARCH_ANALYZED("false"),
        DEFAULT_SEARCH_ANALYZER("english"),
        RERANK("false");

        private String defaultValue;

        SearchParamsKey(String defaultValue) {
            this.defaultValue = defaultValue;
        }

        @Override
        public String defaultValue() {
            return defaultValue;
        }
    }
}
