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

package com.starrocks.common;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class InvertedIndexParams {

    public interface ParamsKey {
        String defaultValue();
        default boolean needDefault() {
            return false;
        }
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
        };
        public static final Set<String> KEY_SET =
                Arrays.stream(values())
                        .map(CommonIndexParamKey::name).collect(Collectors.toSet());
    }


    public enum IndexParamsKey implements ParamsKey {
        PARSER("none", true),
        OMIT_TERM_FREQ_AND_POSITION("false"),
        COMPOUND_FORMAT("false");

        private String defaultValue;
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


        public static final Set<String> KEY_SET =
                Arrays.stream(values())
                        .map(IndexParamsKey::name).collect(Collectors.toSet());
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

        public static final Set<String> KEY_SET =
                Arrays.stream(values())
                        .map(SearchParamsKey::name).collect(Collectors.toSet());
    }
}
