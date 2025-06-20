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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/JoinOperator.java

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

package com.starrocks.analysis;

import com.starrocks.thrift.TMatchType;

public enum MatchType {
    MATCH_ALL,
    MATCH_ANY,
    MATCH_PHRASE,
    MATCH_PHRASE_PREFIX,
    MATCH_REGEXP,
    MATCH_PHRASE_EDGE;

    public static MatchType fromString(String type) {
        switch (type.toLowerCase()) {
            case "match_phrase":
                return MATCH_PHRASE;
            case "match_phrase_prefix":
                return MATCH_PHRASE_PREFIX;
            case "match_regexp":
                return MATCH_REGEXP;
            case "match_phrase_edge":
                return MATCH_PHRASE_EDGE;
            case "match_all":
                return MATCH_ALL;
            case "match_any":
            default:
                return MATCH_ANY;
        }
    }

    public TMatchType toThrift() {
        switch (this) {
            case MATCH_PHRASE:
                return TMatchType.MATCH_PHRASE;
            case MATCH_PHRASE_PREFIX:
                return TMatchType.MATCH_PHRASE_PREFIX;
            case MATCH_REGEXP:
                return TMatchType.MATCH_REGEXP;
            case MATCH_PHRASE_EDGE:
                return TMatchType.MATCH_PHRASE_EDGE;
            case MATCH_ALL:
                return TMatchType.MATCH_ALL;
            case MATCH_ANY:
            default:
                return TMatchType.MATCH_ANY;
        }
    }

    @Override
    public String toString() {
        switch (this) {
            case MATCH_ALL:
                return "MATCH_ALL";
            case MATCH_ANY:
                return "MATCH_ANY";
            case MATCH_PHRASE:
                return "MATCH_PHRASE";
            case MATCH_PHRASE_PREFIX:
                return "MATCH_PHRASE_PREFIX";
            case MATCH_REGEXP:
                return "MATCH_REGEXP";
            case MATCH_PHRASE_EDGE:
                return "MATCH_PHRASE_EDGE";
            default:
                return "MATCH";
        }
    }
}
