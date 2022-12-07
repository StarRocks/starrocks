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


package com.starrocks.common.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.starrocks.thrift.TWriteQuorumType;

public class WriteQuorum {
    public static final String MAJORITY = "MAJORITY";
    public static final String ALL = "ALL";
    public static final String ONE = "ONE";

    private static final ImmutableMap<String, TWriteQuorumType> T_WRITE_QUORUM_BY_NAME =
            (new ImmutableSortedMap.Builder<String, TWriteQuorumType>(String.CASE_INSENSITIVE_ORDER))
                    .put(MAJORITY, TWriteQuorumType.MAJORITY)
                    .put(ONE, TWriteQuorumType.ONE)
                    .put(ALL, TWriteQuorumType.ALL)
                    .build();

    private static final ImmutableMap<TWriteQuorumType, String> T_WRITE_QUORUM_TO_NAME =
            (new ImmutableMap.Builder<TWriteQuorumType, String>())
                    .put(TWriteQuorumType.MAJORITY, MAJORITY)
                    .put(TWriteQuorumType.ONE, ONE)
                    .put(TWriteQuorumType.ALL, ALL)
                    .build();

    public static TWriteQuorumType findTWriteQuorumByName(String name) {
        return T_WRITE_QUORUM_BY_NAME.get(name);
    }

    public static String writeQuorumToName(TWriteQuorumType type) {
        return T_WRITE_QUORUM_TO_NAME.get(type);
    }
}
