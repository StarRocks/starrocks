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

package com.starrocks.qe;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Assigns stable `mock_col_<N>` names for column references so that the
 * mocked SQL and mocked EXPLAIN output stay consistent for the same column
 * (matched by lowercase name). Used by `EXPLAIN COSTS MOCK <query>`.
 *
 * Numbering follows the order in which distinct names are first requested.
 * Lives on ConnectContext for the lifetime of a single explain rendering.
 */
public class MockColumnNameProvider {
    private final Map<String, String> nameToMock = new LinkedHashMap<>();

    public String mockName(String columnName) {
        String key = columnName == null ? "" : columnName.toLowerCase();
        String existing = nameToMock.get(key);
        if (existing != null) {
            return existing;
        }
        String mock = "mock_col_" + (nameToMock.size() + 1);
        nameToMock.put(key, mock);
        return mock;
    }

    public Map<String, String> getMapping() {
        return Collections.unmodifiableMap(nameToMock);
    }
}
