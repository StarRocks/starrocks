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

package com.starrocks.connector.index;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/** Index capabilities exposed by a connector for one table. */
public final class ConnectorIndexMetadata {
    private static final ConnectorIndexMetadata EMPTY = new ConnectorIndexMetadata(Collections.emptyMap());

    private final Map<String, Set<ConnectorIndexType>> columnIndexes;

    private ConnectorIndexMetadata(Map<String, Set<ConnectorIndexType>> columnIndexes) {
        ImmutableMap.Builder<String, Set<ConnectorIndexType>> builder = ImmutableMap.builder();
        columnIndexes.forEach((column, types) -> builder.put(column, ImmutableSet.copyOf(types)));
        this.columnIndexes = builder.build();
    }

    public static ConnectorIndexMetadata empty() {
        return EMPTY;
    }

    public static ConnectorIndexMetadata of(Map<String, Set<ConnectorIndexType>> columnIndexes) {
        if (columnIndexes.isEmpty()) {
            return EMPTY;
        }
        return new ConnectorIndexMetadata(columnIndexes);
    }

    public boolean isEmpty() {
        return columnIndexes.isEmpty();
    }

    public Set<ConnectorIndexType> getIndexTypes(String columnName) {
        return columnIndexes.getOrDefault(columnName, Collections.emptySet());
    }

    public boolean supports(String columnName, ConnectorIndexType type) {
        return getIndexTypes(columnName).contains(type);
    }

    public Map<String, Set<ConnectorIndexType>> getColumnIndexes() {
        return columnIndexes;
    }

    @Override
    public String toString() {
        return columnIndexes.toString();
    }
}
