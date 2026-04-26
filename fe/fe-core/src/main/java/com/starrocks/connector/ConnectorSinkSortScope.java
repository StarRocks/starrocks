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

package com.starrocks.connector;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Controls the sorting scope for connector sink operations.
 * Now we support three scopes: NONE, FILE and HOST.
 * And more sorting scope can be supported in the future, such as WAREHOUSE, CLUSTER, etc.
 */
public enum ConnectorSinkSortScope {
    /**
     * No sorting is performed.
     */
    NONE("none"),

    /**
     * Sort within each file (current default behavior).
     * Each file written by the connector will have sorted data internally.
     */
    FILE("file"),

    /**
     * Sort at host level.
     * Data sent to each host is sorted, ensuring all files on the same host
     * contain globally sorted data. In this mode, BE should not perform
     * additional file-level sorting since the data is already sorted.
     */
    HOST("host");

    private final String scopeName;

    ConnectorSinkSortScope(String scopeName) {
        this.scopeName = scopeName;
    }

    public static ConnectorSinkSortScope fromName(String scopeName) {
        checkArgument(scopeName != null, "Connector sink sort scope name is null");

        if (NONE.scopeName().equalsIgnoreCase(scopeName)) {
            return NONE;
        } else if (FILE.scopeName().equalsIgnoreCase(scopeName)) {
            return FILE;
        } else if (HOST.scopeName().equalsIgnoreCase(scopeName)) {
            return HOST;
        } else {
            throw new IllegalArgumentException(
                    "Unknown connector sink sort scope: " + scopeName +
                    ", only support none, file, and host");
        }
    }

    public String scopeName() {
        return scopeName;
    }
}
