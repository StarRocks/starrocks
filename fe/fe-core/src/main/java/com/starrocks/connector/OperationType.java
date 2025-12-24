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

import java.util.Set;

/**
 * Enum for catalog operations to ensure type safety and easy management.
 * This enum defines the various operations that can be performed on external catalogs.
 */
public enum OperationType {
    /** ALTER operations including add/drop columns, modify table properties */
    ALTER("ALTER"),

    /** DELETE operations for deleting data from tables */
    DELETE("DELETE"),

    /** CREATE TABLE LIKE operations */
    CREATE_TABLE_LIKE("CREATE TABLE LIKE");

    private final String displayName;

    OperationType(String displayName) {
        this.displayName = displayName;
    }

    /**
     * Get the display name for this operation type.
     * @return the display name suitable for error messages
     */
    public String getDisplayName() {
        return displayName;
    }

    /**
     * Convert a display name back to an OperationType.
     * @param displayName the display name to convert
     * @return the corresponding OperationType or null if not found
     */
    public static OperationType fromDisplayName(String displayName) {
        for (OperationType type : values()) {
            if (type.displayName.equals(displayName)) {
                return type;
            }
        }
        return null;
    }

    /**
     * Create a Set from multiple OperationType objects.
     * @param types the operation types to include in the set
     * @return an unmodifiable Set containing the specified operation types
     */
    public static Set<OperationType> setOf(OperationType... types) {
        return Set.of(types);
    }
}
