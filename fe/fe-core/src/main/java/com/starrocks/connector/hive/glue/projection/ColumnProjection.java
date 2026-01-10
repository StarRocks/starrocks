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

package com.starrocks.connector.hive.glue.projection;

import com.starrocks.type.Type;

import java.util.List;
import java.util.Optional;

/**
 * Interface for partition column projection.
 *
 * Each partition column with projection enabled has an associated ColumnProjection
 * that generates partition values based on the projection type and configuration.
 *
 * Implementations handle different projection types:
 * - EnumProjection: enumerated list of values
 * - IntegerProjection: integer range with optional interval
 * - DateProjection: date range with format and interval
 * - InjectedProjection: values injected from query WHERE clause
 */
public interface ColumnProjection {

    /**
     * Gets the column name this projection is for.
     *
     * @return the partition column name
     */
    String getColumnName();

    /**
     * Generates projected partition values based on an optional filter.
     *
     * When a filter value is present (from WHERE clause), only matching values
     * are returned. When no filter is present, all possible values within the
     * projection range are generated.
     *
     * @param filterValue optional filter value from query predicate
     * @return list of projected partition values
     * @throws IllegalArgumentException if the projection requires a filter value
     *         but none is provided (e.g., INJECTED type)
     */
    List<String> getProjectedValues(Optional<Object> filterValue);

    /**
     * Formats a value for use in partition paths.
     *
     * @param value the value to format
     * @return formatted string representation
     */
    String formatValue(Object value);

    /**
     * Gets the StarRocks column type for this projection.
     *
     * @return the column type
     */
    Type getColumnType();

    /**
     * Checks if this projection requires a filter value in the WHERE clause.
     *
     * @return true if a filter is required (e.g., INJECTED type)
     */
    default boolean requiresFilter() {
        return false;
    }
}
