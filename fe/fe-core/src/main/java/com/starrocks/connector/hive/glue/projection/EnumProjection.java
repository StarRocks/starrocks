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

import com.google.common.collect.ImmutableList;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Partition projection for enumerated values.
 *
 * The enum projection type defines a comma-separated list of possible partition values.
 * When a query filter matches one of these values, only that value is projected.
 * Without a filter, all enum values are projected.
 *
 * Example table property:
 * <pre>
 *   'projection.region.type' = 'enum',
 *   'projection.region.values' = 'us-east-1,us-west-2,eu-west-1'
 * </pre>
 */
public class EnumProjection implements ColumnProjection {

    private final String columnName;
    private final List<String> values;

    /**
     * Creates an enum projection for a partition column.
     *
     * @param columnName the partition column name
     * @param valuesProperty comma-separated list of enumerated values
     * @throws IllegalArgumentException if valuesProperty is null or empty
     */
    public EnumProjection(String columnName, String valuesProperty) {
        if (valuesProperty == null || valuesProperty.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Enum projection for column '" + columnName + "' requires non-empty values property");
        }

        this.columnName = columnName;
        this.values = ImmutableList.copyOf(
                Arrays.stream(valuesProperty.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .collect(Collectors.toList())
        );

        if (this.values.isEmpty()) {
            throw new IllegalArgumentException(
                    "Enum projection for column '" + columnName + "' has no valid values");
        }
    }

    @Override
    public String getColumnName() {
        return columnName;
    }

    @Override
    public List<String> getProjectedValues(Optional<Object> filterValue) {
        if (filterValue.isPresent()) {
            Object val = filterValue.get();
            if (val == null) {
                return Collections.emptyList();
            }
            String filter = val.toString();
            // Return only matching values
            if (values.contains(filter)) {
                return Collections.singletonList(filter);
            }
            // Filter value not in enum list - return empty
            return Collections.emptyList();
        }
        // No filter - return all enum values
        return values;
    }

    @Override
    public String formatValue(Object value) {
        return value.toString();
    }

    @Override
    public Type getColumnType() {
        return VarcharType.VARCHAR;
    }

    /**
     * Gets all possible enum values.
     *
     * @return immutable list of enum values
     */
    public List<String> getValues() {
        return values;
    }

    @Override
    public String toString() {
        return "EnumProjection{" +
                "columnName='" + columnName + '\'' +
                ", values=" + values +
                '}';
    }
}
