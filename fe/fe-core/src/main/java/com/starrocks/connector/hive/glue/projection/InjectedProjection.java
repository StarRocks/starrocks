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
import com.starrocks.type.VarcharType;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Partition projection for injected values.
 *
 * The injected projection type requires the partition value to be provided
 * in the query's WHERE clause. This is useful for partitions where the set
 * of valid values is not known in advance or is too large to enumerate.
 *
 * Example table properties:
 * <pre>
 *   'projection.user_id.type' = 'injected'
 * </pre>
 *
 * Query requirement:
 * <pre>
 *   SELECT * FROM table WHERE user_id = 'some_value'
 * </pre>
 *
 * Without a filter on the injected column, the query will fail.
 */
public class InjectedProjection implements ColumnProjection {

    private final String columnName;

    /**
     * Creates an injected projection for a partition column.
     *
     * @param columnName the partition column name
     */
    public InjectedProjection(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public String getColumnName() {
        return columnName;
    }

    @Override
    public List<String> getProjectedValues(Optional<Object> filterValue) {
        if (!filterValue.isPresent()) {
            throw new IllegalArgumentException(
                    "Injected partition column '" + columnName +
                            "' requires a filter value in the WHERE clause. " +
                            "Please add a predicate like: WHERE " + columnName + " = 'value'");
        }
        return Collections.singletonList(filterValue.get().toString());
    }

    @Override
    public String formatValue(Object value) {
        return value.toString();
    }

    @Override
    public Type getColumnType() {
        return VarcharType.VARCHAR;
    }

    @Override
    public boolean requiresFilter() {
        return true;
    }

    @Override
    public String toString() {
        return "InjectedProjection{" +
                "columnName='" + columnName + '\'' +
                '}';
    }
}
