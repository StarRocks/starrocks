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

package com.starrocks.catalog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.thrift.PlanNodesConstants.DYNAMIC_RSS_ID_COLUMN_NAME;
import static com.starrocks.thrift.PlanNodesConstants.ROWSET_ID_COLUMN_NAME;
import static com.starrocks.thrift.PlanNodesConstants.ROW_ID_COLUMN_NAME;
import static com.starrocks.thrift.PlanNodesConstants.RSS_ID_COLUMN_NAME;
import static com.starrocks.thrift.PlanNodesConstants.SEGMENT_ID_COLUMN_NAME;
import static com.starrocks.thrift.PlanNodesConstants.SOURCE_ID_COLUMN_NAME;
import static com.starrocks.thrift.PlanNodesConstants.TABLET_ID_COLUMN_NAME;

/**
 * Central registry for all virtual columns in StarRocks.
 * Virtual columns are computed at runtime and not persisted in storage.
 * 
 * To add a new virtual column:
 * 1. Add the column name constant to PlanNodes.thrift
 * 2. Register the column definition in VIRTUAL_COLUMNS list below
 * 3. The column will be automatically available in queries
 */
public class VirtualColumnRegistry {
    
    /**
     * Registry of all virtual column definitions.
     * To add a new virtual column, add an entry here with:
     * - Column name (from PlanNodesConstants)
     * - Data type
     * - Description
     * - Enabled flag (optional, defaults to true)
     */
    private static final List<VirtualColumnDefinition> VIRTUAL_COLUMNS = ImmutableList.of(
            new VirtualColumnDefinition(
                    TABLET_ID_COLUMN_NAME,
                    IntegerType.BIGINT,
                    "Tablet ID of the data block containing this row"
            ),
            new VirtualColumnDefinition(
                    ROWSET_ID_COLUMN_NAME,
                    StringType.STRING,
                    "RowSet ID of the data block containing this row",
                    false
            ),
            new VirtualColumnDefinition(
                    SEGMENT_ID_COLUMN_NAME,
                    IntegerType.BIGINT,
                    "Segment id"
            ),
            new VirtualColumnDefinition(
                    RSS_ID_COLUMN_NAME,
                    IntegerType.INT,
                    "RowSet Segment id"
            ),
            new VirtualColumnDefinition(
                    SOURCE_ID_COLUMN_NAME,
                    IntegerType.INT,
                    "Source id",
                    false
            ),
            new VirtualColumnDefinition(
                    DYNAMIC_RSS_ID_COLUMN_NAME,
                    IntegerType.INT,
                    "Dynamic RowSet Segment id",
                    false
            ),
            new VirtualColumnDefinition(
                    ROW_ID_COLUMN_NAME,
                    IntegerType.BIGINT,
                    "Row ID within the segment"
            )
    );
    
    // Fast lookup map: column name (case-insensitive) -> definition
    private static final Map<String, VirtualColumnDefinition> NAME_TO_DEFINITION;
    
    static {
        NAME_TO_DEFINITION = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (VirtualColumnDefinition def : VIRTUAL_COLUMNS) {
            NAME_TO_DEFINITION.put(def.getName(), def);
        }
    }
    
    /**
     * Get virtual column definition by name (case-insensitive).
     * @param name Column name
     * @return Virtual column definition, or null if not found
     */
    public static VirtualColumnDefinition getDefinition(String name) {
        return NAME_TO_DEFINITION.get(name);
    }
    
    /**
     * Get virtual column instance by name (case-insensitive).
     * @param name Column name
     * @return Column instance, or null if not found
     */
    public static Column getColumn(String name) {
        VirtualColumnDefinition def = getDefinition(name);
        return def != null ? def.getColumn() : null;
    }
    
    /**
     * Get all enabled virtual column definitions.
     * @return List of enabled virtual column definitions
     */
    public static List<VirtualColumnDefinition> getAllDefinitions() {
        return new ArrayList<>(VIRTUAL_COLUMNS);
    }
    
    /**
     * Get all enabled virtual column instances.
     * @return List of enabled virtual columns
     */
    public static List<Column> getAllColumns() {
        return getAllDefinitions().stream()
                .map(VirtualColumnDefinition::getColumn)
                .collect(Collectors.toList());
    }
    
    /**
     * Check if a column name is a registered virtual column (case-insensitive).
     * @param name Column name
     * @return True if this is a virtual column, false otherwise
     */
    public static boolean isVirtualColumn(String name) {
        return NAME_TO_DEFINITION.containsKey(name);
    }
    
    /**
     * Get the number of enabled virtual columns.
     * @return Count of enabled virtual columns
     */
    public static int getCount() {
        return NAME_TO_DEFINITION.size();
    }
}
