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

package com.starrocks.alter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class AlterHelper {
    static Set<String> collectDroppedOrModifiedColumns(List<Column> oldColumns, List<Column> newColumns) {
        Set<Integer> columnUniqueIdSet = new HashSet<>();
        Set<String> modifiedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        Set<Integer> complexColumnUniqueIdSet = new HashSet<>();
        // Collect modified columns
        for (Column column : newColumns) {
            Preconditions.checkState(column.getUniqueId() >= 0);
            columnUniqueIdSet.add(column.getUniqueId());
            if (column.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX)) {
                modifiedColumns.add(column.getNameWithoutPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX, column.getName()));
            }
        }
        
        // Collect dropped columns
        for (Column column : oldColumns) {
            if (!columnUniqueIdSet.contains(column.getUniqueId())) {
                modifiedColumns.add(column.getName());
            } else if (!column.getType().isScalarType()) {
                complexColumnUniqueIdSet.add(column.getUniqueId());
            }
        }

        // If column is struct column, we may add/drop field of column and these operation does not change
        // the uniqueId of struct column. 
        // So we need to do extra check for struct column.
        for (Integer uid : complexColumnUniqueIdSet) {
            Optional<Column> newCol = newColumns.stream().filter(c -> c.getUniqueId() == uid).findFirst();
            Optional<Column> oldCol = oldColumns.stream().filter(c -> c.getUniqueId() == uid).findFirst();
            if (!newCol.isPresent()) {
                continue;
            }
            if (!newCol.get().equals(oldCol.get())) {
                modifiedColumns.add(oldCol.get().getName());
            }
        }
        return modifiedColumns;
    }
}
