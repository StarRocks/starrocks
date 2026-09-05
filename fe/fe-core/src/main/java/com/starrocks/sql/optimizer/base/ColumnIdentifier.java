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


package com.starrocks.sql.optimizer.base;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.ColumnId;

import java.util.Objects;

/**
 * Currently, the column doesn't have global unique id,
 * so we use table id + column name to identify one column
 */
public final class ColumnIdentifier {
    private final Long tableId;
    private final String tableUuid;
    private final ColumnId columnName;

    private long dbId = -1;

    public ColumnIdentifier(long tableId, ColumnId columnName) {
        this.tableUuid = null; // tableUuid is not used in this context
        this.tableId = tableId;
        this.columnName = columnName;
    }

    public ColumnIdentifier(String tableUuid, ColumnId columnName) {
        this.tableUuid = tableUuid;
        this.tableId = null;
        this.columnName = columnName;
    }

    public long getTableId() {
        Preconditions.checkState(tableId != null);
        return tableId;
    }

    public String getTableUuid() {
        Preconditions.checkState(tableUuid != null);
        return tableUuid;
    }

    public ColumnId getColumnName() {
        return columnName;
    }

    public long getDbId() {
        return dbId;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ColumnIdentifier columnIdentifier)) {
            return false;
        }

        if (this == columnIdentifier) {
            return true;
        }
        return Objects.equals(tableUuid, columnIdentifier.tableUuid) && Objects.equals(tableId,
                columnIdentifier.tableId) && columnName.equalsIgnoreCase(columnIdentifier.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableUuid, tableId, columnName);
    }

    @Override
    public String toString() {
        if (tableUuid != null) {
            return "(uuid)" + dbId + ":" + tableUuid + ":" + columnName;
        } else {
            return "(id)" + dbId + ":" + tableId + ":" + columnName;
        }
    }
}