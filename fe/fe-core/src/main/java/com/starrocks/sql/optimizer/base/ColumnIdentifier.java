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

import java.util.Objects;

/**
 * Currently, the column doesn't have global unique id,
 * so we use table id + column name to identify one column
 */
public final class ColumnIdentifier {
    private final long tableId;
    private final String columnName;

    private long dbId = -1;

    public ColumnIdentifier(long tableId, String columnName) {
        this.tableId = tableId;
        this.columnName = columnName;
    }

    public ColumnIdentifier(long dbId, long tableId, String columnName) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.columnName = columnName;
    }

    public long getTableId() {
        return tableId;
    }

    public String getColumnName() {
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
        if (!(o instanceof ColumnIdentifier)) {
            return false;
        }

        ColumnIdentifier columnIdentifier = (ColumnIdentifier) o;
        if (this == columnIdentifier) {
            return true;
        }
        return tableId == columnIdentifier.tableId && columnName.equalsIgnoreCase(columnIdentifier.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, columnName);
    }
}