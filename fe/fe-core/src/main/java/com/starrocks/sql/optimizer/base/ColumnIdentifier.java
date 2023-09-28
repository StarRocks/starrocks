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

import com.starrocks.catalog.Table;

import java.util.Objects;

/**
 * Currently, the column doesn't have global unique id,
 * so we use table id + column name to identify one column
 */
public final class ColumnIdentifier {
    public final String catalogName;
    public final String dbName;
    public final String tableName;
    private final String columnName;
    public final long tableId;

    public ColumnIdentifier(Table t, String columnName) {
        this.catalogName = t.catalogName;
        this.dbName = t.dbName;
        this.tableName = t.getName();
        this.columnName = columnName;
        tableId = t.getId();
    }

    public String getColumnName() {
        return columnName;
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
        return catalogName.equals(columnIdentifier.catalogName) &&
                dbName.equals(columnIdentifier.dbName) && tableName.equals(columnIdentifier.tableName) &&
                columnName.equalsIgnoreCase(columnIdentifier.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, dbName, tableName, columnName);
    }
}