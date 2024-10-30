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

package com.starrocks.load.batchwrite;

import java.util.Objects;

/**
 * Represents an identifier for a table in the database.
 */
public class TableId {

    /** The name of the database. */
    private final String dbName;

    /** The name of the table. */
    private final String tableName;

    public TableId(String dbName, String tableName) {
        this.dbName = dbName;
        this.tableName = tableName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableId tableId = (TableId) o;
        return Objects.equals(dbName, tableId.dbName) && Objects.equals(tableName, tableId.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbName, tableName);
    }

    @Override
    public String toString() {
        return "TableId{" +
                "dbName='" + dbName + '\'' +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}
