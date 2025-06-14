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


package com.starrocks.connector.jdbc;

import java.util.Objects;

public class JDBCTableName {
    private final String catalogName;
    private final String databaseName;
    private final String tableName;

    public JDBCTableName(String catalogName, String databaseName, String tableName) {
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public static JDBCTableName of(String catalogName, String databaseName, String tableName) {
        return new JDBCTableName(catalogName, databaseName, tableName);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String toString() {
        return "[catalogName: " + catalogName + ", databaseName: " + databaseName + ", tableName: " + tableName + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JDBCTableName other = (JDBCTableName) o;
        return Objects.equals(catalogName, other.catalogName) &&
                Objects.equals(databaseName, other.databaseName) &&
                Objects.equals(tableName, other.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, databaseName, tableName);
    }
}
