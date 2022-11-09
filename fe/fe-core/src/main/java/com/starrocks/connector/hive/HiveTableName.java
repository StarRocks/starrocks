// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive;

import java.util.Objects;

public class HiveTableName {
    private final String databaseName;
    private final String tableName;

    public HiveTableName(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public static HiveTableName of(String databaseName, String tableName) {
        return new HiveTableName(databaseName, tableName);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public String toString() {
        return "[databaseName: " + databaseName + ", tableName: " + tableName + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HiveTableName other = (HiveTableName) o;
        return Objects.equals(databaseName, other.databaseName) &&
                Objects.equals(tableName, other.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName);
    }
}
