// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import java.util.Objects;

public class HiveTableKey {
    private final String databaseName;
    private final String tableName;

    public HiveTableKey(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public static HiveTableKey gen(String databaseName, String tableName) {
        return new HiveTableKey(databaseName, tableName);
    }

    public String getDatabaseName() {
        return databaseName;
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

        HiveTableKey other = (HiveTableKey) o;
        return Objects.equals(databaseName, other.databaseName) &&
                Objects.equals(tableName, other.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName);
    }
}
