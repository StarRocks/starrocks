// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.iceberg;

import java.util.Objects;

public class IcebergTableKey {
    private final String resourceName;
    private final String databaseName;
    private final String tableName;

    public IcebergTableKey(String resourceName, String databaseName, String tableName) {
        this.resourceName = resourceName;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public static IcebergTableKey gen(String resourceName, String databaseName, String tableName) {
        return new IcebergTableKey(resourceName, databaseName, tableName);
    }

    public String getResourceName() {
        return resourceName;
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

        IcebergTableKey other = (IcebergTableKey) o;
        return Objects.equals(databaseName, other.databaseName) &&
                Objects.equals(tableName, other.tableName) &&
                Objects.equals(resourceName, other.resourceName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceName, databaseName, tableName);
    }
}
