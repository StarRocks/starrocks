// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.external.hive;

import com.starrocks.catalog.Column;

import java.util.List;
import java.util.Objects;

public class HivePartitionKeysKey {
    private final String databaseName;
    private final String tableName;

    // does not participate in hashCode/equals
    private final List<Column> partitionColumns;

    public HivePartitionKeysKey(String databaseName, String tableName, List<Column> partitionColumns) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.partitionColumns = partitionColumns;
    }

    public static HivePartitionKeysKey gen(String databaseName, String tableName, List<Column> partitionColumns) {
        return new HivePartitionKeysKey(databaseName, tableName, partitionColumns);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<Column> getPartitionColumns() {
        return partitionColumns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HivePartitionKeysKey other = (HivePartitionKeysKey) o;
        return Objects.equals(databaseName, other.databaseName) &&
                Objects.equals(tableName, other.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName);
    }
}
