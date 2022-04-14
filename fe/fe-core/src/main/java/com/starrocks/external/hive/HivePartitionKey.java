// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.hive;

import com.starrocks.catalog.Table.TableType;

import java.util.List;
import java.util.Objects;

public class HivePartitionKey {
    private final String databaseName;
    private final String tableName;
    private final List<String> partitionValues;
    private final TableType tableType;

    public HivePartitionKey(String databaseName, String tableName, List<String> partitionValues) {
        this(databaseName, tableName, TableType.HIVE, partitionValues);
    }

    public HivePartitionKey(String databaseName, String tableName, TableType tableType, List<String> partitionValues) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.partitionValues = partitionValues;
        this.tableType = tableType;
    }

    public static HivePartitionKey gen(String databaseName, String tableName, List<String> partitionValues) {
        return new HivePartitionKey(databaseName, tableName, partitionValues);
    }

    public String getTableName() {
        return tableName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public List<String> getPartitionValues() {
        return partitionValues;
    }

    public TableType getTableType() {
        return tableType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HivePartitionKey other = (HivePartitionKey) o;
        return Objects.equals(databaseName, other.databaseName) &&
                Objects.equals(tableName, other.tableName) &&
                Objects.equals(partitionValues, other.partitionValues) &&
                Objects.equals(tableType, other.tableType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, partitionValues, tableType);
    }
}
