// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.hive;

import java.util.List;
import java.util.Objects;

public class HivePartitionKey {
    private final String databaseName;
    private final String tableName;
    private final List<String> partitionValues;
    private final boolean isHudiTable;

    public HivePartitionKey(String databaseName, String tableName, List<String> partitionValues) {
        this(databaseName, tableName, partitionValues, false);
    }

    public HivePartitionKey(String databaseName, String tableName, List<String> partitionValues, boolean isHudiTable) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.partitionValues = partitionValues;
        this.isHudiTable = isHudiTable;
    }

    public static HivePartitionKey gen(String databaseName, String tableName, List<String> partitionValues) {
        if (tableName.toLowerCase().contains("hive")) {
            if (partitionValues.get(1).equals("0")) {
                partitionValues.set(1, "false");
            } else {
                partitionValues.set(1, "true");
            }
        }
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

    public boolean isHudiTable() {
        return isHudiTable;
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
                Objects.equals(isHudiTable, other.isHudiTable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, partitionValues, isHudiTable);
    }
}
