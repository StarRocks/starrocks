// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.starrocks.catalog.Table.TableType;

import java.util.List;
import java.util.Objects;

public class HivePartitionName {
    private final String databaseName;
    private final String tableName;
    private final List<String> partitionValues;
    private final TableType tableType;

    public HivePartitionName(String databaseName, String tableName, TableType tableType, List<String> partitionValues) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.partitionValues = partitionValues;
        this.tableType = tableType;
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

    public boolean approximateMatchTable(String db, String tblName) {
        return this.databaseName.equals(db) && this.tableName.equals(tblName) && this.tableType == TableType.HIVE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HivePartitionName other = (HivePartitionName) o;
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
