// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table.TableType;

import java.util.List;
import java.util.Objects;

public class HivePartitionKeysKey {
    private final String databaseName;
    private final String tableName;

    // does not participate in hashCode/equals
    private final List<Column> partitionColumns;
    private final TableType tableType;

    public HivePartitionKeysKey(String databaseName, String tableName,
                                TableType tableType, List<Column> partitionColumns) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.partitionColumns = partitionColumns;
        this.tableType = tableType;
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

        HivePartitionKeysKey other = (HivePartitionKeysKey) o;
        return Objects.equals(databaseName, other.databaseName) &&
                Objects.equals(tableName, other.tableName) &&
                Objects.equals(tableType, other.tableType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, tableType);
    }
}
