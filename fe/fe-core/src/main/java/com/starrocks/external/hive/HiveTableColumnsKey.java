// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table.TableType;

import java.util.List;
import java.util.Objects;

public class HiveTableColumnsKey {
    private final String databaseName;
    private final String tableName;

    // does not participate in hashCode/equals
    private final List<Column> partitionColumns;
    private final List<String> columnNames;

    private final TableType tableType;

    public HiveTableColumnsKey(String databaseName, String tableName, List<Column> partitionColumns,
                               List<String> columnNames, TableType tableType) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.partitionColumns = partitionColumns;
        this.columnNames = columnNames;
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

    public List<String> getColumnNames() {
        return columnNames;
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

        HiveTableColumnsKey other = (HiveTableColumnsKey) o;
        return Objects.equals(databaseName, other.databaseName) &&
                Objects.equals(tableName, other.tableName) &&
                Objects.equals(tableType, other.tableType) &&
                Objects.equals(columnNames, other.columnNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, tableType, columnNames);
    }
}
