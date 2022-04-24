package com.starrocks.external.hive;

import java.util.Objects;

public class HiveTableName {
    private final String databaseName;
    private final String tableName;

    public HiveTableName(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HiveTableName that = (HiveTableName) o;
        return Objects.equals(databaseName, that.databaseName) && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName);
    }
}
