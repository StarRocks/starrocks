// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.dump;

import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.View;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;

import java.util.Map;

public interface DumpInfo {
    default void setOriginStmt(String stmt) {
    }

    default void addException(String exception) {
    }

    default void addTable(String dbName, Table table) {
    }

    default void addView(String dbName, View view) {
    }

    default void addTableStatistics(Table table, String column, ColumnStatistic columnStatistic) {
    }

    default void addPartitionRowCount(Table table, String partition, long rowCount) {
    }

    default void addCatalog(Catalog catalog) {
    }

    default void addHMSTable(String catalogName, String dbName, String tableName) {
    }

    default Map<String, Map<String, Map<String, HiveMetaStoreTableDumpInfo>>> getHmsTableMap() {
        return null;
    }

    default HiveMetaStoreTableDumpInfo getHMSTable(String catalogName, String dbName, String tableName) {
        return new HiveTableDumpInfo();
    }

    default void reset() {
    }
}
