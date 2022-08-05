// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.dump;

import com.starrocks.catalog.Table;
import com.starrocks.catalog.View;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;

public interface DumpInfo {
    void setOriginStmt(String stmt);

    void addException(String exception);

    void addTable(String dbName, Table table);

    void addView(String dbName, View view);

    void addTableStatistics(Table table, String column, ColumnStatistic columnStatistic);

    void addPartitionRowCount(Table table, String partition, long rowCount);

    void reset();
}
