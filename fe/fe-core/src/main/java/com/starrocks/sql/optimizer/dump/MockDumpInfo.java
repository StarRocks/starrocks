// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.dump;

import com.starrocks.catalog.Table;
import com.starrocks.catalog.View;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;

public class MockDumpInfo implements DumpInfo {
    @Override
    public void setOriginStmt(String stmt) {
    }

    @Override
    public void addException(String exception) {
    }

    @Override
    public void addTable(String dbName, Table table) {
    }

    @Override
    public void addView(String dbName, View view) {
    }

    @Override
    public void addTableStatistics(Table table, String column, ColumnStatistic columnStatistic) {
    }

    @Override
    public void addPartitionRowCount(Table table, String partition, long rowCount) {
    }

    @Override
    public void reset() {
    }
}
