// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.CsvFileStatisticsStorage;

import java.util.Objects;

public class MockTPCDSStatisticStorage extends CsvFileStatisticsStorage {
    public MockTPCDSStatisticStorage() {
        super("");
        String path = Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("sql")).getPath();
        setPath(path + "/tpcds/statictics1t.csv");
        parse();
    }

    @Override
    public ColumnStatistic getColumnStatistic(Table table, String columnName) {
        ColumnStatistic cc = super.getColumnStatistic(table, columnName);
        Preconditions.checkNotNull(cc, "Null statistics: " + table.getName() + "." + columnName);
        return cc;
    }
}
