// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


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
