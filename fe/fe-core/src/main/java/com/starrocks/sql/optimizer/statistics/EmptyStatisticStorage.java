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


package com.starrocks.sql.optimizer.statistics;

import com.starrocks.catalog.Table;

import java.util.List;
import java.util.stream.Collectors;

// Only for debug
public class EmptyStatisticStorage implements StatisticStorage {
    @Override
    public ColumnStatistic getColumnStatistic(Table table, String column) {
        return ColumnStatistic.unknown();
    }

    @Override
    public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
        return columns.stream().map(k -> getColumnStatistic(table, k)).collect(Collectors.toList());
    }

    @Override
    public void addColumnStatistic(Table table, String column, ColumnStatistic columnStatistic) {
    }
}
