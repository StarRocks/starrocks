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

package com.starrocks.connector.statistics;

import com.starrocks.sql.optimizer.statistics.ColumnStatistic;

public class ConnectorTableColumnStats {

    private final ColumnStatistic columnStatistic;
    private final long rowCount;
    private final String updateTime;

    private static final ConnectorTableColumnStats UNKNOWN =
            new ConnectorTableColumnStats(ColumnStatistic.unknown(), -1, "");

    public ConnectorTableColumnStats(ColumnStatistic columnStatistic, long rowCount, String updateTime) {
        this.columnStatistic = columnStatistic;
        this.rowCount = rowCount;
        this.updateTime = updateTime;
    }

    public static ConnectorTableColumnStats unknown() {
        return UNKNOWN;
    }

    public boolean isUnknown() {
        return columnStatistic.isUnknown();
    }

    public ColumnStatistic getColumnStatistic() {
        return columnStatistic;
    }

    public long getRowCount() {
        return rowCount;
    }

    public String getUpdateTime() {
        return updateTime;
    }
}
