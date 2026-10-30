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
    // How many partitions the rows behind this aggregate came from, and the per-partition distinct
    // counts added up. Both describe the aggregate itself, and both come out of the same query over the
    // same rows as everything else here, which is what makes them safe to divide by and reason with -
    // see StatisticsUtils#estimateColumnStatistics. 0 when the backend did not report them, which is how
    // a backend older than the query version that added them behaves.
    private final long collectedPartitionCount;
    private final long perPartitionNdvSum;

    private static final ConnectorTableColumnStats UNKNOWN =
            new ConnectorTableColumnStats(ColumnStatistic.unknown(), -1, "");

    public ConnectorTableColumnStats(ColumnStatistic columnStatistic, long rowCount, String updateTime) {
        this(columnStatistic, rowCount, updateTime, 0, 0);
    }

    public ConnectorTableColumnStats(ColumnStatistic columnStatistic, long rowCount, String updateTime,
                                     long collectedPartitionCount, long perPartitionNdvSum) {
        this.columnStatistic = columnStatistic;
        this.rowCount = rowCount;
        this.updateTime = updateTime;
        this.collectedPartitionCount = collectedPartitionCount;
        this.perPartitionNdvSum = perPartitionNdvSum;
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

    public long getCollectedPartitionCount() {
        return collectedPartitionCount;
    }

    public long getPerPartitionNdvSum() {
        return perPartitionNdvSum;
    }
}
