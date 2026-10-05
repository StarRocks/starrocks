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
<<<<<<< HEAD
=======
    // How many partitions the rows behind this aggregate came from, and the per-partition distinct
    // counts added up. Both describe the aggregate itself, and both come out of the same query over the
    // same rows as everything else here, which is what makes them safe to divide by and reason with -
    // see StatisticsUtils#estimateColumnStatistics. 0 when the backend did not report them, which is how
    // a backend older than the query version that added them behaves.
    private final long collectedPartitionCount;
    private final long perPartitionNdvSum;
    // Two nested subsets of those partitions, about a third and about two thirds of them: the merged
    // distinct count over each and how many partitions each holds - two more points on the curve the
    // merged distinct count traces as partitions are added (see StatisticsUtils#extrapolateNdvFromGrowth).
    // 0 when the backend did not report them.
    private final long thirdNdv;
    private final long twoThirdsNdv;
    private final long thirdPartitionCount;
    private final long twoThirdsPartitionCount;
>>>>>>> 319cf3f814a... [Enhancement] Extrapolate sampled external NDV from its growth over nested partition subsets (#64410)

    private static final ConnectorTableColumnStats UNKNOWN =
            new ConnectorTableColumnStats(ColumnStatistic.unknown(), -1, "");

    public ConnectorTableColumnStats(ColumnStatistic columnStatistic, long rowCount, String updateTime) {
<<<<<<< HEAD
        this.columnStatistic = columnStatistic;
        this.rowCount = rowCount;
        this.updateTime = updateTime;
=======
        this(columnStatistic, rowCount, updateTime, 0, 0);
    }

    public ConnectorTableColumnStats(ColumnStatistic columnStatistic, long rowCount, String updateTime,
                                     long collectedPartitionCount, long perPartitionNdvSum) {
        this(columnStatistic, rowCount, updateTime, collectedPartitionCount, perPartitionNdvSum, 0, 0, 0, 0);
    }

    public ConnectorTableColumnStats(ColumnStatistic columnStatistic, long rowCount, String updateTime,
                                     long collectedPartitionCount, long perPartitionNdvSum,
                                     long thirdNdv, long twoThirdsNdv,
                                     long thirdPartitionCount, long twoThirdsPartitionCount) {
        this.columnStatistic = columnStatistic;
        this.rowCount = rowCount;
        this.updateTime = updateTime;
        this.collectedPartitionCount = collectedPartitionCount;
        this.perPartitionNdvSum = perPartitionNdvSum;
        this.thirdNdv = thirdNdv;
        this.twoThirdsNdv = twoThirdsNdv;
        this.thirdPartitionCount = thirdPartitionCount;
        this.twoThirdsPartitionCount = twoThirdsPartitionCount;
>>>>>>> 319cf3f814a... [Enhancement] Extrapolate sampled external NDV from its growth over nested partition subsets (#64410)
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
<<<<<<< HEAD
=======

    public long getCollectedPartitionCount() {
        return collectedPartitionCount;
    }

    public long getPerPartitionNdvSum() {
        return perPartitionNdvSum;
    }

    public long getThirdNdv() {
        return thirdNdv;
    }

    public long getTwoThirdsNdv() {
        return twoThirdsNdv;
    }

    public long getThirdPartitionCount() {
        return thirdPartitionCount;
    }

    public long getTwoThirdsPartitionCount() {
        return twoThirdsPartitionCount;
    }
>>>>>>> 319cf3f814a... [Enhancement] Extrapolate sampled external NDV from its growth over nested partition subsets (#64410)
}
