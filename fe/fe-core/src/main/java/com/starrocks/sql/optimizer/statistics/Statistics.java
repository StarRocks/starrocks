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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.lang.Double.NaN;

public class Statistics {
    private final double outputRowCount;
    private final Map<ColumnRefOperator, ColumnStatistic> columnStatistics;
    // This flag set true if get table row count from GlobalStateMgr LE 1
    // Table row count in FE depends on BE reporting，but FE may not get report from BE which just started，
    // this causes the table row count stored in FE to be inaccurate.
    private final boolean tableRowCountMayInaccurate;
    private final Collection<ColumnRefOperator> shadowColumns;

    private final Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> multiColumnCombinedStats;

    private Statistics(Builder builder) {
        this.outputRowCount = builder.outputRowCount;
        this.columnStatistics = builder.columnStatistics;
        this.tableRowCountMayInaccurate = builder.tableRowCountMayInaccurate;
        this.shadowColumns = builder.shadowColumns;
        this.multiColumnCombinedStats = builder.multiColumnCombinedStats;
    }

    public double getOutputRowCount() {
        return outputRowCount;
    }

    public double getOutputSize(ColumnRefSet outputColumns) {
        double totalSize = 0;
        boolean nonEmpty = false;
        for (Map.Entry<ColumnRefOperator, ColumnStatistic> entry : columnStatistics.entrySet()) {
            if (shadowColumns.contains(entry.getKey())) {
                continue;
            }
            if (outputColumns.contains(entry.getKey().getId())) {
                if (!entry.getValue().isUnknown()) {
                    totalSize += entry.getValue().getAverageRowSize();
                } else {
                    totalSize += entry.getKey().getType().getTypeSize();
                }
                nonEmpty = true;
            }
        }
        if (nonEmpty) {
            totalSize = Math.max(totalSize, 1.0);
        }
        return totalSize * outputRowCount;
    }

    public double getComputeSize() {
        return getAvgRowSize() * outputRowCount;
    }

    public double getAvgRowSize() {
        // Make it at least 1 byte, otherwise the cost model would propagate estimate error
        double totalSize = 0;
        for (Map.Entry<ColumnRefOperator, ColumnStatistic> entry : columnStatistics.entrySet()) {
            if (shadowColumns.contains(entry.getKey())) {
                continue;
            }
            if (!entry.getValue().isUnknown()) {
                totalSize += entry.getValue().getAverageRowSize();
            } else {
                totalSize += entry.getKey().getType().getTypeSize();
            }
        }
        return Math.max(totalSize, 1.0);
    }

    public ColumnStatistic getColumnStatistic(ColumnRefOperator column) {
        if (columnStatistics.get(column) == null) {
            throw new StarRocksPlannerException(ErrorType.INTERNAL_ERROR,
                    "only found column statistics: %s, but missing statistic of col: %s.",
                    ColumnRefOperator.toString(columnStatistics.keySet()), column);
        } else {
            return columnStatistics.get(column);
        }
    }

    public Map<ColumnRefOperator, ColumnStatistic> getColumnStatistics() {
        return columnStatistics;
    }

    public Map<ColumnRefOperator, ColumnStatistic> getOutputColumnsStatistics(ColumnRefSet outputColumns) {
        Map<ColumnRefOperator, ColumnStatistic> outputColumnsStatistics = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ColumnStatistic> entry : columnStatistics.entrySet()) {
            if (outputColumns.contains(entry.getKey().getId())) {
                outputColumnsStatistics.put(entry.getKey(), entry.getValue());
            }
        }
        return outputColumnsStatistics;
    }

    public boolean isTableRowCountMayInaccurate() {
        return this.tableRowCountMayInaccurate;
    }

    public ColumnRefSet getUsedColumns() {
        ColumnRefSet usedColumns = new ColumnRefSet();
        for (Map.Entry<ColumnRefOperator, ColumnStatistic> entry : columnStatistics.entrySet()) {
            usedColumns.union(entry.getKey().getUsedColumns());
        }
        return usedColumns;
    }

    /**
     * Gets the largest subset of the target columns that has multi-column statistics
     * @param targetColumns The target column set
     * @return The largest subset and its corresponding statistics, or null if no match found
     */
    public Pair<Set<ColumnRefOperator>, MultiColumnCombinedStats> getLargestSubsetMCStats(Set<ColumnRefOperator> targetColumns) {
        if (multiColumnCombinedStats.isEmpty() || targetColumns.size() <= 1) {
            return null;
        }

        Set<ColumnRefOperator> maxColumns = null;
        MultiColumnCombinedStats maxStats = null;
        int maxSize = 0;
        int targetSize = targetColumns.size();

        for (Map.Entry<Set<ColumnRefOperator>, MultiColumnCombinedStats> entry : multiColumnCombinedStats.entrySet()) {
            Set<ColumnRefOperator> keySet = entry.getKey();
            int keySize = keySet.size();

            if (keySize <= maxSize) {
                continue;
            }

            if (targetColumns.containsAll(keySet)) {
                maxColumns = keySet;
                maxStats = entry.getValue();
                maxSize = keySize;

                if (maxSize == targetSize) {
                    return new Pair<>(maxColumns, maxStats);
                }
            }
        }

        return maxColumns != null ? Pair.create(maxColumns, maxStats) : null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Statistics that = (Statistics) o;
        return Double.compare(that.outputRowCount, outputRowCount) == 0
                && tableRowCountMayInaccurate == that.tableRowCountMayInaccurate
                && Objects.equals(columnStatistics.keySet(), that.columnStatistics.keySet());
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputRowCount, columnStatistics.keySet(), tableRowCountMayInaccurate);
    }

    public static Builder buildFrom(Statistics other) {
        return new Builder(
                other.getOutputRowCount(),
                other.columnStatistics,
                other.tableRowCountMayInaccurate,
                other.shadowColumns,
                other.multiColumnCombinedStats);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private double outputRowCount;
        private final Map<ColumnRefOperator, ColumnStatistic> columnStatistics;
        private boolean tableRowCountMayInaccurate;
        // columns not used to compute costs
        // which is used by mv rewrite to make the cost accurate
        private Collection<ColumnRefOperator> shadowColumns;
        private final Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> multiColumnCombinedStats;


        public Builder() {
            this(NaN, new HashMap<>(), false);
        }

        private Builder(double outputRowCount, Map<ColumnRefOperator, ColumnStatistic> columnStatistics,
                        boolean tableRowCountMayInaccurate, Collection<ColumnRefOperator> shadowColumns) {
            this(outputRowCount, columnStatistics, tableRowCountMayInaccurate, shadowColumns, new HashMap<>());
        }

        private Builder(double outputRowCount, Map<ColumnRefOperator, ColumnStatistic> columnStatistics,
                        boolean tableRowCountMayInaccurate, Collection<ColumnRefOperator> shadowColumns,
                        Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> multiColumnCombinedStats) {
            this.outputRowCount = outputRowCount;
            this.columnStatistics = new HashMap<>(columnStatistics);
            this.tableRowCountMayInaccurate = tableRowCountMayInaccurate;
            this.shadowColumns = shadowColumns;
            this.multiColumnCombinedStats = new HashMap<>(multiColumnCombinedStats);
        }

        private Builder(double outputRowCount, Map<ColumnRefOperator, ColumnStatistic> columnStatistics,
                        boolean tableRowCountMayInaccurate) {
            this(outputRowCount, columnStatistics, tableRowCountMayInaccurate, Lists.newArrayList());
        }

        public Builder setOutputRowCount(double outputRowCount) {
            // Due to the influence of the default filter coefficient,
            // the number of calculated rows may be less than 1.
            // The minimum value of rowCount is set to 1, and values less than 1 are meaningless.
            if (outputRowCount < 1D) {
                this.outputRowCount = 1D;
            } else if (outputRowCount > StatisticsEstimateCoefficient.MAXIMUM_ROW_COUNT) {
                this.outputRowCount = StatisticsEstimateCoefficient.MAXIMUM_ROW_COUNT;
            } else {
                this.outputRowCount = outputRowCount;
            }
            return this;
        }

        public double getOutputRowCount() {
            return outputRowCount;
        }

        public Builder setTableRowCountMayInaccurate(boolean tableRowCountMayInaccurate) {
            this.tableRowCountMayInaccurate = tableRowCountMayInaccurate;
            return this;
        }

        public boolean getTableRowCountMayInaccurate() {
            return tableRowCountMayInaccurate;
        }

        public Builder addColumnStatistic(ColumnRefOperator column, ColumnStatistic statistic) {
            this.columnStatistics.put(column, statistic);
            return this;
        }

        public Builder addColumnStatistics(Map<ColumnRefOperator, ColumnStatistic> columnStatistics) {
            this.columnStatistics.putAll(columnStatistics);
            return this;
        }

        public Builder addMultiColumnStatistics(Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> mcStats) {
            this.multiColumnCombinedStats.putAll(mcStats);
            return this;
        }

        public Builder addMultiColumnStatistics(Set<ColumnRefOperator> columns, MultiColumnCombinedStats mcStats) {
            this.multiColumnCombinedStats.put(columns, mcStats);
            return this;
        }

        public ColumnStatistic getColumnStatistics(ColumnRefOperator columnRefOperator) {
            return this.columnStatistics.get(columnRefOperator);
        }

        public Builder addColumnStatisticsFromOtherStatistic(Statistics statistics, ColumnRefSet hintRefs, boolean withHist) {
            statistics.getColumnStatistics().forEach((k, v) -> {
                if (hintRefs.contains(k.getId())) {
                    this.columnStatistics.put(k, withHist ? v : ColumnStatistic.buildFrom(v).setHistogram(null).build());
                }
            });
            return this;
        }


        public Builder setShadowColumns(Collection<ColumnRefOperator> shadowColumns) {
            this.shadowColumns = shadowColumns;
            return this;
        }

        public Statistics build() {
            return new Statistics(this);
        }
    }
}
