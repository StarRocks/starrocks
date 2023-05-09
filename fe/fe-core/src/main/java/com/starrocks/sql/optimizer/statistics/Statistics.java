// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.statistic.StatsConstants;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.lang.Double.NaN;

public class Statistics {
    private final double outputRowCount;
    private final Map<ColumnRefOperator, ColumnStatistic> columnStatistics;
    // This flag set true if get table row count from GlobalStateMgr LE 1
    // Table row count in FE depends on BE reporting，but FE may not get report from BE which just started，
    // this causes the table row count stored in FE to be inaccurate.
    private final boolean tableRowCountMayInaccurate;

    private Statistics(Builder builder) {
        this.outputRowCount = builder.outputRowCount;
        this.columnStatistics = builder.columnStatistics;
        this.tableRowCountMayInaccurate = builder.tableRowCountMayInaccurate;
    }

    public double getOutputRowCount() {
        return outputRowCount;
    }

    public double getOutputSize(ColumnRefSet outputColumns) {
        double totalSize = 0;
        boolean nonEmpty = false;
        for (Map.Entry<ColumnRefOperator, ColumnStatistic> entry : columnStatistics.entrySet()) {
            if (outputColumns.contains(entry.getKey().getId())) {
                totalSize += entry.getValue().getAverageRowSize();
                nonEmpty = true;
            }
        }
        if (nonEmpty) {
            totalSize = Math.max(totalSize, 1.0);
        }
        return totalSize * outputRowCount;
    }

    public double getComputeSize() {
        // Make it at least 1 byte, otherwise the cost model would propagate estimate error
        return Math.max(1.0, this.columnStatistics.values().stream().map(ColumnStatistic::getAverageRowSize).
                reduce(0.0, Double::sum)) * outputRowCount;
    }

    public ColumnStatistic getColumnStatistic(ColumnRefOperator column) {
        ColumnStatistic result = columnStatistics.get(column);
        Preconditions.checkState(result != null);
        return result;
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
        return new Builder(other.getOutputRowCount(), other.columnStatistics, other.tableRowCountMayInaccurate);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private double outputRowCount;
        private final Map<ColumnRefOperator, ColumnStatistic> columnStatistics;
        private boolean tableRowCountMayInaccurate;

        public Builder() {
            this(NaN, new HashMap<>(), false);
        }

        private Builder(double outputRowCount, Map<ColumnRefOperator, ColumnStatistic> columnStatistics,
                        boolean tableRowCountMayInaccurate) {
            this.outputRowCount = outputRowCount;
            this.columnStatistics = new HashMap<>(columnStatistics);
            this.tableRowCountMayInaccurate = tableRowCountMayInaccurate;
        }

        public Builder setOutputRowCount(double outputRowCount) {
            // Due to the influence of the default filter coefficient,
            // the number of calculated rows may be less than 1.
            // The minimum value of rowCount is set to 1, and values less than 1 are meaningless.
            if (outputRowCount < 1D) {
                this.outputRowCount = 1D;
            } else if (outputRowCount > StatsConstants.MAXIMUM_ROW_COUNT) {
                this.outputRowCount = StatsConstants.MAXIMUM_ROW_COUNT;
            } else {
                this.outputRowCount = outputRowCount;
            }
            return this;
        }

        public Builder setTableRowCountMayInaccurate(boolean tableRowCountMayInaccurate) {
            this.tableRowCountMayInaccurate = tableRowCountMayInaccurate;
            return this;
        }

        public Builder addColumnStatistic(ColumnRefOperator column, ColumnStatistic statistic) {
            this.columnStatistics.put(column, statistic);
            return this;
        }

        public Builder addColumnStatistics(Map<ColumnRefOperator, ColumnStatistic> columnStatistics) {
            this.columnStatistics.putAll(columnStatistics);
            return this;
        }

        public ColumnStatistic getColumnStatistics(ColumnRefOperator columnRefOperator) {
            return this.columnStatistics.get(columnRefOperator);
        }

        public Builder removeColumnStatistics(ColumnRefOperator column) {
            columnStatistics.remove(column);
            return this;
        }

        public Statistics build() {
            return new Statistics(this);
        }
    }
}
