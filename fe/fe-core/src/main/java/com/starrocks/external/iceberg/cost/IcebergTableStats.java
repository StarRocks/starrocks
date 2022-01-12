// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.iceberg.cost;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class IcebergTableStats {
    private static final IcebergTableStats EMPTY = IcebergTableStats.builder().build();

    private final double rowCount;
    private final Map<String, IcebergColumnStats> columnStatistics;

    public static IcebergTableStats empty() {
        return EMPTY;
    }

    public IcebergTableStats(double rowCount, Map<String, IcebergColumnStats> columnStatistics) {
        this.rowCount = requireNonNull(rowCount, "rowCount cannot be null");
        if (rowCount < 0) {
            throw new IllegalArgumentException(format("rowCount must be greater than or equal to 0: %s", rowCount));
        }
        this.columnStatistics = unmodifiableMap(requireNonNull(columnStatistics, "columnStatistics cannot be null"));
    }

    public double getRowCount() {
        return rowCount;
    }

    public Map<String, IcebergColumnStats> getColumnStatistics() {
        return columnStatistics;
    }

    public boolean isEmpty() {
        return equals(empty());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IcebergTableStats that = (IcebergTableStats) o;
        return Objects.equals(rowCount, that.rowCount) &&
                Objects.equals(columnStatistics, that.columnStatistics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowCount, columnStatistics);
    }

    @Override
    public String toString() {
        return "IcebergTableStats{" +
                "rowCount=" + rowCount +
                ", columnStatistics=" + columnStatistics +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private double rowCount = Double.NaN;
        private Map<String, IcebergColumnStats> columnStatisticsMap = new LinkedHashMap<>();

        public Builder setRowCount(double rowCount) {
            this.rowCount = requireNonNull(rowCount, "rowCount cannot be null");
            return this;
        }

        public Builder setColumnStatistics(String column, IcebergColumnStats columnStatistics) {
            requireNonNull(column, "column cannot be null");
            requireNonNull(columnStatistics, "columnStatistics cannot be null");
            this.columnStatisticsMap.put(column, columnStatistics);
            return this;
        }

        public IcebergTableStats build() {
            return new IcebergTableStats(rowCount, columnStatisticsMap);
        }
    }
}
