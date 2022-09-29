// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.google.common.base.Preconditions;
import com.starrocks.external.ColumnTypeConverter;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;

public class HiveColumnStats {
    private static final Logger LOG = LogManager.getLogger(HiveColumnStats.class);

    public enum StatisticType {
        UNKNOWN,
        ESTIMATE
    }

    // only valid for string type
    private double avgSize = -1.0f;
    private long numNulls = -1L;
    private long numDistinctValues = -1L;
    private double minValue = Double.NEGATIVE_INFINITY;
    private double maxValue = Double.POSITIVE_INFINITY;
    private StatisticType type = StatisticType.UNKNOWN;

    public static final HiveColumnStats UNKNOWN =
            new HiveColumnStats(NEGATIVE_INFINITY, POSITIVE_INFINITY, -1, -1, -1, StatisticType.UNKNOWN);

    public HiveColumnStats() {
    }

    public HiveColumnStats(double minValue,
                           double maxValue,
                           long numNulls,
                           double averageRowSize,
                           long distinctValuesCount) {
        this(minValue, maxValue, numNulls, averageRowSize, distinctValuesCount, StatisticType.ESTIMATE);
    }

    public HiveColumnStats(double minValue,
                           double maxValue,
                           long numNulls,
                           double averageRowSize,
                           long distinctValuesCount,
                           StatisticType type) {
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.numNulls = numNulls;
        this.avgSize = averageRowSize;
        this.numDistinctValues = distinctValuesCount;
        this.type = type;
    }

    public boolean isUnknown() {
        return this.type == StatisticType.UNKNOWN;
    }

    @Override
    public String toString() {
        return String.format("avgSize: %.2f, numNulls: %d, numDistinctValues: %d, minValue: %.2f, maxValue: %.2f, " +
                        "type: %s",
                avgSize, numNulls, numDistinctValues, minValue, maxValue, type);
    }

    public static HiveColumnStats fromString(String columnStats) {
        String[] columnStatsArray = columnStats.split(",");
        Preconditions.checkState(columnStatsArray.length == 6);

        List<String> columnStatsList = Arrays.stream(columnStatsArray).map(s -> s.substring(s.indexOf(":") + 2)).
                collect(Collectors.toList());
        return new HiveColumnStats(Double.parseDouble(columnStatsList.get(3)), Double.parseDouble(columnStatsList.get(4)),
                Long.parseLong(columnStatsList.get(1)), Double.parseDouble(columnStatsList.get(0)),
                Long.parseLong(columnStatsList.get(2)), StatisticType.valueOf(columnStatsList.get(5)));
    }

    public void addNumNulls(long v) {
        if (v > 0L) {
            if (this.numNulls == -1L) {
                this.numNulls = v;
            } else {
                this.numNulls += v;
            }
        }
    }

    // NOTE: This is an estimation method
    public void updateNumDistinctValues(long v) {
        if (v > 0L) {
            if (this.numDistinctValues == -1L) {
                this.numDistinctValues = v;
            } else {
                this.numDistinctValues = Math.max(this.numDistinctValues, v);
            }
        }
    }

    public void updateMinValue(double v) {
        if (Double.isFinite(v)) {
            if (this.minValue == Double.NEGATIVE_INFINITY) {
                this.minValue = v;
            } else {
                this.minValue = Math.min(this.minValue, v);
            }
        }
    }

    public void updateMaxValue(double v) {
        if (Double.isFinite(v)) {
            if (this.maxValue == Double.POSITIVE_INFINITY) {
                this.maxValue = v;
            } else {
                this.maxValue = Math.max(this.maxValue, v);
            }
        }
    }

    public boolean init(String hiveType, ColumnStatisticsData statsData) {
        hiveType = ColumnTypeConverter.getTypeKeyword(hiveType);
        boolean isValid = false;
        switch (hiveType.toUpperCase()) {
            case "BOOLEAN":
                isValid = statsData.isSetBooleanStats();
                if (isValid) {
                    BooleanColumnStatsData boolStats = statsData.getBooleanStats();
                    numNulls = boolStats.getNumNulls();
                    // If we have numNulls, we can infer NDV from that.
                    if (numNulls > 0) {
                        numDistinctValues = 3;
                    } else if (numNulls == 0) {
                        numDistinctValues = 2;
                    } else {
                        numDistinctValues = -1;
                    }
                }
                break;
            case "TINYINT":
            case "SMALLINT":
            case "INT":
            case "BIGINT":
            case "TIMESTAMP": // Hive use LongColumnStatsData for timestamps.
                isValid = statsData.isSetLongStats();
                if (isValid) {
                    LongColumnStatsData longStats = statsData.getLongStats();
                    numDistinctValues = longStats.getNumDVs();
                    numNulls = longStats.getNumNulls();
                    if (longStats.isSetHighValue()) {
                        maxValue = longStats.getHighValue();
                    }
                    if (longStats.isSetLowValue()) {
                        minValue = longStats.getLowValue();
                    }
                }
                break;
            case "FLOAT":
            case "DOUBLE":
                isValid = statsData.isSetDoubleStats();
                if (isValid) {
                    DoubleColumnStatsData doubleStats = statsData.getDoubleStats();
                    numDistinctValues = doubleStats.getNumDVs();
                    numNulls = doubleStats.getNumNulls();
                    if (doubleStats.isSetHighValue()) {
                        maxValue = doubleStats.getHighValue();
                    }
                    if (doubleStats.isSetLowValue()) {
                        minValue = doubleStats.getLowValue();
                    }
                }
                break;
            case "DATE":
                isValid = statsData.isSetDateStats();
                if (isValid) {
                    DateColumnStatsData dateStats = statsData.getDateStats();
                    numDistinctValues = dateStats.getNumDVs();
                    numNulls = dateStats.getNumNulls();
                    if (dateStats.isSetHighValue()) {
                        maxValue = dateStats.getHighValue().getDaysSinceEpoch() * 24.0 * 3600L;
                    }
                    if (dateStats.isSetLowValue()) {
                        minValue = dateStats.getLowValue().getDaysSinceEpoch() * 24.0 * 3600L;
                    }
                }
                break;
            case "CHAR":
            case "VARCHAR":
            case "STRING":
                isValid = statsData.isSetStringStats();
                if (isValid) {
                    StringColumnStatsData stringStats = statsData.getStringStats();
                    numDistinctValues = stringStats.getNumDVs();
                    numNulls = stringStats.getNumNulls();
                    avgSize = stringStats.getAvgColLen();
                }
                break;
            case "DECIMAL":
                isValid = statsData.isSetDecimalStats();
                if (isValid) {
                    DecimalColumnStatsData decimalStats = statsData.getDecimalStats();
                    numNulls = decimalStats.getNumNulls();
                    numDistinctValues = decimalStats.getNumDVs();
                    if (decimalStats.isSetHighValue()) {
                        maxValue = HiveDecimal.create(new BigInteger(decimalStats.getHighValue().getUnscaled()),
                                decimalStats.getHighValue().getScale()).doubleValue();
                    }
                    if (decimalStats.isSetLowValue()) {
                        minValue = HiveDecimal.create(new BigInteger(decimalStats.getLowValue().getUnscaled()),
                                decimalStats.getLowValue().getScale()).doubleValue();
                    }
                }
                break;
            default:
                LOG.warn("unexpected column type {}", hiveType);
                break;
        }
        if (isValid) {
            type = StatisticType.ESTIMATE;
        }
        return isValid;
    }

    public double getAvgSize() {
        return avgSize;
    }

    public void setAvgSize(double avgSize) {
        if (avgSize >= 0.0f) {
            this.avgSize = avgSize;
        }
    }

    public long getNumNulls() {
        return numNulls;
    }

    public void setNumNulls(long numNulls) {
        if (numNulls >= 0L) {
            this.numNulls = numNulls;
        }
    }

    public long getNumDistinctValues() {
        return numDistinctValues;
    }

    public void setNumDistinctValues(long numDistinctValues) {
        if (numDistinctValues >= 0L) {
            this.numDistinctValues = numDistinctValues;
        }
    }

    public double getMinValue() {
        return minValue;
    }

    public void setMinValue(double minValue) {
        this.minValue = minValue;
    }

    public double getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(double maxValue) {
        this.maxValue = maxValue;
    }

    public void setType(StatisticType type) {
        this.type = type;
    }
}
