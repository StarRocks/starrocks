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


package com.starrocks.connector.hive;

import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;

import static java.lang.Math.round;

public class HiveColumnStats {
    private static final Logger LOG = LogManager.getLogger(HiveColumnStats.class);

    private enum StatisticType {
        UNKNOWN,
        ESTIMATE
    }

    private long totalSizeBytes;
    private long numNulls;
    private long ndv;
    private double min = Double.NEGATIVE_INFINITY;
    private double max = Double.POSITIVE_INFINITY;
    private StatisticType type;

    public HiveColumnStats() {
    }

    private void initBoolColumnStats(BooleanColumnStatsData boolStats) {
        numNulls = getNumNulls(boolStats.getNumNulls());
        // If we have numNulls, we can infer NDV from that.
        if (numNulls > 0) {
            ndv = 3;
        } else if (numNulls == 0) {
            ndv = 2;
        } else {
            ndv = -1;
        }
        min = boolStats.isSetNumFalses() ? 0 : 1;
        max = boolStats.isSetNumTrues() ? 1 : 0;
        if (min > max) {
            // when this column is all null values
            min = 0.0;
            max = 0.0;
        }
        type = StatisticType.ESTIMATE;
    }

    private void initLongColumnStats(LongColumnStatsData longStats, long rowNums) {
        numNulls = getNumNulls(longStats.getNumNulls());
        min = longStats.isSetLowValue() ? longStats.getLowValue() : -1;
        max = longStats.isSetHighValue() ? longStats.getHighValue() : -1;
        ndv = longStats.isSetNumDVs() ? getNdvValue(longStats.getNumDVs(), rowNums) : -1;
        type = StatisticType.ESTIMATE;
    }

    private void initDoubleColumnStats(DoubleColumnStatsData doubleStats, long rowNums) {
        numNulls = getNumNulls(doubleStats.getNumNulls());
        min = doubleStats.isSetLowValue() ? doubleStats.getLowValue() : -1;
        max = doubleStats.isSetHighValue() ? doubleStats.getHighValue() : -1;
        ndv = doubleStats.isSetNumDVs() ? getNdvValue(doubleStats.getNumDVs(), rowNums) : -1;
        type = StatisticType.ESTIMATE;
    }

    private void initDateColumnStats(DateColumnStatsData dateStats, long rowNums) {
        numNulls = getNumNulls(dateStats.getNumNulls());
        min = dateStats.isSetLowValue() ? getDateValue(dateStats.getLowValue()) : -1;
        max = dateStats.isSetHighValue() ? getDateValue(dateStats.getHighValue()) : -1;
        ndv = dateStats.isSetNumDVs() ? getNdvValue(dateStats.getNumDVs(), rowNums) : -1;
        type = StatisticType.ESTIMATE;
    }

    private void initDecimalColumnStats(DecimalColumnStatsData decimalStats, long rowNums) {
        numNulls = getNumNulls(decimalStats.getNumNulls());
        min = decimalStats.isSetLowValue() ? getDecimalValue(decimalStats.getLowValue()) : -1;
        max = decimalStats.isSetHighValue() ? getDecimalValue(decimalStats.getHighValue()) : -1;
        ndv = decimalStats.isSetNumDVs() ? getNdvValue(decimalStats.getNumDVs(), rowNums) : -1;
        type = StatisticType.ESTIMATE;
    }

    private void initStringColumnStats(StringColumnStatsData stringStats, long rowNums) {
        numNulls = getNumNulls(stringStats.getNumNulls());
        ndv = stringStats.isSetNumDVs() ? getNdvValue(stringStats.getNumDVs(), rowNums) : -1;
        double avgColLen = stringStats.isSetAvgColLen() ? stringStats.getAvgColLen() : -1;
        totalSizeBytes = getStringColumnTotalSizeBytes(avgColLen, rowNums);
        type = StatisticType.ESTIMATE;
    }

    public void initialize(ColumnStatisticsData statisticsData, long rowNums) {
        if (statisticsData.isSetBooleanStats()) {
            initBoolColumnStats(statisticsData.getBooleanStats());
        } else if (statisticsData.isSetLongStats()) {
            initLongColumnStats(statisticsData.getLongStats(), rowNums);
        } else if (statisticsData.isSetDoubleStats()) {
            initDoubleColumnStats(statisticsData.getDoubleStats(), rowNums);
        } else if (statisticsData.isSetDecimalStats()) {
            initDecimalColumnStats(statisticsData.getDecimalStats(), rowNums);
        } else if (statisticsData.isSetDateStats()) {
            initDateColumnStats(statisticsData.getDateStats(), rowNums);
        } else if (statisticsData.isSetStringStats()) {
            initStringColumnStats(statisticsData.getStringStats(), rowNums);
        } else {
            type = StatisticType.UNKNOWN;
            LOG.warn("Unexpected column statistics data: {}", statisticsData);
        }
    }

    public long getTotalSizeBytes() {
        return totalSizeBytes;
    }

    public long getNumNulls() {
        return numNulls;
    }

    public long getNdv() {
        return ndv;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public StatisticType getType() {
        return type;
    }

    private double getDecimalValue(Decimal decimal) {
        if (decimal == null) {
            return -1;
        }
        return new BigDecimal(new BigInteger(decimal.getUnscaled()), decimal.getScale()).doubleValue();
    }

    private double getDateValue(Date date) {
        return date.getDaysSinceEpoch() * 24.0 * 3600L;
    }

    private long getNdvValue(long ndv, long rowNums) {
        if (ndv != -1 && numNulls != -1 && rowNums != -1) {
            long nonNullsNum = rowNums - numNulls;
            if (numNulls > 0 && ndv > 0) {
                ndv--;
            }

            if (nonNullsNum > 0 && ndv == 0) {
                ndv = 1;
            }

            if (ndv > nonNullsNum) {
                return nonNullsNum;
            }

            return ndv;
        }
        return -1;
    }

    private long getNumNulls(long numNulls) {
        if (numNulls >= 0L) {
            return numNulls;
        }
        return -1L;
    }

    public long getStringColumnTotalSizeBytes(double avgColLen, long rowNums) {
        if (avgColLen != -1 && rowNums != -1 && numNulls != -1) {
            long nonNullsNums = rowNums - numNulls;
            if (nonNullsNums < 0) {
                return -1;
            }
            return round(avgColLen * nonNullsNums);
        }
        return -1;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HiveColumnStats{");
        sb.append("totalSizeBytes=").append(totalSizeBytes);
        sb.append(", ndv=").append(ndv);
        sb.append(", min=").append(min);
        sb.append(", max=").append(max);
        sb.append(", numNulls=").append(numNulls);
        sb.append(", type=").append(type);
        sb.append('}');
        return sb.toString();
    }
}
