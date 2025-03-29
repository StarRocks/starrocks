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

package com.starrocks.statistic.base;

import com.starrocks.statistic.StatsConstants;
import com.starrocks.statistic.sample.SampleInfo;

import java.text.MessageFormat;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.statistic.StatsConstants.COLUMN_ID_SEPARATOR;

// Before obtaining the statistics function, it is necessary to check
// whether it is in the statistics types, but if it is not, it will return empty.
public class MultiColumnStats implements ColumnStats {
    private final List<DefaultColumnStats> columnGroup;
    private final List<StatsConstants.StatisticsType> statisticsTypes;

    public MultiColumnStats(List<DefaultColumnStats> columnGroup, List<StatsConstants.StatisticsType> statisticsTypes) {
        this.columnGroup = columnGroup;
        this.statisticsTypes = statisticsTypes;
    }

    @Override
    public String getColumnNameStr() {
        return columnGroup.stream()
                .sorted(Comparator.comparingInt(DefaultColumnStats::getColumnId))
                .map(columnStats -> String.valueOf(columnStats.getColumnId()))
                .collect(Collectors.collectingAndThen(Collectors.joining(COLUMN_ID_SEPARATOR), s -> "'" + s + "'"));
    }

    @Override
    public String getNDV() {
        if (statisticsTypes.contains(StatsConstants.StatisticsType.MCDISTINCT)) {
            return "ndv(" + getCombinedMultiColumnKey() + ")";
        } else {
            return "0";
        }
    }

    @Override
    public String getCombinedMultiColumnKey() {
        List<String> columnCoalesce = columnGroup.stream()
                .map(columnStats -> "coalesce(" + columnStats.getQuotedColumnName() + ", '')")
                .collect(Collectors.toList());
        return "murmur_hash3_32(" + String.join(", ", columnCoalesce) + ")";
    }

    // we use GEE (Guaranteed-Error Estimator) as estimator to calculate approximate ndv.
    // https://dl.acm.org/doi/pdf/10.1145/335168.335230
    // D_GEE = d + (sqrt(n / r) - 1) * f1
    // D_GEE is the estimated number of distinct values.
    // n is the total number of rows in the table.
    // r is the size of the random sample.
    // d is the number of distinct values observed in the sample.
    // f1 is the number of values that appear exactly once in the sample.
    @Override
    public String getSampleNDV(SampleInfo info) {
        if (statisticsTypes.contains(StatsConstants.StatisticsType.MCDISTINCT)) {
            String sampleRows = "SUM(t1.count)";
            String onceCount = "SUM(IF(t1.count = 1, 1, 0))";
            String sampleNdv = "COUNT(1)";
            String fn = MessageFormat.format("{0} + (sqrt({1} / {2}) - 1) * {3}",
                    sampleNdv, String.valueOf(info.getTotalRowCount()), sampleRows, onceCount);
            return "IFNULL(" + fn + ", COUNT(1))";
        } else {
            return "0";
        }
    }

    @Override
    public long getTypeSize() {
        return 0;
    }

    @Override
    public String getQuotedColumnName() {
        return "";
    }

    @Override
    public String getMax() {
        return "";
    }

    @Override
    public String getMin() {
        return "";
    }

    @Override
    public String getCollectionSize() {
        return "";
    }

    @Override
    public String getFullDataSize() {
        return "";
    }

    @Override
    public String getFullNullCount() {
        return "";
    }

    @Override
    public String getSampleDateSize(SampleInfo info) {
        return "";
    }

    @Override
    public String getSampleNullCount(SampleInfo info) {
        return "";
    }
    
}
