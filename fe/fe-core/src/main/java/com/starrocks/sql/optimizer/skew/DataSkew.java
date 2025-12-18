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
package com.starrocks.sql.optimizer.skew;

import com.google.common.collect.Lists;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;

public class DataSkew {
    private static final int DEFAULT_MCV_LIMIT = 5;
    private static final double DEFAULT_RELATIVE_ROW_THRESHOLD = 0.2;
    private static final Thresholds DEFAULT_THRESHOLDS = new Thresholds(DEFAULT_MCV_LIMIT, DEFAULT_RELATIVE_ROW_THRESHOLD);

    public record Thresholds(int mcvLimit, double relativeRowThreshold) {
        public static Thresholds withRelativeRowThreshold(double relativeRowThreshold) {
            return new Thresholds(DEFAULT_MCV_LIMIT, relativeRowThreshold);
        }

        public static Thresholds withMcvLimit(int mcvLimit) {
            return new Thresholds(mcvLimit, DEFAULT_RELATIVE_ROW_THRESHOLD);
        }
    }

    /**
     * Utility method to check if a certain column is skewed based on statistics.
     */
    public static boolean isColumnSkewed(@NotNull Statistics statistics, @NotNull ColumnStatistic columnStatistic,
                                         Thresholds thresholds) {
        if (columnStatistic.getNullsFraction() >= thresholds.relativeRowThreshold) {
            // A lot of NULL values also indicate skew.
            return true;
        }

        final var rowCount = statistics.getOutputRowCount();
        if (statistics.isTableRowCountMayInaccurate() || rowCount < 1 || columnStatistic.isUnknown() ||
                columnStatistic.isUnknownValue()) {
            // Without sufficient information we can not make a decision.
            return false;
        }

        final var histogram = columnStatistic.getHistogram();
        if (histogram != null) {
            final var mcv = histogram.getMCV();

            if (mcv != null) {
                List<Pair<String, Long>> mcvs = Lists.newArrayList();
                int mcvLimit = Math.min(thresholds.mcvLimit, mcv.size());
                mcv.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())) //
                        .limit(mcvLimit)
                        .forEach(entry -> {
                            mcvs.add(Pair.create(entry.getKey(), entry.getValue()));
                        });

                long rowCountOfMcvs = mcvs.stream().mapToLong(pair -> pair.second).sum();
                return ((double) rowCountOfMcvs / rowCount) > thresholds.relativeRowThreshold;
            }
        }

        // No histogram information, can not deduce skew.
        return false;
    }

    /* Always using default thresholds */
    public static boolean isColumnSkewed(@NotNull Statistics statistics, @NotNull ColumnStatistic columnStatistic) {
        return isColumnSkewed(statistics, columnStatistic, DEFAULT_THRESHOLDS);
    }
}
